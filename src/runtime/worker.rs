use std::num::NonZeroU32;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;

use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use futures::FutureExt;
use futures::StreamExt;
use tokio::sync::Notify;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinError;
use tokio::task::JoinHandle;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::error::SchedulerError;
use crate::error::TaskError;
use crate::ids::QueueName;
use crate::ids::TaskName;
use crate::models::AttemptNo;
use crate::models::ClaimedTask;
use crate::models::RetryOutcome;
use crate::models::TaskLease;
use crate::models::TaskStatus;
use crate::pool::SchedulerPool;
use crate::runtime::builder::Config;
use crate::runtime::builder::Scheduler;
use crate::runtime::context::TaskContext;
use crate::runtime::health::HealthStatus;
use crate::runtime::health::WorkerHealth;
use crate::runtime::registry::Handler;
use crate::runtime::registry::Registry;
use crate::store;
use crate::store::PoisonedClaim;

/// Shortest idle wait before a task due soon (a due task the cycle could not claim waits
/// `max_poll_interval` instead: another transaction holds it).
const MIN_WAIT: Duration = Duration::from_millis(10);

/// Pool connections dead-lettering may hold at once, per scheduler. Rows a claim poisons
/// while all are in use stay `running`, and the reaper recovers them after the lease.
const QUARANTINE_CONNECTIONS: usize = 1;

/// How long shutdown waits, after `shutdown_timeout`, for dropped handlers' tasks to be released.
const RELEASE_TIMEOUT: Duration = Duration::from_secs(5);

/// Bound on each shutdown-time registry write (`stopping`, then the delete), so a stalled
/// database adds at most 1 s to shutdown.
const REGISTRY_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(500);

/// The shutdown phases every dispatch watches.
#[derive(Clone)]
struct Shutdown {
    /// Shutdown began (`run_until_shutdown`'s token): handlers' `ctx.cancellation` fires.
    begun: CancellationToken,
    /// `shutdown_timeout` passed: running handlers are dropped and their tasks released.
    expired: CancellationToken,
}

/// A served queue and the permits that limit its running handlers.
#[derive(Clone)]
struct WorkerQueue {
    name: QueueName,
    permits: Arc<Semaphore>,
    /// When one of its dispatches last ended, for the registry.
    last_finished: Arc<Mutex<Option<Instant>>>,
}

impl<P: SchedulerPool> Scheduler<P> {
    /// Claim and run tasks until `cancel` fires. Then stop claiming and maintenance, cancel
    /// every handler's `ctx.cancellation`, and drain for up to `shutdown_timeout`. Handlers
    /// still running then are dropped and their tasks released; after 5 s more, whatever is
    /// left is aborted and the reaper recovers those tasks after the lease. A completion, retry,
    /// failure or release already in flight is not aborted: it runs on its own task until its
    /// transaction commits or rolls back. After it returns, each heartbeat or such transition
    /// still in flight may hold one pool connection until its transaction ends; each of its
    /// statements is bounded by the lease it had left. A heartbeat renews nothing then, unless
    /// it passed its cancellation check before its dispatch ended.
    ///
    /// It has no error path. Startup and claim failures surface through
    /// [`Scheduler::health`] (`Degraded`, with `last_error`) while the runtime keeps retrying.
    pub async fn run_until_shutdown(self, cancel: CancellationToken) {
        let Scheduler {
            pool,
            registry,
            config,
            health_tx,
        } = self;
        let health = StoppedOnDrop(health_tx);
        let config = Arc::new(config);
        let names = registry.task_names();
        let queues: Vec<_> = config
            .queues
            .iter()
            .map(|(name, limit)| WorkerQueue {
                name: name.clone(),
                permits: Arc::new(Semaphore::new(limit.get())),
                last_finished: Arc::default(),
            })
            .collect();
        let mut in_flight = JoinSet::new();
        let quarantine = Arc::new(Semaphore::new(QUARANTINE_CONNECTIONS));
        let shutdown = Shutdown {
            begun: cancel.clone(),
            expired: CancellationToken::new(),
        };
        // The receiver task that owns the LISTEN session (none yet), and its wakeups.
        let mut receiver: Option<JoinHandle<()>> = None;
        let wake = Arc::new(Notify::new());
        let mut failures = 0u64;
        // The last claim cycle's error, which a materializer run reports beside its own.
        let mut claim_failed: Option<String> = None;
        // A child token: cancelled together with `cancel`, or by the guard if this future is
        // dropped (aborted, or a failed `try_join!` sibling) so maintenance, the materializer
        // and the registry never outlive it.
        let background = cancel.child_token();
        let _stop_background = background.clone().drop_guard();
        let maintenance = tokio::spawn(maintain(pool.clone(), config.clone(), background.clone()));
        // Each materializer run's error, `None` when it succeeded. Without `cron` nothing sends:
        // the channel is closed, and the wait's `changed()` arm never fires.
        let (runs, mut materialized) = watch::channel(None::<String>);
        #[cfg(feature = "cron")]
        let materializer = tokio::spawn(materialize_loop(
            pool.clone(),
            config.clone(),
            runs,
            background.clone(),
        ));
        #[cfg(not(feature = "cron"))]
        drop(runs);
        let registration = tokio::spawn(keep_registered(
            pool.clone(),
            config.clone(),
            names.clone(),
            queues.clone(),
            health.0.subscribe(),
            background.clone(),
        ));

        while !cancel.is_cancelled() {
            // The wait below collects at most one finished task per wake, and a full batch
            // skips it entirely; collect them all here. Dispatches are bounded by the permits.
            // At most QUARANTINE_CONNECTIONS dead-letter tasks run at once; rows poisoned while
            // they all run are left to the reaper. Poisoned rows never make a batch full, so
            // they never skip the wait on their own.
            while let Some(joined) = in_flight.try_join_next() {
                log_join(joined);
            }
            // A finished receiver never listened, or its connection failed: re-listen. On
            // `background`, so an aborted run still stops it and it UNLISTENs.
            let mut listening = None;
            if receiver.as_ref().is_none_or(JoinHandle::is_finished) {
                let (ready, subscribed) = oneshot::channel();
                listening = Some(subscribed);
                receiver = Some(tokio::spawn(listen(
                    pool.clone(),
                    wake.clone(),
                    ready,
                    background.clone(),
                )));
            }
            // Every pool and query await races shutdown, so a stalled database cannot hold it
            // past its bound. A claim cut off after it committed leaves its tasks `running`
            // until the reaper recovers them, as when a claim's response is lost.
            let step = async {
                // LISTEN before the claim, so a task enqueued after the claim's snapshot is
                // notified. An error means the receiver ended without listening.
                if let Some(subscribed) = listening {
                    subscribed.await.ok();
                }
                if queues.iter().all(|q| q.permits.available_permits() == 0) {
                    return Some(config.max_poll_interval);
                }
                let cycle = claim_cycle(
                    &pool,
                    &registry,
                    &names,
                    &config,
                    &queues,
                    &mut in_flight,
                    &quarantine,
                    &shutdown,
                )
                .await;
                claim_failed = cycle.as_ref().err().map(ToString::to_string);
                // The materializer's last failure keeps health degraded but never stops
                // claiming; it counts as one failure per failed run, not per claim cycle.
                let (run_failed, ran) = {
                    let run = materialized.borrow_and_update();
                    (run.clone(), run.has_changed())
                };
                report(
                    &health.0,
                    &mut failures,
                    claim_failed.as_deref().or(run_failed.as_deref()),
                    cycle.is_err() || (ran && run_failed.is_some()),
                );
                cycle.unwrap_or_else(|error| {
                    tracing::error!(%error, "claim failed");
                    Some(config.max_poll_interval)
                })
            };
            let wait = match cancel.run_until_cancelled(step).await {
                None => break,
                Some(None) => continue, // full batch: more work is likely due
                Some(Some(wait)) => wait,
            };
            let until = Instant::now() + wait;
            loop {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => break,
                    Some(joined) = in_flight.join_next() => log_join(joined),
                    () = wake.notified() => {},
                    // A materializer run reports now, beside the last claim cycle's result, even
                    // while no claim cycle is due or no queue has a free permit. Then the wait
                    // goes on: a run is no reason to claim, and one that inserts tasks notifies.
                    Ok(()) = materialized.changed() => {
                        let run_failed = materialized.borrow_and_update().clone();
                        report(
                            &health.0,
                            &mut failures,
                            claim_failed.as_deref().or(run_failed.as_deref()),
                            run_failed.is_some(),
                        );
                        continue;
                    },
                    () = tokio::time::sleep_until(until) => {}
                }
                break;
            }
        }

        if let Err(error) = maintenance.await {
            tracing::error!(%error, "maintenance task panicked");
        }
        #[cfg(feature = "cron")]
        if let Err(error) = materializer.await {
            tracing::error!(%error, "materializer task panicked");
        }
        // Detached, not awaited: `cancel` stops the receiver, which UNLISTENs on its own task, so
        // a stalled connection cannot delay the drain and returns to the pool only once UNLISTEN
        // completes.
        drop(receiver);
        // `true` once every dispatch has ended within `limit`.
        let mut drain = async |limit| {
            let all = async {
                while let Some(joined) = in_flight.join_next().await {
                    log_join(joined);
                }
            };
            tokio::time::timeout(limit, all).await.is_ok()
        };
        if !drain(config.shutdown_timeout).await {
            tracing::warn!("shutdown timeout: dropping running handlers and releasing their tasks");
            shutdown.expired.cancel();
            if !drain(RELEASE_TIMEOUT).await {
                tracing::warn!("release timeout: aborting; the reaper recovers those tasks");
            }
        }
        in_flight.shutdown().await;
        // It exits within REGISTRY_SHUTDOWN_TIMEOUT of `cancel`. That bounds shutdown, not the
        // server: a periodic upsert cut off by `cancel`, or a `stopping` write that timed out,
        // may still execute after the delete below and re-create the row, which then reads
        // live until its `lost_after` (docs/AdminUI.md §6.11).
        if let Err(error) = registration.await {
            tracing::error!(%error, "registry task panicked");
        }
        registry_bounded(async {
            let mut conn = acquire(&pool).await?;
            store::remove_worker(&mut conn, &config.worker_id).await
        })
        .await;
        // `health` reports Stopped as it drops
    }
}

async fn acquire<P: SchedulerPool>(pool: &P) -> Result<P::Conn, SchedulerError> {
    pool.acquire().await.map_err(SchedulerError::Pool)
}

/// Await a store call, recording `pg_task_scheduler_db_query_seconds{operation}`.
async fn timed<T>(operation: &'static str, query: impl Future<Output = T>) -> T {
    let started = Instant::now();
    let out = query.await;
    metrics::histogram!("pg_task_scheduler_db_query_seconds", "operation" => operation)
        .record(started.elapsed());
    out
}

/// A row count as `u64`, for metrics and for comparing with a `u32` batch size; no supported
/// `usize` exceeds `u64`, so the saturation never applies.
fn count(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

/// Turn due recurring schedules into queued tasks; the metric counts tasks inserted.
#[cfg(feature = "cron")]
async fn materialize<P: SchedulerPool>(pool: &P) -> Result<(), SchedulerError> {
    let mut conn = acquire(pool).await?;
    let n = timed("materialize", store::materialize_due_jobs(&mut conn))
        .await
        .inspect_err(|error| tracing::error!(%error, "materialize failed"))?;
    metrics::counter!("pg_task_scheduler_tasks_materialized_total").increment(count(n));
    Ok(())
}

/// Materialize at once and every `max_poll_interval`, on a connection of its own, so a stalled
/// schedule table never holds up claims. Each run is one bounded pass, so a backlog of due
/// schedules takes several. Publishes each run's error on `runs` (`None` on success); every
/// await races `cancel`.
#[cfg(feature = "cron")]
async fn materialize_loop<P: SchedulerPool>(
    pool: P,
    config: Arc<Config>,
    runs: watch::Sender<Option<String>>,
    cancel: CancellationToken,
) {
    let mut tick = tokio::time::interval(config.max_poll_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    while cancel.run_until_cancelled(tick.tick()).await.is_some() {
        if let Some(run) = cancel.run_until_cancelled(materialize(&pool)).await {
            runs.send_replace(run.err().map(|error| error.to_string()));
        }
    }
}

/// The receiver: owns a pooled connection, runs `LISTEN queue_available`, reports on
/// `ready`, then reads the notification stream until it fails or `cancel` fires. Each
/// `queue_available` notification calls `wake.notify_one()`, which stores at most one permit,
/// so a burst while the loop is busy is one wakeup. Other channels (a host's own `LISTEN` on
/// the pooled session) are ignored. It then runs `UNLISTEN queue_available`, never `*`, and
/// drops the connection. If it gets no connection or `LISTEN` fails, it ends at once: the
/// worker only polls, and re-listens on the next loop.
async fn listen<P: SchedulerPool>(
    pool: P,
    wake: Arc<Notify>,
    ready: oneshot::Sender<()>,
    cancel: CancellationToken,
) {
    let mut conn = match cancel.run_until_cancelled(acquire(&pool)).await {
        Some(Ok(conn)) => conn,
        Some(Err(error)) => {
            tracing::warn!(%error, "LISTEN failed; polling only");
            return;
        }
        None => return,
    };
    let subscribe = diesel::sql_query("LISTEN queue_available").execute(&mut *conn);
    match cancel.run_until_cancelled(subscribe).await {
        Some(Err(error)) => {
            tracing::warn!(%error, "LISTEN failed; polling only");
            return; // a failed LISTEN is not in effect: return the connection as is
        }
        None => {} // cut off mid-LISTEN, which may be in effect
        Some(Ok(_)) => {
            ready.send(()).ok();
            let mut stream = std::pin::pin!(conn.notifications_stream());
            loop {
                match cancel.run_until_cancelled(stream.next()).await {
                    Some(Some(Ok(n))) if n.channel == "queue_available" => wake.notify_one(),
                    Some(Some(Ok(_))) => {}
                    Some(_) => {
                        tracing::warn!("LISTEN connection failed; re-listening");
                        break;
                    }
                    None => break,
                }
            }
        }
    }
    if let Err(error) = diesel::sql_query("UNLISTEN queue_available")
        .execute(&mut *conn)
        .await
    {
        tracing::warn!(%error, "UNLISTEN failed");
    }
}

/// Reports `Stopped` when the run future ends, whether it returned or was dropped.
struct StoppedOnDrop(watch::Sender<WorkerHealth>);

impl Drop for StoppedOnDrop {
    fn drop(&mut self) {
        self.0.send_modify(|h| h.status = HealthStatus::Stopped);
    }
}

/// For each queue with a free permit, in configuration order: claim up to
/// `min(its free permits, claim_batch_size)` of its due tasks and spawn a dispatch for each.
/// `None` when any batch was full, else how long to wait: until the next due task in a queue
/// that still has a free permit, within `MIN_WAIT..=max_poll_interval`.
#[expect(
    clippy::too_many_arguments,
    reason = "the run loop's state, borrowed for one cycle"
)]
async fn claim_cycle<P: SchedulerPool>(
    pool: &P,
    registry: &Registry,
    names: &[TaskName],
    config: &Arc<Config>,
    queues: &[WorkerQueue],
    in_flight: &mut JoinSet<()>,
    quarantine: &Arc<Semaphore>,
    shutdown: &Shutdown,
) -> Result<Option<Duration>, SchedulerError> {
    let mut conn = acquire(pool).await?;
    let (mut full, mut claimed_any) = (false, false);
    for queue in queues {
        if shutdown.begun.is_cancelled() {
            return Ok(Some(config.max_poll_interval));
        }
        let free = queue.permits.available_permits();
        // Exact: the `min` with the `u32` batch size bounds the result, so a `free` beyond
        // `u32` only means the batch size wins.
        let want = u32::try_from(free).unwrap_or(u32::MAX);
        let Some(want) = NonZeroU32::new(want.min(config.claim_batch_size.get())) else {
            continue;
        };
        let sent = Instant::now();
        let batch = timed(
            "claim",
            store::claim(
                &mut conn,
                &config.worker_id,
                &queue.name,
                names,
                want,
                config.lease_duration,
            ),
        )
        .await?;
        // Ready rows only: a poisoned row takes no permit, so counting it would loop at once
        // and spawn a dead-letter task per batch of a poisoned backlog.
        let full_batch = count(batch.ready.len()) == u64::from(want.get());
        claimed_any |= !batch.ready.is_empty();
        for task in batch.ready {
            let name = task.task_name.as_str().to_owned();
            let queue_label = queue.name.as_str().to_owned();
            let latency = (task.claimed_at - task.due_at).to_std().unwrap_or_default();
            metrics::counter!("pg_task_scheduler_tasks_claimed_total", "task_name" => name.clone(), "queue" => queue_label.clone())
                .increment(1);
            metrics::histogram!("pg_task_scheduler_claim_latency_seconds", "task_name" => name, "queue" => queue_label)
                .record(latency);
            let permit =
                queue.permits.clone().try_acquire_owned().map_err(|_| {
                    SchedulerError::Invariant("claimed more tasks than free permits")
                })?;
            let handler = registry
                .get(&task.task_name)
                .ok_or(SchedulerError::Invariant("claimed a task with no handler"))?;
            let deadline = sent + config.lease_duration.get();
            let run = dispatch(
                pool.clone(),
                config.clone(),
                handler,
                task,
                deadline,
                permit,
                shutdown.clone(),
            );
            let last_finished = queue.last_finished.clone();
            in_flight.spawn(async move {
                run.await;
                *last_finished.lock().unwrap_or_else(PoisonError::into_inner) =
                    Some(Instant::now());
            });
        }
        // After the dispatches, and never awaited here: a slow dead-letter must not use up the
        // healthy rows' leases. In `in_flight`, so shutdown drains it like a dispatch. Never
        // waits for the permit either: that would hold these leases, unbounded.
        if !batch.poisoned.is_empty() {
            match quarantine.clone().try_acquire_owned() {
                Ok(permit) => {
                    in_flight.spawn(dead_letter(pool.clone(), batch.poisoned, permit));
                }
                Err(_) => {
                    for claim in &batch.poisoned {
                        tracing::warn!(task_id = claim.task_id.0, reason = %claim.reason, "dead-lettering is busy; the reaper recovers the task after its lease");
                    }
                }
            }
        }
        // A full batch only means "loop now" if this queue can still take more.
        full |= full_batch && queue.permits.available_permits() > 0;
    }
    if full {
        return Ok(None);
    }
    let max = config.max_poll_interval;
    let open: Vec<QueueName> = queues
        .iter()
        .filter(|q| q.permits.available_permits() > 0)
        .map(|q| q.name.clone())
        .collect();
    if open.is_empty() {
        return Ok(Some(max));
    }
    let next = store::next_due_in(&mut conn, &open, names).await?;
    Ok(Some(match next {
        // Due, yet this cycle claimed nothing: another transaction holds the row (SKIP
        // LOCKED), and polling every MIN_WAIT would not get it sooner. Notifications still
        // wake the loop for new work.
        Some(Duration::ZERO) if !claimed_any => max,
        next => next.unwrap_or(max).max(MIN_WAIT).min(max),
    }))
}

/// Dead-letter rows the claim could not convert, on a connection of their own, holding
/// `_permit` until done. A row this misses stays `running` until the reaper requeues it, and
/// the next claim poisons it again.
async fn dead_letter<P: SchedulerPool>(
    pool: P,
    poisoned: Vec<PoisonedClaim>,
    _permit: OwnedSemaphorePermit,
) {
    for claim in &poisoned {
        tracing::error!(task_id = claim.task_id.0, reason = %claim.reason, "claimed task is unusable; marking it dead");
    }
    let mut conn = match acquire(&pool).await {
        Ok(conn) => conn,
        Err(error) => {
            tracing::error!(%error, "could not dead-letter the tasks; skipping them");
            return;
        }
    };
    for claim in &poisoned {
        if let Err(error) = timed("fail", store::dead_letter(&mut conn, claim)).await {
            tracing::error!(task_id = claim.task_id.0, %error, "could not dead-letter the task; skipping it");
        }
    }
}

/// Publish health: `error` is the last claim cycle's, else the materializer's last run's.
/// `fresh` is whether a failure happened since the last report; a standing error carried over
/// (a failed materializer run, or a failed cycle beside a materializer run) does not count again.
fn report(
    health: &watch::Sender<WorkerHealth>,
    failures: &mut u64,
    error: Option<&str>,
    fresh: bool,
) {
    *failures = match (error, fresh) {
        (None, _) => 0,
        (Some(_), true) => failures.saturating_add(1),
        (Some(_), false) => *failures,
    };
    let next = WorkerHealth {
        status: match error {
            Some(_) => HealthStatus::Degraded,
            None => HealthStatus::Healthy,
        },
        consecutive_failures: *failures,
        last_error: error.map(str::to_owned),
    };
    health.send_if_modified(|h| {
        let changed = *h != next;
        *h = next;
        changed
    });
}

fn log_join(joined: Result<(), JoinError>) {
    if let Err(error) = joined
        && error.is_panic()
    {
        tracing::error!(%error, "dispatch panicked; the reaper recovers its task after the lease");
    }
}

/// Run one claimed task: the handler and its lease keeper concurrently, then finalize.
fn dispatch<P: SchedulerPool>(
    pool: P,
    config: Arc<Config>,
    handler: Handler,
    task: ClaimedTask,
    deadline: Instant,
    permit: OwnedSemaphorePermit,
    shutdown: Shutdown,
) -> impl Future<Output = ()> {
    let span = tracing::info_span!(
        "task",
        task_id = task.lease.task_id.0,
        task_key = task.task_key.as_str(),
        task_name = task.task_name.as_str(),
        queue = task.queue.as_str(),
        attempt_no = task.lease.attempt_no.get(),
        worker_id = config.worker_id.as_str(),
        lock_token = task.lease.lock_token.0,
    );
    async move {
        let _permit = permit;
        let name = task.task_name.as_str().to_owned();
        // A slow claim response can arrive after the lease it claimed; never start external
        // work on it. The reaper recovers the task.
        if Instant::now() >= deadline {
            metrics::counter!("pg_task_scheduler_leases_lost_total", "task_name" => name)
                .increment(1);
            tracing::warn!("claim arrived after its lease; handler not started");
            return;
        }
        // Cancelled when the lease keeper gives up, and by the guard however dispatch ends
        // (finished, released, or dropped with the scheduler), so a heartbeat still in flight
        // renews nothing. Not a child of `shutdown.begun`: draining handlers keep their leases.
        let lease_stop = CancellationToken::new();
        let _stop_heartbeats = lease_stop.clone().drop_guard();
        // Cancelled when shutdown begins, and by the guard however dispatch ends: lease lost,
        // finished, released, or aborted.
        let cancellation = shutdown.begun.child_token();
        let cancel_on_drop = cancellation.clone().drop_guard();
        let ctx = TaskContext {
            lease: task.lease.clone(),
            task_key: task.task_key,
            task_name: task.task_name,
            max_attempts: task.max_attempts,
            locked_until: task.locked_until,
            cancellation,
        };
        let started = Instant::now();
        // The lease keeper is its own future, not a select! arm body: a DB-contained
        // handler holding the row lock must keep being polled while a heartbeat waits.
        // It is polled before the handler (and so is forced-shutdown expiry): a handler
        // suspended past the local deadline must be dropped, not resumed for one more segment.
        let result = tokio::select! {
            biased;
            () = keep_lease(&pool, &task.lease, &config, &name, deadline, &lease_stop) => {
                drop(cancel_on_drop); // cancel now, not after the check below
                lease_stop.cancel(); // and end any heartbeat still in flight
                // A DB-contained handler may have committed its own completion while a
                // heartbeat waited on the row lock, or an operator cancelled the task; the
                // heartbeat then matches no row. Bounded, so a database outage cannot hold
                // the permit here.
                let ended = async {
                    let mut conn = acquire(&pool).await.ok()?;
                    Some(ended_elsewhere(&mut conn, &task.lease, &name).await)
                };
                let ended = tokio::time::timeout(config.heartbeat_interval, ended).await;
                if !matches!(ended, Ok(Some(true))) {
                    metrics::counter!("pg_task_scheduler_leases_lost_total", "task_name" => name)
                        .increment(1);
                    tracing::warn!("lease lost; handler dropped");
                }
                return;
            }
            // `shutdown_timeout` passed with the handler still running; it is dropped already.
            () = shutdown.expired.cancelled() => {
                release(&pool, &task.lease, name).await;
                return;
            }
            result = AssertUnwindSafe(handler(ctx, task.payload)).catch_unwind() => match result {
                Ok(result) => result,
                // Not finalized: the attempt stays open and the reaper recovers the task.
                Err(panic) => {
                    let message = panic
                        .downcast_ref::<&str>()
                        .copied()
                        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
                        .unwrap_or("non-string panic payload");
                    tracing::error!(panic = message, "handler panicked; the reaper recovers its task after the lease");
                    return;
                }
            },
        };
        metrics::histogram!("pg_task_scheduler_task_duration_seconds", "task_name" => name.clone())
            .record(started.elapsed());
        let shutting_down = shutdown.begun.is_cancelled();
        finish(&pool, &config, &task.lease, name, result, shutting_down).await;
    }
    .instrument(span)
}

/// Heartbeat every `heartbeat_interval`; returns only once ownership is lost: a heartbeat
/// matched no row, or the local deadline (claim send + lease, advanced by each successful
/// heartbeat) passed. The deadline only gives up early; PostgreSQL decides validity.
/// `stop` is the dispatch's: a heartbeat spawned before the dispatch ended renews nothing.
async fn keep_lease<P: SchedulerPool>(
    pool: &P,
    lease: &TaskLease,
    config: &Config,
    name: &str,
    mut deadline: Instant,
    stop: &CancellationToken,
) {
    loop {
        tokio::time::sleep_until((Instant::now() + config.heartbeat_interval).min(deadline)).await;
        let sent = Instant::now();
        // A beat started now could not be awaited, and could renew after this keeper is gone.
        if sent >= deadline {
            return;
        }
        let beat = tokio::time::timeout_at(deadline, async {
            let mut conn = acquire(pool).await?;
            // The pool wait may have used up the lease; never spawn a beat with no time left.
            let Some(lock_wait) = deadline
                .checked_duration_since(Instant::now())
                .filter(|left| !left.is_zero())
            else {
                return Ok(None);
            };
            // Only the transaction is spawned, so a dropped keep_lease (handler finished,
            // deadline, scheduler dropped) cannot abandon it mid-flight holding the row lock:
            // dropping a JoinHandle does not cancel the task. Each of its statements runs for
            // at most `lock_wait`, and it renews nothing once `stop` fires.
            let (lease, duration, stop) = (lease.clone(), config.lease_duration, stop.clone());
            tokio::spawn(async move {
                let query = store::heartbeat_bounded(&mut conn, &lease, duration, lock_wait, &stop);
                timed("heartbeat", query).await
            })
            .await
            .map_err(|_| SchedulerError::Invariant("heartbeat task panicked"))?
            .map(Some)
        })
        .await;
        match beat {
            Ok(Ok(Some(_))) => deadline = sent + config.lease_duration.get(),
            Ok(Ok(None) | Err(SchedulerError::LostOwnership(_))) | Err(_) => return,
            Ok(Err(error)) => {
                metrics::counter!("pg_task_scheduler_heartbeat_failures_total", "task_name" => name.to_owned())
                    .increment(1);
                tracing::warn!(%error, "heartbeat failed");
            }
        }
    }
}

/// Record the handler's result with an ownership-guarded transition. A handler interrupted
/// by shutdown has its task released rather than retried.
async fn finish<P: SchedulerPool>(
    pool: &P,
    config: &Config,
    lease: &TaskLease,
    name: String,
    result: Result<(), TaskError>,
    shutting_down: bool,
) {
    if shutting_down && matches!(&result, Err(error) if error.is_interrupted()) {
        return release(pool, lease, name).await;
    }
    let mut conn = match acquire(pool).await {
        Ok(conn) => conn,
        Err(error) => {
            tracing::error!(%error, "cannot finalize; the reaper recovers the task after the lease");
            return;
        }
    };
    let owned = lease.clone();
    let error = match result {
        Ok(()) => {
            let Some((mut conn, completed)) = spawned(async move {
                let completed = timed("complete", store::complete(&mut conn, &owned)).await;
                (conn, completed)
            })
            .await
            else {
                return;
            };
            match completed {
                Ok(()) => {
                    metrics::counter!("pg_task_scheduler_tasks_done_total", "task_name" => name)
                        .increment(1);
                }
                Err(SchedulerError::LostOwnership(_)) => {
                    if !ended_elsewhere(&mut conn, lease, &name).await {
                        tracing::warn!("lost ownership before completion");
                    }
                }
                Err(error) => tracing::error!(%error, "completion failed"),
            }
            return;
        }
        Err(error) => error,
    };
    let message = error.to_string();
    let delay = error.is_retryable().then(|| {
        let random = uuid::Uuid::new_v4().as_u64_pair().0;
        retry_delay(
            lease.attempt_no,
            config.base_retry_delay,
            config.max_retry_delay,
            config.retry_jitter,
            random,
        )
    });
    let classification = if delay.is_some() {
        "retryable"
    } else {
        "permanent"
    };
    let text = message.clone();
    let Some((mut conn, outcome)) = spawned(async move {
        let outcome = match delay {
            Some(delay) => timed("retry", store::retry(&mut conn, &owned, &text, delay)).await,
            None => timed("fail", store::fail_permanently(&mut conn, &owned, &text))
                .await
                .map(|()| RetryOutcome::Dead),
        };
        (conn, outcome)
    })
    .await
    else {
        return;
    };
    match outcome {
        Ok(RetryOutcome::Queued { due_at }) => {
            metrics::counter!("pg_task_scheduler_tasks_retried_total", "task_name" => name)
                .increment(1);
            tracing::warn!(classification, retry_due_at = %due_at, error = %message, "task failed; retry scheduled");
        }
        Ok(RetryOutcome::Dead) => {
            metrics::counter!("pg_task_scheduler_tasks_dead_total", "task_name" => name)
                .increment(1);
            tracing::error!(classification, error = %message, "task failed; marked dead");
        }
        Err(store_error) => {
            let lost = matches!(store_error, SchedulerError::LostOwnership(_));
            if !(lost && ended_elsewhere(&mut conn, lease, &name).await) {
                tracing::warn!(classification, error = %message, %store_error, "task failed; result not recorded");
            }
        }
    }
}

/// Return the task to `queued` at shutdown without counting the attempt. On failure it stays
/// `running` until the reaper recovers it after the lease, and that attempt counts.
async fn release<P: SchedulerPool>(pool: &P, lease: &TaskLease, name: String) {
    let released = match acquire(pool).await {
        Ok(mut conn) => {
            let owned = lease.clone();
            let Some((mut conn, released)) = spawned(async move {
                let released = timed("release", store::release(&mut conn, &owned)).await;
                (conn, released)
            })
            .await
            else {
                return;
            };
            match released {
                Err(SchedulerError::LostOwnership(_)) => {
                    if !ended_elsewhere(&mut conn, lease, &name).await {
                        tracing::warn!("lost ownership before release");
                    }
                    return;
                }
                released => released,
            }
        }
        Err(error) => Err(error),
    };
    match released {
        Ok(()) => {
            metrics::counter!("pg_task_scheduler_tasks_released_total", "task_name" => name)
                .increment(1);
            tracing::info!("task released at shutdown; attempt not counted");
        }
        Err(error) => {
            tracing::error!(%error, "release failed; the reaper recovers the task after the lease");
        }
    }
}

/// Run a finalizing transition on its own task, which owns the connection and hands it back with
/// the result. As with `keep_lease`'s heartbeats, dropping this future (shutdown's abort) does
/// not cancel that task, so the transaction always commits or rolls back; `BOUND` limits each
/// of its statements by the lease left. `None` if the task panicked.
async fn spawned<C, T>(transition: impl Future<Output = (C, T)> + Send + 'static) -> Option<(C, T)>
where
    C: Send + 'static,
    T: Send + 'static,
{
    tokio::spawn(transition)
        .await
        .inspect_err(|error| tracing::error!(%error, "finalizing transition task panicked"))
        .ok()
}

/// After a heartbeat or transition matched no row: if the task under this lease was completed
/// by its own handler or cancelled by an operator, count that and return `true`. `false` means
/// the lease was lost (or the check failed); the caller reports that its own way.
async fn ended_elsewhere(conn: &mut AsyncPgConnection, lease: &TaskLease, name: &str) -> bool {
    match store::lease_status(conn, lease).await {
        Ok(Some(TaskStatus::Done)) => {
            metrics::counter!("pg_task_scheduler_tasks_done_total", "task_name" => name.to_owned())
                .increment(1);
            tracing::debug!("handler completed the task itself");
            true
        }
        Ok(Some(TaskStatus::Cancelled)) => {
            metrics::counter!("pg_task_scheduler_tasks_cancelled_total", "task_name" => name.to_owned())
                .increment(1);
            tracing::info!("task cancelled by an operator; handler dropped");
            true
        }
        Ok(_) => false,
        Err(error) => {
            tracing::warn!(%error, "lease check failed");
            false
        }
    }
}

/// Reaper and retention cleanup every `reaper_interval`. Errors are logged and leave worker
/// health unchanged.
async fn maintain<P: SchedulerPool>(pool: P, config: Arc<Config>, cancel: CancellationToken) {
    let mut tick = tokio::time::interval(config.reaper_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    while cancel.run_until_cancelled(tick.tick()).await.is_some() {
        if let Some(Err(error)) = cancel
            .run_until_cancelled(maintenance_pass(&pool, &config))
            .await
        {
            tracing::warn!(%error, "maintenance failed");
        }
    }
}

/// Rounds of one reap batch then one cleanup batch, until both run short. Reaping runs
/// every round, so a cleanup backlog never delays lease recovery.
async fn maintenance_pass<P: SchedulerPool>(
    pool: &P,
    config: &Config,
) -> Result<(), SchedulerError> {
    let mut conn = acquire(pool).await?;
    let mut cleaning = true;
    loop {
        let reaped = timed(
            "reap",
            store::reap_expired(
                &mut conn,
                config.reaper_batch_size,
                config.crash_retry_delay,
            ),
        )
        .await?;
        let n = reaped.requeued + reaped.dead;
        metrics::counter!("pg_task_scheduler_leases_expired_total").increment(count(n));
        if cleaning {
            let deleted = timed("cleanup", store::cleanup(&mut conn, &config.retention)).await?;
            metrics::counter!("pg_task_scheduler_cleanup_deleted_total", "kind" => "task")
                .increment(count(deleted.tasks()));
            metrics::counter!("pg_task_scheduler_cleanup_deleted_total", "kind" => "outbox")
                .increment(count(deleted.outbox));
            cleaning = deleted.any_full(config.retention.batch_size);
        }
        if count(n) < u64::from(config.reaper_batch_size.get()) && !cleaning {
            return Ok(());
        }
    }
}

/// Keep this worker's display-only registry row (docs/AdminUI.md §6.7): upsert at once and every
/// `registry_interval`, each time then pruning rows not written for 24 h. Here, not in
/// maintenance, so a lock on the registry never delays a reap. When `cancel` fires, write
/// `stopping` once, within `REGISTRY_SHUTDOWN_TIMEOUT`, and return. Every other await races
/// `cancel`, so a stalled pool never holds shutdown.
async fn keep_registered<P: SchedulerPool>(
    pool: P,
    config: Arc<Config>,
    task_names: Vec<TaskName>,
    queues: Vec<WorkerQueue>,
    health: watch::Receiver<WorkerHealth>,
    cancel: CancellationToken,
) {
    let registration = |stopping: bool| {
        let health = health.borrow();
        let status = match (stopping, health.status) {
            (true, _) | (_, HealthStatus::Stopped) => store::WorkerStatus::Stopping,
            (_, HealthStatus::Starting) => store::WorkerStatus::Starting,
            (_, HealthStatus::Healthy) => store::WorkerStatus::Healthy,
            (_, HealthStatus::Degraded) => store::WorkerStatus::Degraded,
        };
        let now = Instant::now();
        store::WorkerRegistration {
            worker_id: config.worker_id.clone(),
            label: config.label.clone(),
            status,
            last_error: health.last_error.clone(),
            queues: config
                .queues
                .iter()
                .zip(&queues)
                .map(|((_, limit), q)| store::QueueSlots {
                    name: q.name.clone(),
                    limit: limit.get(),
                    in_use: limit.get().saturating_sub(q.permits.available_permits()),
                    finished_ago: q
                        .last_finished
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .map(|at| now.saturating_duration_since(at)),
                })
                .collect(),
            task_names: task_names.clone(),
            lost_after: config.registry_interval.saturating_mul(3),
        }
    };
    let upsert = async |reg: store::WorkerRegistration| {
        let mut conn = acquire(&pool).await?;
        store::upsert_worker(&mut conn, &reg).await
    };
    let refresh = async || {
        let mut conn = acquire(&pool).await?;
        store::upsert_worker(&mut conn, &registration(false)).await?;
        store::prune_workers(&mut conn).await.map(drop)
    };
    let mut tick = tokio::time::interval(config.registry_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    while cancel.run_until_cancelled(tick.tick()).await.is_some() {
        if let Some(Err(error)) = cancel.run_until_cancelled(refresh()).await {
            registry_failed(&error);
        }
    }
    registry_bounded(upsert(registration(true))).await;
}

/// A shutdown-time registry write, bounded by `REGISTRY_SHUTDOWN_TIMEOUT`.
async fn registry_bounded(write: impl Future<Output = Result<(), SchedulerError>>) {
    match tokio::time::timeout(REGISTRY_SHUTDOWN_TIMEOUT, write).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => registry_failed(&error),
        Err(elapsed) => registry_failed(&elapsed),
    }
}

/// Registry failures are logged and counted; they never touch health or claiming.
fn registry_failed(error: &dyn std::fmt::Display) {
    metrics::counter!("pg_task_scheduler_registry_failures_total").increment(1);
    tracing::warn!(%error, "worker registry write failed");
}

/// `min(base · 2^(attempt_no − 1), max) + random % (jitter + 1 ns)`, computed exactly for
/// any `attempt_no`.
pub(crate) fn retry_delay(
    attempt_no: AttemptNo,
    base: Duration,
    max: Duration,
    jitter: Duration,
    random: u64,
) -> Duration {
    // 2^127 ns exceeds the 5-year duration cap, so clamping the count never changes the
    // capped result; saturation is likewise absorbed by the `max` cap.
    let doublings = (attempt_no.get() - 1).min(127);
    let nanos = base.as_nanos().saturating_mul(1u128 << doublings);
    let nanos = nanos.min(max.as_nanos());
    let jitter_nanos = u64::try_from(jitter.as_nanos()).unwrap_or(u64::MAX);
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX)).saturating_add(
        Duration::from_nanos(random % jitter_nanos.saturating_add(1)),
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::retry_delay;
    use crate::AttemptNo;

    fn n(v: u32) -> AttemptNo {
        AttemptNo::try_from(v).unwrap()
    }

    const BASE: Duration = Duration::from_secs(1);
    const MAX: Duration = Duration::from_secs(300);

    #[test]
    fn retry_delay_doubles_from_base() {
        assert_eq!(retry_delay(n(1), BASE, MAX, Duration::ZERO, 0), BASE);
        assert_eq!(retry_delay(n(3), BASE, MAX, Duration::ZERO, 0), 4 * BASE);
    }

    #[test]
    fn retry_delay_first_attempt_is_base() {
        assert_eq!(
            retry_delay(AttemptNo::FIRST, BASE, MAX, Duration::ZERO, 0),
            BASE
        );
    }

    #[test]
    fn retry_delay_saturates_at_max() {
        assert_eq!(
            retry_delay(n(i32::MAX.unsigned_abs()), BASE, MAX, Duration::ZERO, 0),
            MAX
        );
    }

    #[test]
    fn retry_delay_keeps_doubling_past_attempt_32() {
        let ns = Duration::from_nanos(1);
        assert_eq!(
            retry_delay(n(34), ns, MAX, Duration::ZERO, 0),
            Duration::from_nanos(1 << 33)
        );
        assert_eq!(retry_delay(n(40), ns, MAX, Duration::ZERO, 0), MAX);
        assert_eq!(
            retry_delay(n(i32::MAX.unsigned_abs()), ns, MAX, Duration::ZERO, 0),
            MAX
        );
    }

    #[test]
    fn retry_delay_jitter_is_bounded() {
        let jitter = Duration::from_millis(500);
        let d = retry_delay(n(1), BASE, MAX, jitter, u64::MAX);
        assert!((BASE..=BASE + jitter).contains(&d), "{d:?}");
        assert_eq!(retry_delay(n(1), BASE, MAX, jitter, 0), BASE);
    }
}
