use std::num::NonZeroUsize;
use std::ops::DerefMut;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use diesel_async::{AsyncPgConnection, RunQueryDsl};
use futures::{FutureExt, StreamExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::{JoinError, JoinSet};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::error::{SchedulerError, TaskError};
use crate::ids::QueueName;
use crate::models::{ClaimedTask, RetryOutcome, TaskLease, TaskStatus};
use crate::pool::SchedulerPool;
use crate::runtime::builder::{Config, Scheduler};
use crate::runtime::context::TaskContext;
use crate::runtime::health::{HealthStatus, WorkerHealth};
use crate::runtime::registry::{Handler, Registry};
use crate::store;

/// Shortest idle wait before a task due soon (a due task the cycle could not claim waits
/// `max_poll_interval` instead: another transaction holds it).
const MIN_WAIT: Duration = Duration::from_millis(10);

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
    /// left is aborted and the reaper recovers those tasks after the lease.
    pub async fn run_until_shutdown(self, cancel: CancellationToken) -> Result<(), SchedulerError> {
        let Scheduler {
            pool,
            registry,
            config,
            health_tx,
        } = self;
        let health = StoppedOnDrop(health_tx);
        let config = Arc::new(config);
        let names = registry.names();
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
        let shutdown = Shutdown {
            begun: cancel.clone(),
            expired: CancellationToken::new(),
        };
        let runtime = tokio::runtime::Handle::current();
        let mut listener = Listener::<P> {
            conn: None,
            runtime: runtime.clone(),
        };
        let mut failures = 0u64;
        // A child token: cancelled together with `cancel`, or by the guard if this future is
        // dropped (aborted, or a failed `try_join!` sibling) so maintenance and the registry
        // never outlive it.
        let background = cancel.child_token();
        let _stop_background = background.clone().drop_guard();
        let maintenance = tokio::spawn(maintain(pool.clone(), config.clone(), background.clone()));
        let registration = tokio::spawn(keep_registered(
            pool.clone(),
            config.clone(),
            names.clone(),
            queues.clone(),
            health.0.subscribe(),
            background,
        ));
        #[cfg(feature = "cron")]
        let (mut materialized, mut next_materialize) = (Ok(()), Instant::now());

        while !cancel.is_cancelled() {
            // The wait below collects at most one finished dispatch per wake, and a full batch
            // skips it entirely; collect them all here so the set stays bounded by the permits.
            while let Some(joined) = in_flight.try_join_next() {
                log_join(joined);
            }
            // Every pool and query await races shutdown, so a stalled database cannot hold it
            // past its bound. A claim cut off after it committed leaves its tasks `running`
            // until the reaper recovers them, as when a claim's response is lost.
            let step = async {
                // At most once per `max_poll_interval`; the wait below wakes for it, so a due
                // schedule is materialized within `max_poll_interval`.
                #[cfg(feature = "cron")]
                let materialized_now = Instant::now() >= next_materialize;
                #[cfg(feature = "cron")]
                if materialized_now {
                    next_materialize = Instant::now() + config.max_poll_interval;
                    materialized = materialize(&pool).await;
                }
                if listener.conn.is_none() {
                    listen(&pool, &mut listener).await;
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
                    &shutdown,
                )
                .await;
                // The last materialization's failure keeps health degraded but never stops
                // claiming; it counts as one failure per failed run, not per claim cycle.
                #[cfg(feature = "cron")]
                let (failed, fresh) = (
                    cycle.as_ref().err().or(materialized.as_ref().err()),
                    cycle.is_err() || (materialized_now && materialized.is_err()),
                );
                #[cfg(not(feature = "cron"))]
                let (failed, fresh) = (cycle.as_ref().err(), true);
                report(&health.0, &mut failures, failed, fresh);
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
            #[cfg(feature = "cron")]
            let wait = wait.min(next_materialize.saturating_duration_since(Instant::now()));
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                Some(joined) = in_flight.join_next() => log_join(joined),
                listening = notified(&mut listener.conn) => if !listening {
                    tracing::warn!("LISTEN connection failed; re-listening");
                    listener = Listener {
                        conn: None,
                        runtime: runtime.clone(),
                    };
                },
                () = tokio::time::sleep(wait) => {}
            }
        }

        if let Err(error) = maintenance.await {
            tracing::error!(%error, "maintenance task panicked");
        }
        // The guard runs UNLISTEN on a spawned task, so a stalled connection cannot delay the
        // drain; the connection returns to the pool only once UNLISTEN completes.
        drop(listener);
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
        // live until its `lost_after` (AdminUI §6.11).
        if let Err(error) = registration.await {
            tracing::error!(%error, "registry task panicked");
        }
        registry_bounded(async {
            let mut conn = acquire(&pool).await?;
            store::remove_worker(&mut conn, &config.worker_id).await
        })
        .await;
        Ok(()) // `health` reports Stopped as it drops
    }
}

async fn acquire<P: SchedulerPool>(pool: &P) -> Result<P::Conn, SchedulerError> {
    pool.acquire()
        .await
        .map_err(|e| SchedulerError::Pool(e.to_string()))
}

/// Await a store call, recording `pg_task_scheduler_db_query_seconds{operation}`.
async fn timed<T>(operation: &'static str, query: impl Future<Output = T>) -> T {
    let started = Instant::now();
    let out = query.await;
    metrics::histogram!("pg_task_scheduler_db_query_seconds", "operation" => operation)
        .record(started.elapsed());
    out
}

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

/// Fill `listener` with a pooled connection listening on `queue_available`. The guard holds
/// the connection before `LISTEN` is sent, so a future dropped mid-`LISTEN` (shutdown, or
/// the run future dropped) still UNLISTENs it. On failure the listener stays empty: the
/// worker only polls, and retries on the next loop.
async fn listen<P: SchedulerPool>(pool: &P, listener: &mut Listener<P>) {
    let listening: Result<(), SchedulerError> = async {
        let conn = listener.conn.insert(acquire(pool).await?);
        diesel::sql_query("LISTEN queue_available")
            .execute(&mut **conn)
            .await?;
        Ok(())
    }
    .await;
    if let Err(error) = listening {
        tracing::warn!(%error, "LISTEN failed; polling only");
        listener.conn = None; // a failed LISTEN is not in effect: return the connection as is
    }
}

/// The `LISTEN` connection, if any. Dropped while still holding one (the run future was
/// dropped, the connection failed, or shutdown), it runs `UNLISTEN *` on a spawned task before the
/// connection returns to the pool: a pooled connection left listening buffers every
/// `queue_available` notification in the database, unbounded, for as long as it lives.
/// The runtime handle is captured at start, so this also works when the run future is
/// dropped from a thread outside the runtime.
struct Listener<P: SchedulerPool> {
    conn: Option<P::Conn>,
    runtime: tokio::runtime::Handle,
}

impl<P: SchedulerPool> Drop for Listener<P> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.runtime.spawn(unlisten(conn));
        }
    }
}

/// Reports `Stopped` when the run future ends, whether it returned or was dropped.
struct StoppedOnDrop(watch::Sender<WorkerHealth>);

impl Drop for StoppedOnDrop {
    fn drop(&mut self) {
        self.0.send_modify(|h| h.status = HealthStatus::Stopped);
    }
}

async fn unlisten<C: DerefMut<Target = AsyncPgConnection>>(mut conn: C) {
    if let Err(error) = diesel::sql_query("UNLISTEN *").execute(&mut *conn).await {
        tracing::warn!(%error, "UNLISTEN failed");
    }
}

/// Wait for a notification (forever without a listener), then drain any already
/// buffered. `false` when the listening connection has failed.
async fn notified<C: DerefMut<Target = AsyncPgConnection>>(listener: &mut Option<C>) -> bool {
    let Some(conn) = listener else {
        return std::future::pending().await;
    };
    let mut stream = std::pin::pin!(conn.notifications_stream());
    if !matches!(stream.next().await, Some(Ok(_))) {
        return false;
    }
    while let Some(Some(Ok(_))) = stream.next().now_or_never() {}
    true
}

/// For each queue with a free permit, in configuration order: claim up to
/// `min(its free permits, claim_batch_size)` of its due tasks and spawn a dispatch for each.
/// `None` when any batch was full, else how long to wait: until the next due task in a queue
/// that still has a free permit, within `MIN_WAIT..=max_poll_interval`.
async fn claim_cycle<P: SchedulerPool>(
    pool: &P,
    registry: &Registry,
    names: &[String],
    config: &Arc<Config>,
    queues: &[WorkerQueue],
    in_flight: &mut JoinSet<()>,
    shutdown: &Shutdown,
) -> Result<Option<Duration>, SchedulerError> {
    let mut conn = acquire(pool).await?;
    let (mut full, mut claimed_any) = (false, false);
    for queue in queues {
        if shutdown.begun.is_cancelled() {
            return Ok(Some(config.max_poll_interval));
        }
        let free = queue.permits.available_permits();
        let Some(want) = NonZeroUsize::new(free.min(config.claim_batch_size.get())) else {
            continue;
        };
        let sent = Instant::now();
        let claimed = timed(
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
        let full_batch = claimed.len() == want.get();
        claimed_any |= !claimed.is_empty();
        for task in claimed {
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
            let deadline = sent + config.lease_duration;
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

/// Publish health. `fresh` is whether `error` happened in this cycle; a standing error
/// carried over from an earlier cycle (a failed materialization) does not count again.
fn report(
    health: &watch::Sender<WorkerHealth>,
    failures: &mut u64,
    error: Option<&SchedulerError>,
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
        last_error: error.map(ToString::to_string),
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
        attempt_no = task.lease.attempt_no,
        worker_id = config.worker_id.as_str(),
        lock_token = task.lease.lock_token.0,
    );
    async move {
        let _permit = permit;
        let name = task.task_name.as_str().to_owned();
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
        let result = tokio::select! {
            biased;
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
            () = keep_lease(&pool, &task.lease, &config, &name, deadline) => {
                drop(cancel_on_drop); // cancel now, not after the check below
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
async fn keep_lease<P: SchedulerPool>(
    pool: &P,
    lease: &TaskLease,
    config: &Config,
    name: &str,
    mut deadline: Instant,
) {
    loop {
        tokio::time::sleep_until((Instant::now() + config.heartbeat_interval).min(deadline)).await;
        let sent = Instant::now();
        let beat = tokio::time::timeout_at(deadline, async {
            let mut conn = acquire(pool).await?;
            timed(
                "heartbeat",
                store::heartbeat(&mut conn, lease, config.lease_duration),
            )
            .await
        })
        .await;
        match beat {
            Ok(Ok(_)) => deadline = sent + config.lease_duration,
            Ok(Err(SchedulerError::LostOwnership(_))) | Err(_) => return,
            Ok(Err(error)) => {
                metrics::counter!("pg_task_scheduler_heartbeat_failures_total", "task_name" => name.to_owned())
                    .increment(1);
                tracing::warn!(%error, "heartbeat failed");
            }
        }
        if Instant::now() >= deadline {
            return;
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
    let error = match result {
        Ok(()) => {
            match timed("complete", store::complete(&mut conn, lease)).await {
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
    let (classification, outcome) = if error.is_retryable() {
        let random = uuid::Uuid::new_v4().as_u64_pair().0;
        let delay = retry_delay(
            lease.attempt_no,
            config.base_retry_delay,
            config.max_retry_delay,
            config.retry_jitter,
            random,
        );
        let outcome = timed("retry", store::retry(&mut conn, lease, &message, delay)).await;
        ("retryable", outcome)
    } else {
        let outcome = timed("fail", store::fail_permanently(&mut conn, lease, &message)).await;
        ("permanent", outcome.map(|()| RetryOutcome::Dead))
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
        Ok(mut conn) => match timed("release", store::release(&mut conn, lease)).await {
            Err(SchedulerError::LostOwnership(_)) => {
                if !ended_elsewhere(&mut conn, lease, &name).await {
                    tracing::warn!("lost ownership before release");
                }
                return;
            }
            released => released,
        },
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
    if let Err(error) = store::prune_workers(&mut conn).await {
        registry_failed(&error);
    }
    let batch = config.retention.batch_size.get();
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
                .increment(count(deleted.tasks));
            metrics::counter!("pg_task_scheduler_cleanup_deleted_total", "kind" => "outbox")
                .increment(count(deleted.outbox));
            cleaning = deleted.tasks >= batch || deleted.outbox >= batch;
        }
        if n < config.reaper_batch_size.get() && !cleaning {
            return Ok(());
        }
    }
}

/// Keep this worker's display-only registry row (§5.2): upsert at once and every
/// `registry_interval`. When `cancel` fires, write `stopping` once, within
/// `REGISTRY_SHUTDOWN_TIMEOUT`, and return. Every other await races `cancel`, so a stalled
/// pool never holds shutdown.
async fn keep_registered<P: SchedulerPool>(
    pool: P,
    config: Arc<Config>,
    task_names: Vec<String>,
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
    let mut tick = tokio::time::interval(config.registry_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    while cancel.run_until_cancelled(tick.tick()).await.is_some() {
        if let Some(Err(error)) = cancel
            .run_until_cancelled(upsert(registration(false)))
            .await
        {
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

/// `min(base · 2^(attempt_no − 1), max) + random % (jitter + 1 ns)` (§11.2). Saturates
/// rather than overflowing for any `attempt_no`.
pub(crate) fn retry_delay(
    attempt_no: i32,
    base: Duration,
    max: Duration,
    jitter: Duration,
    random: u64,
) -> Duration {
    let doublings = attempt_no.saturating_sub(1).clamp(0, 31).unsigned_abs();
    let jitter_nanos = u64::try_from(jitter.as_nanos()).unwrap_or(u64::MAX);
    base.saturating_mul(1 << doublings)
        .min(max)
        .saturating_add(Duration::from_nanos(
            random % jitter_nanos.saturating_add(1),
        ))
}

#[cfg(test)]
mod tests {
    use super::retry_delay;
    use std::time::Duration;

    const BASE: Duration = Duration::from_secs(1);
    const MAX: Duration = Duration::from_secs(300);

    #[test]
    fn retry_delay_doubles_from_base() {
        assert_eq!(retry_delay(1, BASE, MAX, Duration::ZERO, 0), BASE);
        assert_eq!(retry_delay(3, BASE, MAX, Duration::ZERO, 0), 4 * BASE);
    }

    #[test]
    fn retry_delay_saturates_at_max() {
        assert_eq!(retry_delay(i32::MAX, BASE, MAX, Duration::ZERO, 0), MAX);
        assert_eq!(retry_delay(0, BASE, MAX, Duration::ZERO, 0), BASE);
    }

    #[test]
    fn retry_delay_jitter_is_bounded() {
        let jitter = Duration::from_millis(500);
        let d = retry_delay(1, BASE, MAX, jitter, u64::MAX);
        assert!((BASE..=BASE + jitter).contains(&d), "{d:?}");
        assert_eq!(retry_delay(1, BASE, MAX, jitter, 0), BASE);
    }
}
