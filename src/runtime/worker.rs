use std::num::NonZeroUsize;
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::Duration;

use diesel_async::{AsyncPgConnection, RunQueryDsl};
use futures::{FutureExt, StreamExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::{JoinError, JoinSet};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::error::{SchedulerError, TaskError};
use crate::models::{ClaimedTask, RetryOutcome, TaskLease};
use crate::pool::SchedulerPool;
use crate::runtime::builder::{Config, Scheduler};
use crate::runtime::context::TaskContext;
use crate::runtime::health::{HealthStatus, WorkerHealth};
use crate::runtime::registry::{Handler, Registry};
use crate::store;

/// Shortest idle wait, so an overdue-but-unclaimable task cannot spin the loop.
const MIN_WAIT: Duration = Duration::from_millis(10);

impl<P: SchedulerPool> Scheduler<P> {
    /// Claim and run tasks until `cancel` fires. Then stop maintenance, drain running
    /// handlers for up to `shutdown_timeout`, and abort the rest (their leases are left
    /// for the reaper).
    pub async fn run_until_shutdown(self, cancel: CancellationToken) -> Result<(), SchedulerError> {
        let Scheduler {
            pool,
            registry,
            config,
            health_tx,
        } = self;
        let config = Arc::new(config);
        let names = registry.names();
        let semaphore = Arc::new(Semaphore::new(config.max_concurrency.get()));
        let mut in_flight = JoinSet::new();
        let mut listener: Option<P::Conn> = None;
        let mut failures = 0u64;
        // A child token: cancelled together with `cancel`.
        let maintenance =
            tokio::spawn(maintain(pool.clone(), config.clone(), cancel.child_token()));

        while !cancel.is_cancelled() {
            // [cron] materialize due schedules here.
            if listener.is_none() {
                listener = listen(&pool).await;
            }
            let want = semaphore
                .available_permits()
                .min(config.claim_batch_size.get());
            let wait = match NonZeroUsize::new(want) {
                None => config.max_poll_interval,
                Some(want) => {
                    let cycle = claim_cycle(
                        &pool,
                        &registry,
                        &names,
                        &config,
                        &semaphore,
                        &mut in_flight,
                        want,
                    )
                    .await;
                    report(&health_tx, &mut failures, cycle.as_ref().err());
                    match cycle {
                        Ok(Some(wait)) => wait,
                        Ok(None) => continue, // full batch: more work is likely due
                        Err(error) => {
                            tracing::error!(%error, "claim failed");
                            config.max_poll_interval
                        }
                    }
                }
            };
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                Some(joined) = in_flight.join_next() => log_join(joined),
                listening = notified(&mut listener) => if !listening {
                    tracing::warn!("LISTEN connection failed; re-listening");
                    listener = None;
                },
                () = tokio::time::sleep(wait) => {}
            }
        }

        let _ = maintenance.await;
        if let Some(mut conn) = listener
            && let Err(error) = diesel::sql_query("UNLISTEN *").execute(&mut *conn).await
        {
            tracing::warn!(%error, "UNLISTEN failed");
        }
        let drain = async {
            while let Some(joined) = in_flight.join_next().await {
                log_join(joined);
            }
        };
        if tokio::time::timeout(config.shutdown_timeout, drain)
            .await
            .is_err()
        {
            tracing::warn!("shutdown timeout: aborting running handlers");
        }
        in_flight.shutdown().await;
        health_tx.send_modify(|h| h.status = HealthStatus::Stopped);
        Ok(())
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

/// A pooled connection listening on `queue_available`, or `None` on failure (the worker
/// then only polls, and retries on the next loop).
async fn listen<P: SchedulerPool>(pool: &P) -> Option<P::Conn> {
    let listening: Result<P::Conn, SchedulerError> = async {
        let mut conn = acquire(pool).await?;
        diesel::sql_query("LISTEN queue_available")
            .execute(&mut *conn)
            .await?;
        Ok(conn)
    }
    .await;
    listening
        .inspect_err(|error| tracing::warn!(%error, "LISTEN failed; polling only"))
        .ok()
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

/// Claim up to `want` due tasks and spawn a dispatch for each. `None` when the batch was
/// full, else how long to wait: until the next due task, within `MIN_WAIT..=max_poll_interval`.
async fn claim_cycle<P: SchedulerPool>(
    pool: &P,
    registry: &Registry,
    names: &[String],
    config: &Arc<Config>,
    semaphore: &Arc<Semaphore>,
    in_flight: &mut JoinSet<()>,
    want: NonZeroUsize,
) -> Result<Option<Duration>, SchedulerError> {
    let mut conn = acquire(pool).await?;
    let sent = Instant::now();
    let claimed = timed(
        "claim",
        store::claim(
            &mut conn,
            &config.worker_id,
            names,
            want,
            config.lease_duration,
        ),
    )
    .await?;
    let full = claimed.len() == want.get();
    for task in claimed {
        let name = task.task_name.as_str().to_owned();
        let latency = (task.claimed_at - task.due_at).to_std().unwrap_or_default();
        metrics::counter!("pg_task_scheduler_tasks_claimed_total", "task_name" => name.clone())
            .increment(1);
        metrics::histogram!("pg_task_scheduler_claim_latency_seconds", "task_name" => name)
            .record(latency);
        let permit = semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| SchedulerError::Invariant("claimed more tasks than free permits"))?;
        let handler = registry
            .get(&task.task_name)
            .ok_or(SchedulerError::Invariant("claimed a task with no handler"))?;
        let deadline = sent + config.lease_duration;
        in_flight.spawn(dispatch(
            pool.clone(),
            config.clone(),
            handler,
            task,
            deadline,
            permit,
        ));
    }
    if full {
        return Ok(None);
    }
    let next = store::next_due_in(&mut conn, names).await?;
    let max = config.max_poll_interval;
    Ok(Some(next.unwrap_or(max).max(MIN_WAIT).min(max)))
}

fn report(
    health: &watch::Sender<WorkerHealth>,
    failures: &mut u64,
    error: Option<&SchedulerError>,
) {
    *failures = match error {
        Some(_) => failures.saturating_add(1),
        None => 0,
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
        tracing::error!(%error, "handler panicked; the reaper recovers its task after the lease");
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
) -> impl Future<Output = ()> {
    let span = tracing::info_span!(
        "task",
        task_id = task.lease.task_id.0,
        task_key = task.task_key.as_str(),
        task_name = task.task_name.as_str(),
        attempt_no = task.lease.attempt_no,
        worker_id = config.worker_id.as_str(),
        lock_token = task.lease.lock_token.0,
    );
    async move {
        let _permit = permit;
        let name = task.task_name.as_str().to_owned();
        let cancellation = CancellationToken::new();
        // Cancels ctx.cancellation however dispatch ends: lease lost, finished, or aborted.
        let _cancel_on_drop = cancellation.clone().drop_guard();
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
            result = handler(ctx, task.payload) => result,
            () = keep_lease(&pool, &task.lease, &config, &name, deadline) => {
                metrics::counter!("pg_task_scheduler_leases_lost_total", "task_name" => name)
                    .increment(1);
                tracing::warn!("lease lost; handler dropped");
                return;
            }
        };
        metrics::histogram!("pg_task_scheduler_task_duration_seconds", "task_name" => name.clone())
            .record(started.elapsed());
        finish(&pool, &config, &task.lease, name, result).await;
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

/// Record the handler's result with an ownership-guarded transition.
async fn finish<P: SchedulerPool>(
    pool: &P,
    config: &Config,
    lease: &TaskLease,
    name: String,
    result: Result<(), TaskError>,
) {
    let mut conn = match acquire(pool).await {
        Ok(conn) => conn,
        Err(error) => {
            tracing::error!(%error, "cannot finalize; the reaper recovers the task after the lease");
            return;
        }
    };
    let error = match result {
        Ok(()) => {
            let completed = match timed("complete", store::complete(&mut conn, lease)).await {
                Ok(()) => Ok(true),
                // Benign when a DB-contained handler completed the task itself.
                Err(SchedulerError::LostOwnership(_)) => store::completed_by(&mut conn, lease)
                    .await
                    .inspect(|&by_handler| {
                        if by_handler {
                            tracing::debug!("handler completed the task itself");
                        }
                    }),
                Err(error) => Err(error),
            };
            match completed {
                Ok(true) => {
                    metrics::counter!("pg_task_scheduler_tasks_done_total", "task_name" => name)
                        .increment(1);
                }
                Ok(false) => tracing::warn!("lost ownership before completion"),
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
            tracing::warn!(classification, error = %message, %store_error, "task failed; result not recorded");
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

/// Loop each bounded batch until a short one: expired leases first, then retention.
async fn maintenance_pass<P: SchedulerPool>(
    pool: &P,
    config: &Config,
) -> Result<(), SchedulerError> {
    let mut conn = acquire(pool).await?;
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
        if n < config.reaper_batch_size.get() {
            break;
        }
    }
    let batch = config.retention.batch_size.get();
    loop {
        let deleted = timed("cleanup", store::cleanup(&mut conn, &config.retention)).await?;
        metrics::counter!("pg_task_scheduler_cleanup_deleted_total", "kind" => "task")
            .increment(count(deleted.tasks));
        metrics::counter!("pg_task_scheduler_cleanup_deleted_total", "kind" => "outbox")
            .increment(count(deleted.outbox));
        if deleted.tasks < batch && deleted.outbox < batch {
            return Ok(());
        }
    }
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
