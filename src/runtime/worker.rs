use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::error::SchedulerError;
use crate::ids::WorkerId;
use crate::metrics;
use crate::models::{ClaimedRun, FailureOutcome, FinalizeOutcome, Outcome, RenewalOutcome};
use crate::pool::SchedulerPool;
use crate::runtime::builder::Scheduler;
use crate::runtime::context::JobContext;
use crate::runtime::health::{HealthStatus, WorkerHealth};
use crate::runtime::registry::{Handler, Registry};
use crate::store;

struct CancelOnDrop(CancellationToken);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

async fn conn<P: SchedulerPool>(pool: &P) -> Result<P::Conn, SchedulerError> {
    pool.acquire()
        .await
        .map_err(|e| SchedulerError::Pool(e.to_string()))
}

impl<P: SchedulerPool> Scheduler<P> {
    pub async fn run_until_shutdown(self, cancel: CancellationToken) -> Result<(), SchedulerError> {
        let Scheduler {
            pool,
            registry,
            config,
            health_tx,
        } = self;
        let registry = Arc::new(registry);
        let names = Arc::new(registry.names());
        let semaphore = Arc::new(Semaphore::new(config.max_concurrency.get()));
        let mut in_flight = JoinSet::new();
        let mut poll = tokio::time::interval(config.poll_interval);
        let mut recover = tokio::time::interval(config.reaper_interval);
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        recover.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut failures = 0u64;

        // Tokio intervals tick immediately. The first iteration therefore
        // materializes/reclaims/claims without an artificial startup delay.
        loop {
            let mut cycle_error = None;
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = poll.tick() => {
                    if let Err(error) = materialize(&pool).await {
                        cycle_error = Some(error);
                    }
                }
                _ = recover.tick() => {
                    if let Err(error) = recover_leases(&pool).await {
                        cycle_error = Some(error);
                    }
                }
                joined = in_flight.join_next(), if !in_flight.is_empty() => {
                    if let Some(result) = joined {
                        log_handler_join(result);
                    }
                }
            }

            while let Some(result) = in_flight.try_join_next() {
                log_handler_join(result);
            }

            if cycle_error.is_none()
                && let Err(error) = claim_and_dispatch(
                    &pool,
                    &registry,
                    &names,
                    &config.worker_id,
                    &semaphore,
                    &mut in_flight,
                )
                .await
            {
                cycle_error = Some(error);
            }

            update_health(&health_tx, &mut failures, cycle_error.as_ref());
            if let Some(error) = cycle_error {
                tracing::error!(error = %error, "scheduler cycle failed");
            }
        }

        tracing::info!("draining in-flight handlers");
        let _ = tokio::time::timeout(config.shutdown_timeout, async {
            while let Some(result) = in_flight.join_next().await {
                log_handler_join(result);
            }
        })
        .await;
        in_flight.abort_all();
        while let Some(result) = in_flight.join_next().await {
            log_handler_join(result);
        }
        health_tx.send_replace(WorkerHealth {
            status: HealthStatus::Stopped,
            consecutive_failures: failures,
            last_error: None,
        });
        Ok(())
    }
}

fn update_health(
    health: &tokio::sync::watch::Sender<WorkerHealth>,
    failures: &mut u64,
    error: Option<&SchedulerError>,
) {
    match error {
        Some(error) => {
            *failures = failures.saturating_add(1);
            health.send_replace(WorkerHealth {
                status: HealthStatus::Degraded,
                consecutive_failures: *failures,
                last_error: Some(error.to_string()),
            });
        }
        None => {
            *failures = 0;
            health.send_replace(WorkerHealth {
                status: HealthStatus::Healthy,
                consecutive_failures: 0,
                last_error: None,
            });
        }
    }
}

async fn materialize<P: SchedulerPool>(pool: &P) -> Result<(), SchedulerError> {
    let mut connection = conn(pool).await?;
    let count = store::materialize_due_jobs(&mut connection).await?;
    if count > 0 {
        metrics::incr_by(
            metrics::RUNS_MATERIALIZED,
            u64::try_from(count).unwrap_or(u64::MAX),
        );
    }
    Ok(())
}

async fn recover_leases<P: SchedulerPool>(pool: &P) -> Result<(), SchedulerError> {
    let mut connection = conn(pool).await?;
    let summary =
        store::recover_expired(&mut connection, NonZeroUsize::new(1_000).unwrap()).await?;
    if summary.requeued > 0 {
        metrics::incr_by(
            metrics::RUNS_REQUEUED,
            u64::try_from(summary.requeued).unwrap_or(u64::MAX),
        );
    }
    if summary.failed > 0 {
        metrics::incr_by(
            metrics::RUNS_REAPED,
            u64::try_from(summary.failed).unwrap_or(u64::MAX),
        );
    }
    Ok(())
}

async fn claim_and_dispatch<P: SchedulerPool>(
    pool: &P,
    registry: &Arc<Registry>,
    names: &Arc<Vec<String>>,
    worker_id: &WorkerId,
    semaphore: &Arc<Semaphore>,
    in_flight: &mut JoinSet<()>,
) -> Result<(), SchedulerError> {
    let available = semaphore.available_permits();
    let Some(limit) = NonZeroUsize::new(available) else {
        return Ok(());
    };
    let mut connection = conn(pool).await?;
    let claimed = store::claim_batch(&mut connection, worker_id, names, limit).await?;
    drop(connection);

    for task in claimed {
        let permit = semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| SchedulerError::Invariant("claimed more tasks than worker capacity"))?;
        let handler = registry
            .get(&task.job_name)
            .ok_or(SchedulerError::Invariant(
                "claimed task has no registered handler",
            ))?;
        metrics::incr(metrics::RUNS_CLAIMED);
        in_flight.spawn(dispatch(pool.clone(), handler, task, permit));
    }
    Ok(())
}

fn log_handler_join(result: Result<(), tokio::task::JoinError>) {
    if let Err(error) = result
        && !error.is_cancelled()
    {
        tracing::error!(error = %error, "handler task panicked");
    }
}

async fn dispatch<P: SchedulerPool>(
    pool: P,
    handler: Handler,
    claimed: ClaimedRun,
    permit: tokio::sync::OwnedSemaphorePermit,
) {
    let _permit = permit;
    let cancellation = CancellationToken::new();
    let _cancel_on_drop = CancelOnDrop(cancellation.clone());
    let ctx = JobContext {
        run_id: claimed.run_id,
        job_id: claimed.job_id,
        job_name: claimed.job_name.clone(),
        scheduled_for: claimed.scheduled_for,
        attempt: claimed.attempt,
        lease_token: claimed.lease_token,
        lease_expires_at: claimed.lease_expires_at,
        cancellation: cancellation.clone(),
    };
    let mut handler_future = handler(ctx, claimed.job_args.clone());
    let heartbeat = heartbeat_interval(claimed.lease_duration.as_duration());
    let mut lease_expires_at = claimed.lease_expires_at;

    let result = loop {
        tokio::select! {
            result = &mut handler_future => break Some(result),
            _ = tokio::time::sleep(heartbeat) => {
                match pool.acquire().await {
                    Ok(mut connection) => {
                        match store::renew_lease(
                            &mut connection,
                            claimed.run_id,
                            claimed.lease_token,
                        ).await {
                            Ok(RenewalOutcome::Renewed { lease_expires_at: renewed }) => {
                                lease_expires_at = renewed;
                            }
                            Ok(RenewalOutcome::Fenced) => {
                                cancellation.cancel();
                                break None;
                            }
                            Err(error) => {
                                tracing::error!(error = %error, run = %claimed.run_id.0, "lease renewal failed");
                            }
                        }
                    }
                    Err(error) => {
                        tracing::error!(error = %error, run = %claimed.run_id.0, "lease renewal connection failed");
                    }
                }
                if Utc::now() >= lease_expires_at {
                    cancellation.cancel();
                    break None;
                }
            }
        }
    };

    let Some(result) = result else {
        tracing::warn!(run = %claimed.run_id.0, "handler cancelled after lease loss");
        return;
    };
    finalize_handler(&pool, &claimed, result).await;
}

fn heartbeat_interval(lease: Duration) -> Duration {
    let third = lease / 3;
    if third.is_zero() {
        Duration::from_micros(1)
    } else {
        third
    }
}

async fn finalize_handler<P: SchedulerPool>(
    pool: &P,
    claimed: &ClaimedRun,
    result: Result<(), crate::error::JobError>,
) {
    let mut connection = match pool.acquire().await {
        Ok(connection) => connection,
        Err(error) => {
            tracing::error!(error = %error, run = %claimed.run_id.0, "finalize connection failed");
            return;
        }
    };
    match result {
        Ok(()) => match store::finalize_run(
            &mut connection,
            claimed.run_id,
            claimed.lease_token,
            Outcome::Completed,
        )
        .await
        {
            Ok(FinalizeOutcome::Applied) => metrics::incr(metrics::RUNS_COMPLETED),
            Ok(FinalizeOutcome::Fenced | FinalizeOutcome::AlreadyTerminal) => {
                tracing::warn!(run = %claimed.run_id.0, "completion was fenced")
            }
            Err(error) => tracing::error!(error = %error, "completion failed"),
        },
        Err(error) => {
            let retryable = error.is_retryable();
            match store::fail_run(
                &mut connection,
                claimed.run_id,
                claimed.lease_token,
                error.to_string(),
                retryable,
            )
            .await
            {
                Ok(FailureOutcome::Retried { .. }) => metrics::incr(metrics::RUNS_REQUEUED),
                Ok(FailureOutcome::Failed) => metrics::incr(metrics::RUNS_FAILED),
                Ok(FailureOutcome::Fenced | FailureOutcome::AlreadyTerminal) => {
                    tracing::warn!(run = %claimed.run_id.0, "failure was fenced")
                }
                Err(error) => tracing::error!(error = %error, "failure finalization failed"),
            }
        }
    }
}
