use std::sync::Arc;

use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::error::SchedulerError;
use crate::ids::WorkerId;
use crate::metrics;
use crate::models::{ClaimedRun, Outcome};
use crate::pool::SchedulerPool;
use crate::runtime::builder::Scheduler;
use crate::runtime::context::JobContext;
use crate::runtime::registry::Registry;
use crate::store;

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
        } = self;
        let registry = Arc::new(registry);
        let names = Arc::new(registry.names());
        let semaphore = Arc::new(Semaphore::new(config.max_concurrency.get()));
        let mut in_flight = JoinSet::new();

        let reaper = spawn_reaper(pool.clone(), config.reaper_interval, cancel.clone());

        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(config.poll_interval) => {
                    if let Ok(mut c) = conn(&pool).await {
                        match store::materialize_due_jobs(&mut c).await {
                            Ok(n) if n > 0 => {
                                metrics::incr_by(
                                    metrics::RUNS_MATERIALIZED,
                                    u64::try_from(n).unwrap_or(u64::MAX),
                                );
                            }
                            Ok(_) => {}
                            Err(e) => tracing::error!(error = %e, "materialize tick failed"),
                        }
                    }
                    claim_and_dispatch(&pool, &registry, &names, &config.worker_id,
                                       &semaphore, &mut in_flight, &cancel).await;
                    while let Some(res) = in_flight.try_join_next() {
                        if let Err(e) = res { tracing::error!(error = %e, "handler join error"); }
                    }
                }
            }
        }

        tracing::info!("draining in-flight handlers");
        let _ = tokio::time::timeout(config.shutdown_timeout, async {
            while in_flight.join_next().await.is_some() {}
        })
        .await;
        in_flight.shutdown().await;
        reaper.abort();
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
async fn claim_and_dispatch<P: SchedulerPool>(
    pool: &P,
    registry: &Arc<Registry>,
    names: &Arc<Vec<String>>,
    worker_id: &WorkerId,
    semaphore: &Arc<Semaphore>,
    in_flight: &mut JoinSet<()>,
    cancel: &CancellationToken,
) {
    loop {
        if cancel.is_cancelled() {
            break;
        }
        let Ok(permit) = semaphore.clone().try_acquire_owned() else {
            break;
        };
        let mut c = match conn(pool).await {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(error = %e, "claim: no connection");
                break;
            }
        };
        match store::claim_one(&mut c, worker_id, names).await {
            Ok(Some(claimed)) => {
                metrics::incr(metrics::RUNS_CLAIMED);
                drop(c);
                let fut = dispatch(pool.clone(), registry.clone(), claimed, permit);
                in_flight.spawn(fut);
            }
            Ok(None) => break,
            Err(e) => {
                tracing::error!(error = %e, "claim failed");
                break;
            }
        }
    }
}

async fn dispatch<P: SchedulerPool>(
    pool: P,
    registry: Arc<Registry>,
    claimed: ClaimedRun,
    permit: tokio::sync::OwnedSemaphorePermit,
) {
    let _permit = permit;
    let outcome = match registry.get(&claimed.job_name) {
        Some(handler) => {
            let ctx = JobContext {
                run_id: claimed.run_id,
                job_id: claimed.job_id,
                job_name: claimed.job_name.clone(),
                scheduled_for: claimed.scheduled_for,
                attempt: claimed.attempt,
                lease_token: claimed.lease_token,
                lease_expires_at: claimed.lease_expires_at,
            };
            let args = claimed.job_args.clone();
            match tokio::spawn(async move { handler(ctx, args).await }).await {
                Ok(Ok(())) => Outcome::Completed,
                Ok(Err(e)) => Outcome::Failed(e.to_string()),
                Err(join) => Outcome::Failed(format!("handler panicked: {join}")),
            }
        }
        // We only claim jobs whose names we registered, so this is unreachable;
        // surface loudly rather than silently and fail the run so it doesn't loop.
        None => {
            tracing::error!(
                job = claimed.job_name.as_str(),
                "claimed run with no handler (invariant)"
            );
            Outcome::Failed("internal: no handler for claimed job".into())
        }
    };

    match &outcome {
        Outcome::Completed => metrics::incr(metrics::RUNS_COMPLETED),
        Outcome::Failed(_) => metrics::incr(metrics::RUNS_FAILED),
    }

    match pool.acquire().await {
        Ok(mut c) => {
            match store::finalize_run(&mut c, claimed.run_id, claimed.lease_token, outcome).await {
                Ok(true) => {}
                Ok(false) => tracing::warn!(run = %claimed.run_id.0, "finalize fenced out"),
                Err(e) => tracing::error!(error = %e, "finalize failed"),
            }
        }
        Err(e) => tracing::error!(error = %e, "finalize: no connection"),
    }
}

fn spawn_reaper<P: SchedulerPool>(
    pool: P,
    interval: std::time::Duration,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(interval) => {
                    match pool.acquire().await {
                        Ok(mut c) => match store::reap_expired(&mut c).await {
                            Ok(n) if n > 0 => {
                                metrics::incr_by(
                                    metrics::RUNS_REAPED,
                                    u64::try_from(n).unwrap_or(u64::MAX),
                                );
                            }
                            Ok(_) => {}
                            Err(e) => tracing::error!(error = %e, "reaper failed"),
                        },
                        Err(e) => tracing::error!(error = %e, "reaper: no connection"),
                    }
                }
            }
        }
    })
}
