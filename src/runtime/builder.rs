use std::num::NonZeroUsize;
use std::time::Duration;

use crate::error::{RegisterError, SchedulerError, TaskError};
use crate::ids::{IdentifierError, QueueName, TaskName, WorkerId};
use crate::models::Retention;
use crate::pool::SchedulerPool;
use crate::queue::Task;
use crate::runtime::context::TaskContext;
use crate::runtime::health::WorkerHealth;
use crate::runtime::registry::Registry;

const SIXTEEN: NonZeroUsize = NonZeroUsize::new(16).unwrap();
const HUNDRED: NonZeroUsize = NonZeroUsize::new(100).unwrap();

/// Longest duration `build()` accepts for any setting: 5 years (5 × 365 days). Every
/// duration becomes a PostgreSQL interval added to or subtracted from `now()`, and a far
/// larger one (such as `Duration::MAX`) fails each such statement with
/// "timestamp out of range".
const MAX_DURATION: Duration = Duration::from_secs(5 * 365 * 86_400);

pub(crate) struct Config {
    pub worker_id: WorkerId,
    /// Served queues and their handler limits, in configuration order.
    pub queues: Vec<(QueueName, NonZeroUsize)>,
    pub claim_batch_size: NonZeroUsize,
    pub lease_duration: Duration,
    pub heartbeat_interval: Duration,
    pub max_poll_interval: Duration,
    pub base_retry_delay: Duration,
    pub max_retry_delay: Duration,
    pub retry_jitter: Duration,
    pub reaper_interval: Duration,
    pub reaper_batch_size: NonZeroUsize,
    pub crash_retry_delay: Duration,
    pub retention: Retention,
    pub shutdown_timeout: Duration,
}

pub struct SchedulerBuilder<P: SchedulerPool> {
    pool: P,
    registry: Registry,
    config: Config,
}

macro_rules! setters {
    ($($(#[$doc:meta])* $name:ident: $ty:ty;)+) => {
        $(
            $(#[$doc])*
            pub fn $name(mut self, value: $ty) -> Self {
                self.config.$name = value;
                self
            }
        )+
    };
}

impl<P: SchedulerPool> SchedulerBuilder<P> {
    setters! {
        /// Most tasks taken per claim statement, per queue (default 16).
        claim_batch_size: NonZeroUsize;
        /// Lease granted by each claim and heartbeat, in PostgreSQL time (default 60 s).
        lease_duration: Duration;
        /// Heartbeat period; at most a third of `lease_duration` (default 20 s).
        heartbeat_interval: Duration;
        /// Longest idle wait between polls (default 1 s).
        max_poll_interval: Duration;
        /// Retry delay after the first attempt, doubling per attempt (default 1 s).
        base_retry_delay: Duration;
        /// Cap on the exponential retry delay, before jitter (default 5 min).
        max_retry_delay: Duration;
        /// Upper bound of the uniform random delay added to each retry (default 1 s).
        retry_jitter: Duration;
        /// Period of the reaper and retention cleanup (default 20 s).
        reaper_interval: Duration;
        /// Expired leases recovered per reaper statement (default 100).
        reaper_batch_size: NonZeroUsize;
        /// Delay before a reaped task is due again (default 10 s).
        crash_retry_delay: Duration;
        /// Retention of terminal rows (default [`Retention::default`]).
        retention: Retention;
        /// Shutdown cancels every handler's `ctx.cancellation`, then waits this long for handlers
        /// to return; those still running are released, for up to 5 s more, then aborted
        /// (default 25 s).
        shutdown_timeout: Duration;
    }

    /// Serve `name`, running at most `limit` of its handlers at once. Configuring any queue
    /// replaces the implicit `default` queue (limit 16). Add `.queue("default", n)` to keep
    /// serving it.
    pub fn queue(
        mut self,
        name: impl TryInto<QueueName, Error: Into<IdentifierError>>,
        limit: NonZeroUsize,
    ) -> Result<Self, IdentifierError> {
        let name = name.try_into().map_err(Into::into)?;
        match self.config.queues.iter_mut().find(|(q, _)| *q == name) {
            Some(entry) => entry.1 = limit,
            None => self.config.queues.push((name, limit)),
        }
        Ok(self)
    }

    /// Register a handler for `name`; the payload is deserialized into `A`.
    pub fn register<A, F, Fut>(
        mut self,
        name: impl TryInto<TaskName, Error: Into<IdentifierError>>,
        handler: F,
    ) -> Result<Self, RegisterError>
    where
        A: serde::de::DeserializeOwned + Send + 'static,
        F: Fn(TaskContext, A) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), TaskError>> + Send + 'static,
    {
        let name = name.try_into().map_err(Into::into)?;
        self.registry.register(name, handler)?;
        Ok(self)
    }

    /// Register a handler for `T::NAME` taking `T::Args`.
    pub fn register_task<T, F, Fut>(self, handler: F) -> Result<Self, RegisterError>
    where
        T: Task,
        F: Fn(TaskContext, T::Args) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), TaskError>> + Send + 'static,
    {
        self.register::<T::Args, _, _>(T::NAME, handler)
    }

    pub fn build(mut self) -> Result<Scheduler<P>, SchedulerError> {
        if self.config.queues.is_empty() {
            self.config.queues.push((QueueName::default(), SIXTEEN));
        }
        let c = &self.config;
        let r = &c.retention;
        let durations = [
            c.lease_duration,
            c.heartbeat_interval,
            c.max_poll_interval,
            c.base_retry_delay,
            c.max_retry_delay,
            c.retry_jitter,
            c.reaper_interval,
            c.crash_retry_delay,
            c.shutdown_timeout,
            r.done,
            r.dead,
            r.idempotency_horizon,
            r.outbox_published,
        ];
        let invalid = if self.registry.is_empty() {
            Some("at least one handler must be registered")
        } else if durations.iter().any(|d| *d > MAX_DURATION) {
            Some("every duration must be at most 5 years")
        } else if [
            c.lease_duration,
            c.heartbeat_interval,
            c.max_poll_interval,
            c.reaper_interval,
        ]
        .contains(&Duration::ZERO)
        {
            Some("lease, heartbeat, poll and reaper intervals must be greater than zero")
        } else if c.heartbeat_interval.saturating_mul(3) > c.lease_duration {
            Some("heartbeat_interval must be at most a third of lease_duration")
        } else if c.base_retry_delay > c.max_retry_delay {
            Some("base_retry_delay must not exceed max_retry_delay")
        } else if c.retention.done < c.retention.idempotency_horizon
            || c.retention.dead < c.retention.idempotency_horizon
        {
            Some("done and dead retention must be at least the idempotency horizon")
        } else {
            None
        };
        if let Some(message) = invalid {
            return Err(SchedulerError::Config(message.into()));
        }
        let (health_tx, _) = tokio::sync::watch::channel(WorkerHealth::default());
        Ok(Scheduler {
            pool: self.pool,
            registry: self.registry,
            config: self.config,
            health_tx,
        })
    }
}

pub struct Scheduler<P: SchedulerPool> {
    pub(crate) pool: P,
    pub(crate) registry: Registry,
    pub(crate) config: Config,
    pub(crate) health_tx: tokio::sync::watch::Sender<WorkerHealth>,
}

impl<P: SchedulerPool> Scheduler<P> {
    /// A builder with the documented defaults and a fresh UUID worker id (§7.1).
    pub fn builder(pool: P) -> SchedulerBuilder<P> {
        SchedulerBuilder {
            pool,
            registry: Registry::new(),
            config: Config {
                worker_id: WorkerId::generate(),
                queues: Vec::new(),
                claim_batch_size: SIXTEEN,
                lease_duration: Duration::from_secs(60),
                heartbeat_interval: Duration::from_secs(20),
                max_poll_interval: Duration::from_secs(1),
                base_retry_delay: Duration::from_secs(1),
                max_retry_delay: Duration::from_secs(300),
                retry_jitter: Duration::from_secs(1),
                reaper_interval: Duration::from_secs(20),
                reaper_batch_size: HUNDRED,
                crash_retry_delay: Duration::from_secs(10),
                retention: Retention::default(),
                shutdown_timeout: Duration::from_secs(25),
            },
        }
    }

    pub fn worker_id(&self) -> &WorkerId {
        &self.config.worker_id
    }

    pub fn health(&self) -> tokio::sync::watch::Receiver<WorkerHealth> {
        self.health_tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Satisfies `SchedulerPool` at the type level only; `build()` never acquires.
    #[derive(Clone)]
    struct FakePool;

    impl SchedulerPool for FakePool {
        type Conn = Box<diesel_async::AsyncPgConnection>;

        async fn acquire(&self) -> Result<Self::Conn, crate::pool::PoolError> {
            panic!("FakePool::acquire must never be called in this test")
        }
    }

    fn builder() -> SchedulerBuilder<FakePool> {
        Scheduler::builder(FakePool)
            .register::<serde_json::Value, _, _>("t", |_c, _a| async { Ok(()) })
            .unwrap()
    }

    fn rejected(b: SchedulerBuilder<FakePool>) -> bool {
        matches!(b.build(), Err(SchedulerError::Config(_)))
    }

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn n(v: usize) -> NonZeroUsize {
        NonZeroUsize::new(v).unwrap()
    }

    #[test]
    fn default_queue_when_none_configured() {
        let s = builder().build().unwrap();
        assert_eq!(s.config.queues, vec![(QueueName::default(), SIXTEEN)]);
    }

    #[test]
    fn configured_queue_replaces_default_and_last_limit_wins() {
        let s = builder()
            .queue("bulk", n(2))
            .unwrap()
            .queue("bulk", n(3))
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            s.config.queues,
            vec![(QueueName::try_from("bulk").unwrap(), n(3))]
        );
    }

    #[test]
    fn queue_rejects_invalid_name() {
        assert!(matches!(
            builder().queue("", n(1)),
            Err(IdentifierError::Empty)
        ));
    }

    #[test]
    fn build_errors_on_empty_registry() {
        assert!(rejected(Scheduler::builder(FakePool)));
    }

    #[test]
    fn defaults_build() {
        assert!(builder().build().is_ok());
    }

    #[test]
    fn builder_rejects_duplicate_registration() {
        let err = builder()
            .register::<serde_json::Value, _, _>("t", |_c, _a| async { Ok(()) })
            .err()
            .unwrap();
        assert!(matches!(err, RegisterError::Duplicate(_)), "got {err:?}");
    }

    #[test]
    fn builder_rejects_invalid_name() {
        let err = Scheduler::builder(FakePool)
            .register::<serde_json::Value, _, _>("", |_c, _a| async { Ok(()) })
            .err()
            .unwrap();
        assert!(
            matches!(err, RegisterError::Name(IdentifierError::Empty)),
            "got {err:?}"
        );
    }

    /// Guards the `impl TryInto<TaskName, Error: Into<IdentifierError>>` signature.
    #[test]
    fn builder_accepts_prebuilt_task_name() {
        let name = TaskName::try_from("built").unwrap();
        let result = Scheduler::builder(FakePool)
            .register::<serde_json::Value, _, _>(name, |_c, _a| async { Ok(()) });
        assert!(result.is_ok());
    }

    #[test]
    fn build_rejects_heartbeat_not_below_a_third_of_lease() {
        let lease = || builder().lease_duration(secs(60));
        assert!(rejected(lease().heartbeat_interval(secs(21))));
        assert!(lease().heartbeat_interval(secs(20)).build().is_ok());
    }

    #[test]
    fn build_rejects_retention_shorter_than_idempotency_horizon() {
        let r = Retention::default();
        let short = r.idempotency_horizon - secs(1);
        assert!(rejected(
            builder().retention(Retention { done: short, ..r })
        ));
        assert!(rejected(
            builder().retention(Retention { dead: short, ..r })
        ));
        let at_horizon = Retention {
            done: r.idempotency_horizon,
            dead: r.idempotency_horizon,
            ..r
        };
        assert!(builder().retention(at_horizon).build().is_ok());
    }

    #[test]
    fn build_rejects_base_retry_above_max() {
        assert!(rejected(
            builder()
                .base_retry_delay(secs(10))
                .max_retry_delay(secs(5))
        ));
    }

    #[test]
    fn build_rejects_durations_over_5_years() {
        assert_eq!(MAX_DURATION, secs(157_680_000));
        let over = MAX_DURATION + secs(1);
        let r = Retention::default();
        let horizon = |d| Retention {
            done: d,
            dead: d,
            idempotency_horizon: d,
            ..r
        };
        // Each setting, with any companion value its other rules need. heartbeat_interval is
        // absent: at most lease / 3, it can never reach the cap without the lease passing it.
        type Set = fn(SchedulerBuilder<FakePool>, Duration) -> SchedulerBuilder<FakePool>;
        let settings: [Set; 11] = [
            |b, d| b.lease_duration(d),
            |b, d| b.max_poll_interval(d),
            |b, d| b.base_retry_delay(d).max_retry_delay(d),
            |b, d| b.max_retry_delay(d),
            |b, d| b.retry_jitter(d),
            |b, d| b.reaper_interval(d),
            |b, d| b.crash_retry_delay(d),
            |b, d| b.shutdown_timeout(d),
            |b, d| {
                b.retention(Retention {
                    done: d,
                    ..Retention::default()
                })
            },
            |b, d| {
                b.retention(Retention {
                    dead: d,
                    ..Retention::default()
                })
            },
            |b, d| {
                b.retention(Retention {
                    outbox_published: d,
                    ..Retention::default()
                })
            },
        ];
        for (i, set) in settings.into_iter().enumerate() {
            assert!(
                set(builder(), MAX_DURATION).build().is_ok(),
                "setting {i} at the cap"
            );
            assert!(rejected(set(builder(), over)), "setting {i} over the cap");
        }
        assert!(rejected(builder().retention(horizon(over))));
        assert!(builder().retention(horizon(MAX_DURATION)).build().is_ok());
        assert!(rejected(builder().max_retry_delay(Duration::MAX)));
    }

    #[test]
    fn build_rejects_zero_intervals() {
        assert!(rejected(builder().lease_duration(Duration::ZERO)));
        assert!(rejected(builder().heartbeat_interval(Duration::ZERO)));
        assert!(rejected(builder().max_poll_interval(Duration::ZERO)));
        assert!(rejected(builder().reaper_interval(Duration::ZERO)));
    }
}
