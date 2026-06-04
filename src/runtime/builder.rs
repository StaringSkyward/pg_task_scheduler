use std::num::NonZeroUsize;
use std::time::Duration;

use crate::error::{JobError, RegisterError, SchedulerError};
use crate::ids::{IdentifierError, JobName, WorkerId};
use crate::pool::SchedulerPool;
use crate::runtime::context::JobContext;
use crate::runtime::registry::Registry;

/// Compile-time constant so there is no runtime partiality (no `unwrap`/`expect`
/// in production paths). `const Option::unwrap` is stable since Rust 1.64.
const DEFAULT_MAX_CONCURRENCY: NonZeroUsize = NonZeroUsize::new(16).unwrap();

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub worker_id: WorkerId,
    pub poll_interval: Duration,
    pub reaper_interval: Duration,
    pub shutdown_timeout: Duration,
    pub max_concurrency: NonZeroUsize,
}

pub struct SchedulerBuilder<P: SchedulerPool> {
    pool: P,
    registry: Registry,
    worker_id: WorkerId,
    poll_interval: Duration,
    reaper_interval: Duration,
    shutdown_timeout: Duration,
    max_concurrency: NonZeroUsize,
}

impl<P: SchedulerPool + std::fmt::Debug> std::fmt::Debug for SchedulerBuilder<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchedulerBuilder")
            .field("registry", &self.registry)
            .field("worker_id", &self.worker_id)
            .field("poll_interval", &self.poll_interval)
            .field("reaper_interval", &self.reaper_interval)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .field("max_concurrency", &self.max_concurrency)
            .finish_non_exhaustive()
    }
}

impl<P: SchedulerPool> SchedulerBuilder<P> {
    pub(crate) fn new(pool: P, worker_id: WorkerId) -> Self {
        Self {
            pool,
            registry: Registry::new(),
            worker_id,
            poll_interval: Duration::from_secs(1),
            reaper_interval: Duration::from_secs(20),
            shutdown_timeout: Duration::from_secs(25),
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
        }
    }

    pub fn poll_interval(mut self, d: Duration) -> Self {
        self.poll_interval = d;
        self
    }

    pub fn reaper_interval(mut self, d: Duration) -> Self {
        self.reaper_interval = d;
        self
    }

    pub fn shutdown_timeout(mut self, d: Duration) -> Self {
        self.shutdown_timeout = d;
        self
    }

    pub fn max_concurrency(mut self, n: NonZeroUsize) -> Self {
        self.max_concurrency = n;
        self
    }

    pub fn register<A, F, Fut>(
        mut self,
        name: impl TryInto<JobName, Error: Into<IdentifierError>>,
        handler: F,
    ) -> Result<Self, RegisterError>
    where
        A: serde::de::DeserializeOwned + Send + 'static,
        F: Fn(JobContext, A) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<(), JobError>> + Send + 'static,
    {
        let name = name.try_into().map_err(Into::into)?; // E -> IdentifierError -> RegisterError::Name
        self.registry.register(name, handler)?; // DuplicateJobName -> RegisterError::Duplicate
        Ok(self)
    }

    pub fn build(self) -> Result<Scheduler<P>, SchedulerError> {
        if self.registry.is_empty() {
            return Err(SchedulerError::Config(
                "at least one handler must be registered".into(),
            ));
        }
        Ok(Scheduler {
            pool: self.pool,
            registry: self.registry,
            config: Config {
                worker_id: self.worker_id,
                poll_interval: self.poll_interval,
                reaper_interval: self.reaper_interval,
                shutdown_timeout: self.shutdown_timeout,
                max_concurrency: self.max_concurrency,
            },
        })
    }
}

pub struct Scheduler<P: SchedulerPool> {
    pub(crate) pool: P,
    pub(crate) registry: Registry,
    pub(crate) config: Config,
}

impl<P: SchedulerPool> Scheduler<P> {
    /// `worker_id` is a required constructor argument — a `Scheduler` cannot
    /// exist without one (no `Option`, no runtime "missing worker_id" error).
    pub fn builder(pool: P, worker_id: WorkerId) -> SchedulerBuilder<P> {
        SchedulerBuilder::new(pool, worker_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal fake pool satisfying `SchedulerPool` at the type level; it never
    /// acquires a connection (used only so the builder can be constructed in tests).
    #[derive(Clone, Debug)]
    struct FakePool;

    impl crate::pool::SchedulerPool for FakePool {
        // Box<AsyncPgConnection> satisfies DerefMut<Target = AsyncPgConnection>.
        type Conn = Box<diesel_async::AsyncPgConnection>;

        async fn acquire(&self) -> Result<Self::Conn, crate::pool::PoolError> {
            panic!("FakePool::acquire must never be called in this test")
        }
    }

    /// `build()` returns `SchedulerError::Config` when no handler is registered.
    #[test]
    fn build_errors_on_empty_registry() {
        let worker_id = WorkerId::try_from("test-worker").unwrap();
        let result = Scheduler::<FakePool>::builder(FakePool, worker_id).build();

        match result {
            Err(SchedulerError::Config(_)) => {} // expected
            Err(other) => panic!("expected SchedulerError::Config, got {other:?}"),
            Ok(_) => panic!("build() must fail when no handlers are registered"),
        }
    }

    /// Registering the same name twice is rejected at the second `register`.
    #[test]
    fn builder_rejects_duplicate_registration() {
        let builder = Scheduler::<FakePool>::builder(FakePool, WorkerId::try_from("w").unwrap())
            .register::<serde_json::Value, _, _>("dup", |_c, _a| async { Ok(()) })
            .unwrap();
        let err = builder
            .register::<serde_json::Value, _, _>("dup", |_c, _a| async { Ok(()) })
            .unwrap_err();
        assert!(matches!(err, RegisterError::Duplicate(_)), "got {err:?}");
    }

    /// An invalid job name still surfaces, now through `RegisterError::Name`.
    #[test]
    fn builder_rejects_invalid_name() {
        let err = Scheduler::<FakePool>::builder(FakePool, WorkerId::try_from("w").unwrap())
            .register::<serde_json::Value, _, _>("", |_c, _a| async { Ok(()) })
            .unwrap_err();
        assert!(
            matches!(err, RegisterError::Name(IdentifierError::Empty)),
            "got {err:?}"
        );
    }

    /// Guards the `impl TryInto<JobName, Error: Into<IdentifierError>>` signature:
    /// an already-constructed `JobName` is still accepted by `register`.
    #[test]
    fn builder_accepts_prebuilt_job_name() {
        let name = JobName::try_from("built").unwrap();
        let result = Scheduler::<FakePool>::builder(FakePool, WorkerId::try_from("w").unwrap())
            .register::<serde_json::Value, _, _>(name, |_c, _a| async { Ok(()) });
        assert!(result.is_ok());
    }

    /// `DEFAULT_MAX_CONCURRENCY` evaluates to 16 (compile-time const sanity check).
    #[test]
    fn default_concurrency_is_16() {
        assert_eq!(DEFAULT_MAX_CONCURRENCY.get(), 16);
    }
}
