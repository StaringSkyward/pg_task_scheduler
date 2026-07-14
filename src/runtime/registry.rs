use std::collections::HashMap;
use std::sync::Arc;

use futures::future::BoxFuture;
use serde::de::DeserializeOwned;

use crate::error::{DuplicateJobName, JobError};
use crate::ids::JobName;
use crate::runtime::context::JobContext;

pub type Handler = Arc<
    dyn Fn(JobContext, serde_json::Value) -> BoxFuture<'static, Result<(), JobError>> + Send + Sync,
>;

#[derive(Clone, Default)]
pub struct Registry {
    handlers: HashMap<JobName, Handler>,
}

impl std::fmt::Debug for Registry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registry")
            .field("handler_count", &self.handlers.len())
            .finish()
    }
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }

    /// Returns the inner job-name strings, for use in the `ANY($1)` claim filter.
    pub fn names(&self) -> Vec<String> {
        self.handlers
            .keys()
            .map(|n| n.as_str().to_owned())
            .collect()
    }

    pub fn get(&self, name: &JobName) -> Option<Handler> {
        self.handlers.get(name).cloned()
    }

    /// Register a typed handler. `job_args` (a `serde_json::Value`) is deserialized
    /// into `A` at this boundary. A deserialize failure becomes `Err(JobError::from(e))`,
    /// never a panic. Returns `Err(DuplicateJobName)` if a handler is already registered
    /// for `name`; the first handler wins and is never replaced.
    pub fn register<A, F, Fut>(&mut self, name: JobName, handler: F) -> Result<(), DuplicateJobName>
    where
        A: DeserializeOwned + Send + 'static,
        F: Fn(JobContext, A) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<(), JobError>> + Send + 'static,
    {
        if self.handlers.contains_key(&name) {
            return Err(DuplicateJobName(name)); // first handler wins; do not insert
        }
        let handler = Arc::new(handler);
        let boxed: Handler = Arc::new(move |ctx, value| {
            let handler = handler.clone();
            match serde_json::from_value::<A>(value) {
                Ok(args) => Box::pin(async move { handler(ctx, args).await }),
                Err(e) => Box::pin(async move { Err(JobError::from(e)) }),
            }
        });
        self.handlers.insert(name, boxed);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{JobId, LeaseToken, RunId};
    use chrono::Utc;
    use std::num::NonZeroU32;
    use std::sync::atomic::{AtomicI64, Ordering};

    fn ctx() -> JobContext {
        JobContext {
            run_id: RunId(uuid::Uuid::new_v4()),
            job_id: Some(JobId(uuid::Uuid::new_v4())),
            job_name: JobName::try_from("t").unwrap(),
            scheduled_for: Utc::now(),
            attempt: NonZeroU32::new(1).unwrap(),
            lease_token: LeaseToken::generate(),
            lease_expires_at: Utc::now(),
            cancellation: tokio_util::sync::CancellationToken::new(),
        }
    }

    #[tokio::test]
    async fn typed_args_are_deserialized() {
        #[derive(serde::Deserialize)]
        struct Args {
            n: i64,
        }
        static SUM: AtomicI64 = AtomicI64::new(0);
        let mut reg = Registry::new();
        reg.register::<Args, _, _>(JobName::try_from("t").unwrap(), |_ctx, a| async move {
            SUM.fetch_add(a.n, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
        reg.get(&JobName::try_from("t").unwrap()).unwrap()(ctx(), serde_json::json!({"n": 5}))
            .await
            .unwrap();
        assert_eq!(SUM.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn bad_args_yield_job_error() {
        #[derive(serde::Deserialize)]
        struct Args {
            _n: i64,
        }
        let mut reg = Registry::new();
        reg.register::<Args, _, _>(JobName::try_from("t").unwrap(), |_c, _a: Args| async {
            Ok(())
        })
        .unwrap();
        let r = reg.get(&JobName::try_from("t").unwrap()).unwrap()(
            ctx(),
            serde_json::json!({"wrong": true}),
        )
        .await;
        assert!(r.is_err());
    }

    #[tokio::test]
    async fn duplicate_registration_is_rejected_first_wins() {
        static RAN: AtomicI64 = AtomicI64::new(0);
        let mut reg = Registry::new();
        // First handler for "x" increments by 1.
        reg.register::<serde_json::Value, _, _>(JobName::try_from("x").unwrap(), |_c, _a| async {
            RAN.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
        // A second handler for "x" (would increment by 100) must be rejected.
        let err = reg
            .register::<serde_json::Value, _, _>(JobName::try_from("x").unwrap(), |_c, _a| async {
                RAN.fetch_add(100, Ordering::SeqCst);
                Ok(())
            })
            .unwrap_err();
        assert_eq!(err, DuplicateJobName(JobName::try_from("x").unwrap()));
        // First-wins: the retained handler is the FIRST one.
        reg.get(&JobName::try_from("x").unwrap()).unwrap()(ctx(), serde_json::json!({}))
            .await
            .unwrap();
        assert_eq!(RAN.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn distinct_names_both_register() {
        let mut reg = Registry::new();
        reg.register::<serde_json::Value, _, _>(JobName::try_from("a").unwrap(), |_c, _a| async {
            Ok(())
        })
        .unwrap();
        reg.register::<serde_json::Value, _, _>(JobName::try_from("b").unwrap(), |_c, _a| async {
            Ok(())
        })
        .unwrap();
        assert!(reg.get(&JobName::try_from("a").unwrap()).is_some());
        assert!(reg.get(&JobName::try_from("b").unwrap()).is_some());
    }
}
