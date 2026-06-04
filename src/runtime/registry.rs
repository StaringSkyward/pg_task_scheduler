use std::collections::HashMap;
use std::sync::Arc;

use futures::future::BoxFuture;
use serde::de::DeserializeOwned;

use crate::error::JobError;
use crate::ids::JobName;
use crate::runtime::context::JobContext;

pub type Handler = Arc<
    dyn Fn(JobContext, serde_json::Value) -> BoxFuture<'static, Result<(), JobError>> + Send + Sync,
>;

#[derive(Clone, Default)]
pub struct Registry {
    handlers: HashMap<JobName, Handler>,
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
    /// never a panic.
    pub fn register<A, F, Fut>(&mut self, name: JobName, handler: F)
    where
        A: DeserializeOwned + Send + 'static,
        F: Fn(JobContext, A) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<(), JobError>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let boxed: Handler = Arc::new(move |ctx, value| {
            let handler = handler.clone();
            match serde_json::from_value::<A>(value) {
                Ok(args) => Box::pin(async move { handler(ctx, args).await }),
                Err(e) => Box::pin(async move { Err(JobError::from(e)) }),
            }
        });
        self.handlers.insert(name, boxed);
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
            job_id: JobId(uuid::Uuid::new_v4()),
            job_name: JobName::new("t"),
            scheduled_for: Utc::now(),
            attempt: NonZeroU32::new(1).unwrap(),
            lease_token: LeaseToken::generate(),
            lease_expires_at: Utc::now(),
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
        reg.register::<Args, _, _>(JobName::new("t"), |_ctx, a| async move {
            SUM.fetch_add(a.n, Ordering::SeqCst);
            Ok(())
        });
        reg.get(&JobName::new("t")).unwrap()(ctx(), serde_json::json!({"n": 5}))
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
        reg.register::<Args, _, _>(JobName::new("t"), |_c, _a: Args| async { Ok(()) });
        let r =
            reg.get(&JobName::new("t")).unwrap()(ctx(), serde_json::json!({"wrong": true})).await;
        assert!(r.is_err());
    }
}
