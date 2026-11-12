use std::collections::HashMap;
use std::sync::Arc;

use futures::future::BoxFuture;
use serde::de::DeserializeOwned;

use crate::error::DuplicateTaskName;
use crate::error::TaskError;
use crate::ids::TaskName;
use crate::runtime::context::TaskContext;

pub type Handler = Arc<
    dyn Fn(TaskContext, serde_json::Value) -> BoxFuture<'static, Result<(), TaskError>>
        + Send
        + Sync,
>;

#[derive(Clone, Default)]
pub struct Registry {
    handlers: HashMap<TaskName, Handler>,
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

    /// Returns the inner task-name strings, for use in the `ANY($1)` claim filter.
    pub fn names(&self) -> Vec<String> {
        self.handlers
            .keys()
            .map(|n| n.as_str().to_owned())
            .collect()
    }

    pub fn get(&self, name: &TaskName) -> Option<Handler> {
        self.handlers.get(name).cloned()
    }

    /// Register a typed handler. `payload` (a `serde_json::Value`) is deserialized
    /// into `A` at this boundary. A deserialize failure becomes `Err(TaskError::from(e))`,
    /// never a panic. Returns `Err(DuplicateTaskName)` if a handler is already registered
    /// for `name`; the first handler wins and is never replaced.
    pub fn register<A, F, Fut>(
        &mut self,
        name: TaskName,
        handler: F,
    ) -> Result<(), DuplicateTaskName>
    where
        A: DeserializeOwned + Send + 'static,
        F: Fn(TaskContext, A) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<(), TaskError>> + Send + 'static,
    {
        if self.handlers.contains_key(&name) {
            return Err(DuplicateTaskName(name)); // first handler wins; do not insert
        }
        let handler = Arc::new(handler);
        let boxed: Handler = Arc::new(move |ctx, value| {
            let handler = handler.clone();
            match serde_json::from_value::<A>(value) {
                Ok(args) => Box::pin(async move { handler(ctx, args).await }),
                Err(e) => Box::pin(async move { Err(TaskError::from(e)) }),
            }
        });
        self.handlers.insert(name, boxed);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicI64;
    use std::sync::atomic::Ordering;

    use chrono::Utc;

    use super::*;
    use crate::ids::LockToken;
    use crate::ids::TaskId;
    use crate::ids::TaskKey;
    use crate::ids::WorkerId;
    use crate::models::TaskLease;

    fn ctx() -> TaskContext {
        TaskContext {
            lease: TaskLease {
                task_id: TaskId(1),
                worker_id: WorkerId::generate(),
                lock_token: LockToken(1),
                attempt_no: 1,
            },
            task_key: TaskKey::try_from("k").unwrap(),
            task_name: TaskName::try_from("t").unwrap(),
            max_attempts: 3,
            locked_until: Utc::now(),
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
        reg.register::<Args, _, _>(TaskName::try_from("t").unwrap(), |_ctx, a| async move {
            SUM.fetch_add(a.n, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
        reg.get(&TaskName::try_from("t").unwrap()).unwrap()(ctx(), serde_json::json!({"n": 5}))
            .await
            .unwrap();
        assert_eq!(SUM.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn bad_args_yield_task_error() {
        #[derive(serde::Deserialize)]
        struct Args {
            _n: i64,
        }
        let mut reg = Registry::new();
        reg.register::<Args, _, _>(TaskName::try_from("t").unwrap(), |_c, _a: Args| async {
            Ok(())
        })
        .unwrap();
        let r = reg.get(&TaskName::try_from("t").unwrap()).unwrap()(
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
        reg.register::<serde_json::Value, _, _>(TaskName::try_from("x").unwrap(), |_c, _a| async {
            RAN.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
        // A second handler for "x" (would increment by 100) must be rejected.
        let err = reg
            .register::<serde_json::Value, _, _>(TaskName::try_from("x").unwrap(), |_c, _a| async {
                RAN.fetch_add(100, Ordering::SeqCst);
                Ok(())
            })
            .unwrap_err();
        assert_eq!(err, DuplicateTaskName(TaskName::try_from("x").unwrap()));
        // First-wins: the retained handler is the FIRST one.
        reg.get(&TaskName::try_from("x").unwrap()).unwrap()(ctx(), serde_json::json!({}))
            .await
            .unwrap();
        assert_eq!(RAN.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn distinct_names_both_register() {
        let mut reg = Registry::new();
        reg.register::<serde_json::Value, _, _>(TaskName::try_from("a").unwrap(), |_c, _a| async {
            Ok(())
        })
        .unwrap();
        reg.register::<serde_json::Value, _, _>(TaskName::try_from("b").unwrap(), |_c, _a| async {
            Ok(())
        })
        .unwrap();
        assert!(reg.get(&TaskName::try_from("a").unwrap()).is_some());
        assert!(reg.get(&TaskName::try_from("b").unwrap()).is_some());
    }
}
