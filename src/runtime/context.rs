use chrono::DateTime;
use chrono::Utc;

use crate::ids::TaskKey;
use crate::ids::TaskName;
use crate::models::TaskLease;

/// What a handler knows about the task it is running, passed as its first argument.
#[derive(Debug, Clone)]
pub struct TaskContext {
    /// Ownership of this attempt. Pass it to [`store::complete`](crate::store::complete)
    /// and [`store::lock_owned`](crate::store::lock_owned) to finish the task in the
    /// handler's own transaction.
    pub lease: TaskLease,
    /// The producer's idempotency key; stable across retries, so use it as the
    /// idempotency key of external effects.
    pub task_key: TaskKey,
    /// The task's name.
    pub task_name: TaskName,
    /// Attempts allowed in total; `lease.attempt_no` is this attempt.
    pub max_attempts: i32,
    /// Lease expiry as of claim; not updated by heartbeats.
    pub locked_until: DateTime<Utc>,
    /// Cancelled when worker shutdown begins, when a heartbeat proves this
    /// worker no longer owns the task (its lease was lost, or an operator
    /// cancelled it), or when the dispatch ends. In the lost-ownership case the
    /// handler future is dropped before the token fires, so the handler itself
    /// never observes it; only work it spawned with a clone of the token does.
    /// A handler that stops early because shutdown began should return
    /// `TaskError::interrupted()`.
    pub cancellation: tokio_util::sync::CancellationToken,
}
