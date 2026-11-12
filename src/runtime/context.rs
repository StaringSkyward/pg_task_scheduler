use chrono::DateTime;
use chrono::Utc;

use crate::ids::TaskKey;
use crate::ids::TaskName;
use crate::models::TaskLease;

#[derive(Debug, Clone)]
pub struct TaskContext {
    pub lease: TaskLease,
    pub task_key: TaskKey,
    pub task_name: TaskName,
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
