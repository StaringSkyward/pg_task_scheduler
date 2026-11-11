use crate::ids::{TaskKey, TaskName};
use crate::models::TaskLease;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct TaskContext {
    pub lease: TaskLease,
    pub task_key: TaskKey,
    pub task_name: TaskName,
    pub max_attempts: i32,
    /// Lease expiry as of claim; not updated by heartbeats.
    pub locked_until: DateTime<Utc>,
    /// Cancelled when heartbeat proves that this worker no longer owns the
    /// task, or when worker shutdown aborts the handler.
    pub cancellation: tokio_util::sync::CancellationToken,
}
