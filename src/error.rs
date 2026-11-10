//! Crate error types.

#[cfg(feature = "cron")]
use crate::ids::JobId;
use crate::ids::{IdentifierError, TaskId, TaskKey, TaskName};
#[cfg(feature = "cron")]
use crate::models::MaxAttemptsError;

#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    #[error("database error: {0}")]
    Database(#[from] diesel::result::Error),
    #[error("connection pool error: {0}")]
    Pool(String),
    #[cfg(feature = "cron")]
    #[error("cron parse error: {0}")]
    Cron(String),
    #[error("configuration error: {0}")]
    Config(String),
    #[error("invalid identifier: {0}")]
    Identifier(#[from] IdentifierError),
    #[error(transparent)]
    Register(#[from] RegisterError),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    /// A structural guarantee was violated (should be impossible); surfaced loudly.
    #[error("invariant violated: {0}")]
    Invariant(&'static str),
    /// The same `task_key` was enqueued with a different task name or payload.
    #[error("idempotency conflict for task key {}", .0.as_str())]
    IdempotencyConflict(TaskKey),
    /// A worker-owned mutation matched no row: the lease was lost or expired.
    #[error("lost ownership of task {0:?}")]
    LostOwnership(TaskId),
    /// A persisted `scheduler_jobs` row failed to project into a domain `Job`
    /// because a stored value violates an invariant the scheduler guarantees on
    /// write (e.g. the row was edited directly in SQL).
    #[cfg(feature = "cron")]
    #[error("corrupt job row {job_id:?}: {source}")]
    CorruptJob {
        job_id: JobId,
        #[source]
        source: CorruptJobRow,
    },
}

/// Why a stored `scheduler_jobs` row failed to project into a domain `Job`. Each
/// arm is a value the scheduler guarantees on write, so reaching one means the row
/// was corrupted. The cron arm is a `String` because cron errors are stringly
/// crate-wide (`src/cron.rs`); tightening that is a separate finding.
#[cfg(feature = "cron")]
#[derive(Debug, thiserror::Error)]
pub enum CorruptJobRow {
    #[error("unparseable cron: {0}")]
    Cron(String),
    #[error(transparent)]
    MaxAttempts(#[from] MaxAttemptsError),
}

/// A handler was already registered for this task name.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("a handler is already registered for task name {}", .0.as_str())]
pub struct DuplicateTaskName(pub TaskName);

/// Why `SchedulerBuilder::register` rejected a registration.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RegisterError {
    #[error(transparent)]
    Name(#[from] IdentifierError),
    #[error(transparent)]
    Duplicate(#[from] DuplicateTaskName),
}

/// Opaque handler error. Deliberately does NOT implement `std::error::Error`
/// (same trick as `anyhow::Error`) so the blanket `From<E: Error>` below is
/// coherent — if `TaskError` were itself an `Error`, that blanket would collide
/// with the std reflexive `From<TaskError> for TaskError`. Its `Display` is what
/// gets stored in `queue_tasks.last_error`.
pub struct TaskError {
    source: Box<dyn std::error::Error + Send + Sync>,
    retryable: bool,
}

impl TaskError {
    pub fn permanent(message: impl Into<String>) -> Self {
        Self {
            source: message.into().into(),
            retryable: false,
        }
    }

    pub fn retry(message: impl Into<String>) -> Self {
        Self {
            source: message.into().into(),
            retryable: true,
        }
    }

    pub fn is_retryable(&self) -> bool {
        self.retryable
    }
}

impl std::fmt::Debug for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskError")
            .field("source", &self.source)
            .field("retryable", &self.retryable)
            .finish()
    }
}

impl std::fmt::Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.source, f)
    }
}

impl<E: std::error::Error + Send + Sync + 'static> From<E> for TaskError {
    fn from(e: E) -> Self {
        TaskError {
            source: Box::new(e),
            retryable: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_error_from_std_error() {
        let serde_err = serde_json::from_str::<i32>("x").unwrap_err();
        let task_err = TaskError::from(serde_err);
        // Should produce a non-empty display string from the inner error.
        assert!(!task_err.to_string().is_empty());
    }

    #[test]
    fn task_error_retry_flag() {
        assert!(TaskError::retry("e").is_retryable());
        assert!(!TaskError::permanent("e").is_retryable());
        assert_eq!(TaskError::permanent("e").to_string(), "e");
    }

    #[cfg(feature = "cron")]
    #[test]
    fn corrupt_job_row_from_max_attempts() {
        let e: CorruptJobRow = MaxAttemptsError::NonPositive.into();
        assert!(e.to_string().contains("max_attempts"));
    }
}
