//! Crate error types.

use crate::ids::IdentifierError;
#[cfg(feature = "cron")]
use crate::ids::JobId;
use crate::ids::TaskId;
use crate::ids::TaskKey;
use crate::ids::TaskName;
#[cfg(feature = "cron")]
use crate::models::MaxAttemptsError;

/// The error type of every fallible scheduler, store and enqueue operation.
#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    /// A query failed.
    #[error("database error: {0}")]
    Database(#[from] diesel::result::Error),
    /// The connection pool could not hand out a connection.
    #[error("connection pool error: {0}")]
    Pool(String),
    /// A cron expression did not parse.
    #[cfg(feature = "cron")]
    #[error("cron parse error: {0}")]
    Cron(String),
    /// [`SchedulerBuilder::build`](crate::SchedulerBuilder::build) rejected a setting.
    #[error("configuration error: {0}")]
    Config(String),
    /// A string was not a valid [`TaskName`], [`TaskKey`], [`QueueName`](crate::QueueName)
    /// or [`WorkerId`](crate::WorkerId).
    #[error("invalid identifier: {0}")]
    Identifier(#[from] IdentifierError),
    /// A handler registration was rejected.
    #[error(transparent)]
    Register(#[from] RegisterError),
    /// Task arguments or an outbox payload failed to serialize or deserialize.
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
        /// The schedule whose row is corrupt.
        job_id: JobId,
        /// Which stored value is invalid.
        #[source]
        source: CorruptJobRow,
    },
}

/// Why a stored `scheduler_jobs` row failed to project into a domain `Job`. Each
/// arm is a value the scheduler guarantees on write, so reaching one means the row
/// was corrupted.
#[cfg(feature = "cron")]
#[derive(Debug, thiserror::Error)]
pub enum CorruptJobRow {
    /// The stored cron expression does not parse; holds the parser's message.
    #[error("unparseable cron: {0}")]
    Cron(String),
    /// The stored `max_attempts` is not positive.
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
    /// The task name is not a valid identifier.
    #[error(transparent)]
    Name(#[from] IdentifierError),
    /// A handler is already registered for the name.
    #[error(transparent)]
    Duplicate(#[from] DuplicateTaskName),
}

/// The error a task handler returns. It says what happens to the task next:
/// [`retry`](Self::retry), [`permanent`](Self::permanent) or
/// [`interrupted`](Self::interrupted). Any `std::error::Error` converts into a
/// permanent failure, so `?` marks the task `dead`.
///
/// Opaque handler error. Deliberately does NOT implement `std::error::Error`
/// (same trick as `anyhow::Error`) so the blanket `From<E: Error>` below is
/// coherent — if `TaskError` were itself an `Error`, that blanket would collide
/// with the std reflexive `From<TaskError> for TaskError`. Its `Display` is what
/// gets stored in `queue_tasks.last_error`.
pub struct TaskError {
    source: Box<dyn std::error::Error + Send + Sync>,
    kind: Kind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Retry,
    Permanent,
    Interrupted,
}

impl TaskError {
    /// Marks the task `dead` now, whatever attempts remain. `message` is stored in
    /// `last_error`.
    pub fn permanent(message: impl Into<String>) -> Self {
        Self {
            source: message.into().into(),
            kind: Kind::Permanent,
        }
    }

    /// Requeues the task with exponential backoff and jitter, or marks it `dead` once
    /// `max_attempts` is used up. `message` is stored in `last_error`.
    pub fn retry(message: impl Into<String>) -> Self {
        Self {
            source: message.into().into(),
            kind: Kind::Retry,
        }
    }

    /// Return from a handler that stopped because `ctx.cancellation` fired during shutdown: the
    /// task is released without counting the attempt. Outside shutdown it is retried like `retry`.
    pub fn interrupted() -> Self {
        Self {
            source: "interrupted by worker shutdown".into(),
            kind: Kind::Interrupted,
        }
    }

    /// Whether the task will be tried again: true for [`retry`](Self::retry) and
    /// [`interrupted`](Self::interrupted).
    pub fn is_retryable(&self) -> bool {
        matches!(self.kind, Kind::Retry | Kind::Interrupted)
    }

    /// Whether this was made by [`interrupted`](Self::interrupted).
    pub fn is_interrupted(&self) -> bool {
        self.kind == Kind::Interrupted
    }
}

impl std::fmt::Debug for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskError")
            .field("source", &self.source)
            .field("kind", &self.kind)
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
            kind: Kind::Permanent,
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

    #[test]
    fn task_error_interrupted() {
        let e = TaskError::interrupted();
        assert!(e.is_interrupted() && e.is_retryable());
        assert!(!TaskError::retry("e").is_interrupted());
        assert!(!TaskError::permanent("e").is_interrupted());
        assert_eq!(e.to_string(), "interrupted by worker shutdown");
    }

    #[cfg(feature = "cron")]
    #[test]
    fn corrupt_job_row_from_max_attempts() {
        let e: CorruptJobRow = MaxAttemptsError::NonPositive.into();
        assert!(e.to_string().contains("max_attempts"));
    }
}
