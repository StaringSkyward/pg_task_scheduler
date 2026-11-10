//! # pg_task_scheduler
//!
//! A PostgreSQL-backed task queue with idempotent enqueue and leased, fenced,
//! at-least-once execution (see `docs/postgresql_queue_design.md`). Install the
//! schema in `migrations/0001_create_queue_tables/up.sql` before use; tasks are
//! enqueued with [`queue::enqueue`] on the caller's connection, so they commit or
//! roll back with the caller's own writes. Handlers must be idempotent.

mod error;
mod ids;
mod models;
mod pool;
pub mod queue;
pub mod runtime;
pub mod store;

pub use crate::error::{DuplicateTaskName, RegisterError, SchedulerError, TaskError};
pub use crate::ids::{IdentifierError, LockToken, TaskId, TaskKey, TaskName, WorkerId};
pub use crate::models::{
    Attempt, AttemptOutcome, ClaimedTask, MaxAttempts, MaxAttemptsError, OutboxCommand,
    ParseEnumError, RetryOutcome, TaskLease, TaskStatus, TaskView,
};
pub use crate::pool::{PoolError, SchedulerPool};
pub use crate::queue::{EnqueueOptions, Enqueued, Priority, Task, enqueue};
pub use crate::runtime::{HealthStatus, TaskContext, WorkerHealth};
