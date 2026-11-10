//! # pg_task_scheduler
//!
//! A PostgreSQL-backed task queue with idempotent enqueue and leased, fenced,
//! at-least-once execution (see `docs/postgresql_queue_design.md`). Install the
//! schema in `migrations/0001_create_queue_tables/up.sql` before use; tasks are
//! enqueued with [`queue::enqueue`] on the caller's connection, so they commit or
//! roll back with the caller's own writes. Handlers must be idempotent.

#[cfg(feature = "axum")]
pub mod admin;
#[cfg(feature = "cron")]
mod cron;
mod error;
mod ids;
#[cfg(feature = "cron")]
pub mod jobs;
mod models;
mod pool;
pub mod queue;
pub mod runtime;
#[cfg(feature = "cron")]
mod schema;
pub mod store;

pub use crate::error::{DuplicateTaskName, RegisterError, SchedulerError, TaskError};
pub use crate::ids::{IdentifierError, LockToken, TaskId, TaskKey, TaskName, WorkerId};
pub use crate::models::{
    Attempt, AttemptOutcome, ClaimedTask, CleanupSummary, MaxAttempts, MaxAttemptsError,
    OutboxCommand, ParseEnumError, QueueDepth, QueueStats, ReapSummary, Retention, RetryOutcome,
    TaskLease, TaskStatus, TaskView,
};
pub use crate::pool::{PoolError, SchedulerPool};
pub use crate::queue::{EnqueueOptions, Enqueued, Priority, Task, enqueue, replay};
pub use crate::runtime::{HealthStatus, Scheduler, SchedulerBuilder, TaskContext, WorkerHealth};

#[cfg(feature = "cron")]
pub use crate::cron::CronExpression;
#[cfg(feature = "cron")]
pub use crate::error::CorruptJobRow;
#[cfg(feature = "cron")]
pub use crate::ids::JobId;
#[cfg(feature = "cron")]
pub use crate::jobs::{Applied, CreateJob, ScheduleUpdate};
#[cfg(feature = "cron")]
pub use crate::models::{Job, JobLifecycle};
