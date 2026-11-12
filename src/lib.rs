#![doc = include_str!("../README.md")]

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

#[cfg(feature = "cron")]
pub use crate::cron::CronExpression;
#[cfg(feature = "cron")]
pub use crate::error::CorruptJobRow;
pub use crate::error::DuplicateTaskName;
pub use crate::error::RegisterError;
pub use crate::error::SchedulerError;
pub use crate::error::TaskError;
pub use crate::ids::IdentifierError;
#[cfg(feature = "cron")]
pub use crate::ids::JobId;
pub use crate::ids::LockToken;
pub use crate::ids::QueueName;
pub use crate::ids::TaskId;
pub use crate::ids::TaskKey;
pub use crate::ids::TaskName;
pub use crate::ids::WorkerId;
#[cfg(feature = "cron")]
pub use crate::jobs::Applied;
#[cfg(feature = "cron")]
pub use crate::jobs::CreateJob;
#[cfg(feature = "cron")]
pub use crate::jobs::ScheduleUpdate;
pub use crate::models::Attempt;
pub use crate::models::AttemptOutcome;
pub use crate::models::ClaimedTask;
pub use crate::models::CleanupSummary;
#[cfg(feature = "cron")]
pub use crate::models::Job;
#[cfg(feature = "cron")]
pub use crate::models::JobLifecycle;
pub use crate::models::MaxAttempts;
pub use crate::models::MaxAttemptsError;
pub use crate::models::OutboxCommand;
pub use crate::models::ParseEnumError;
pub use crate::models::QueueDepth;
pub use crate::models::QueueStats;
pub use crate::models::ReapSummary;
pub use crate::models::Retention;
pub use crate::models::RetryOutcome;
pub use crate::models::TaskLease;
pub use crate::models::TaskStatus;
pub use crate::models::TaskView;
pub use crate::pool::PoolError;
pub use crate::pool::SchedulerPool;
pub use crate::queue::EnqueueOptions;
pub use crate::queue::Enqueued;
pub use crate::queue::Priority;
pub use crate::queue::Task;
pub use crate::queue::enqueue;
pub use crate::queue::replay;
pub use crate::runtime::HealthStatus;
pub use crate::runtime::Scheduler;
pub use crate::runtime::SchedulerBuilder;
pub use crate::runtime::TaskContext;
pub use crate::runtime::WorkerHealth;
