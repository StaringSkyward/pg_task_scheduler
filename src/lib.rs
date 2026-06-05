//! # pg_task_scheduler
//!
//! Cron-like, PostgreSQL-backed scheduling with **leased at-least-once** execution.
//!
//! Each scheduled occurrence is a durable row in PostgreSQL.  A worker claims it
//! using `FOR UPDATE SKIP LOCKED` and a fencing token.  If the worker crashes or
//! exceeds its lease, another worker reclaims the row after the lease expires.
//! Because a run may therefore execute more than once for the same occurrence,
//! **handlers must be idempotent** — use `ctx.run_id` or `ctx.scheduled_for` as
//! idempotency keys.
//!
//! Apply `migrations/0001_create_scheduler_tables/up.sql` (e.g. via your Diesel
//! migration set) before starting the scheduler.
//!
//! ## Quick start
//!
//! ```no_run
//! # async fn ex(pool: diesel_async::pooled_connection::deadpool::Pool<diesel_async::AsyncPgConnection>) -> Result<(), Box<dyn std::error::Error>> {
//! use std::time::Duration;
//! use pg_task_scheduler::{Scheduler, WorkerId, JobContext, JobError};
//! use tokio_util::sync::CancellationToken;
//!
//! #[derive(serde::Deserialize)]
//! struct DigestArgs { user_id: uuid::Uuid }
//!
//! async fn send_digest(ctx: JobContext, _args: DigestArgs) -> Result<(), JobError> {
//!     // ctx.run_id and ctx.scheduled_for are stable across retries — use them as
//!     // idempotency keys so re-execution is safe.
//!     let _ = ctx.run_id;
//!     Ok(())
//! }
//!
//! let scheduler = Scheduler::builder(pool, WorkerId::try_from("api-1")?)
//!     .poll_interval(Duration::from_secs(1))
//!     .register::<DigestArgs, _, _>("send_digest_email", send_digest)?
//!     .build()?;
//! scheduler.run_until_shutdown(CancellationToken::new()).await?;
//! # Ok(()) }
//! ```

mod cron;
mod error;
mod ids;
mod metrics;
mod models;
mod pool;
mod schema;
pub mod store;

pub mod jobs;
pub mod runtime;

#[cfg(feature = "axum")]
pub mod admin;

pub use crate::cron::CronExpression;
pub use crate::error::{DuplicateJobName, JobError, RegisterError, SchedulerError};
pub use crate::ids::{IdentifierError, JobId, JobName, LeaseToken, RunId, WorkerId};
pub use crate::jobs::{CreateJob, ScheduleUpdate};
pub use crate::models::RunOutcome;
pub use crate::models::{
    ClaimedRun, JobLifecycle, Lease, LeaseDuration, LeaseDurationError, MaxAttempts,
    MaxAttemptsError, Outcome, RunState, SchedulerJob,
};
pub use crate::pool::{PoolError, SchedulerPool};
pub use crate::runtime::JobContext;
pub use crate::runtime::{Scheduler, SchedulerBuilder};
