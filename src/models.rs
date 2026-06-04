use std::num::NonZeroU32;
use std::time::Duration;

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel::sql_types;

use crate::ids::{JobId, JobName, LeaseToken, RunId, WorkerId};
use crate::schema::scheduler_jobs;

// ---------------------------------------------------------------------------
// RunOutcome — PG enum via diesel-derive-enum
// ---------------------------------------------------------------------------

#[derive(diesel_derive_enum::DbEnum, Debug, Clone, Copy, PartialEq, Eq)]
#[ExistingTypePath = "crate::schema::sql_types::RunOutcome"]
pub enum RunOutcome {
    Completed,
    Failed,
}

// ---------------------------------------------------------------------------
// Validated config types (input boundary)
// ---------------------------------------------------------------------------

/// Max claim attempts; zero is meaningless.
#[derive(Debug, Clone, Copy)]
pub struct MaxAttempts(pub NonZeroU32);

/// Max single-attempt runtime / lease length.
#[derive(Debug, Clone, Copy)]
pub struct LeaseDuration(pub Duration);

impl MaxAttempts {
    /// Convert to the `i32` the column stores; errors if it exceeds `i32::MAX`.
    pub fn to_i32(self) -> Result<i32, crate::error::SchedulerError> {
        i32::try_from(self.0.get()).map_err(|_| {
            crate::error::SchedulerError::Config("max_attempts exceeds i32::MAX".into())
        })
    }
}

impl LeaseDuration {
    /// Checked conversion to a Postgres interval (microsecond precision).
    pub fn to_pg_interval(
        self,
    ) -> Result<diesel::pg::data_types::PgInterval, crate::error::SchedulerError> {
        let micros = i64::try_from(self.0.as_micros())
            .map_err(|_| crate::error::SchedulerError::Config("lease_duration too large".into()))?;
        Ok(diesel::pg::data_types::PgInterval::from_microseconds(
            micros,
        ))
    }
}

// ---------------------------------------------------------------------------
// Derived run status (read model)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Lease {
    pub worker_id: WorkerId,
    pub lease_token: LeaseToken,
    pub lease_expires_at: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
}

/// Status is NEVER stored; it is derived from lease/outcome presence.
#[derive(Debug, Clone)]
pub enum RunState {
    Pending,
    Running(Lease),
    Completed {
        finished_at: DateTime<Utc>,
    },
    Failed {
        finished_at: DateTime<Utc>,
        error: String,
    },
}

// ---------------------------------------------------------------------------
// ClaimedRun — always carries a complete lease
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ClaimedRun {
    pub run_id: RunId,
    pub job_id: JobId,
    pub job_name: JobName,
    pub job_args: serde_json::Value,
    pub scheduled_for: DateTime<Utc>,
    pub attempt: NonZeroU32,
    pub lease_token: LeaseToken,
    pub lease_expires_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Finalization outcome
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum Outcome {
    Completed,
    Failed(String),
}

// ---------------------------------------------------------------------------
// DB row structs
// ---------------------------------------------------------------------------

/// Faithful `scheduler_jobs` row.
#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = scheduler_jobs)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct SchedulerJob {
    pub id: JobId,
    pub name: JobName,
    pub cron_expression: String,
    pub job_args: serde_json::Value,
    pub next_run_at: DateTime<Utc>,
    pub lease_duration: diesel::pg::data_types::PgInterval,
    pub max_attempts: i32,
    pub is_paused: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = scheduler_jobs)]
pub struct NewJob {
    pub name: JobName,
    pub cron_expression: String,
    pub job_args: serde_json::Value,
    pub next_run_at: DateTime<Utc>,
    pub lease_duration: diesel::pg::data_types::PgInterval,
    pub max_attempts: i32,
    pub is_paused: bool,
}

// ---------------------------------------------------------------------------
// Status view query struct
// ---------------------------------------------------------------------------

/// `QueryableByName` row of `scheduler_runs_status`, mapped into `RunState` by `store::inspect`.
/// The id/job_id/scheduled_for/attempt_count fields are part of the view's column set but are
/// not read by `state_of`; the allow silences that (they must exist to map the SELECT).
#[allow(dead_code)]
#[derive(Debug, Clone, QueryableByName)]
pub struct StatusRow {
    #[diesel(sql_type = sql_types::Uuid)]
    pub id: RunId,
    #[diesel(sql_type = sql_types::Uuid)]
    pub job_id: JobId,
    #[diesel(sql_type = sql_types::Timestamptz)]
    pub scheduled_for: DateTime<Utc>,
    #[diesel(sql_type = sql_types::Integer)]
    pub attempt_count: i32,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Text>)]
    pub worker_id: Option<WorkerId>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Uuid>)]
    pub lease_token: Option<LeaseToken>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Timestamptz>)]
    pub lease_expires_at: Option<DateTime<Utc>>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Timestamptz>)]
    pub started_at: Option<DateTime<Utc>>,
    #[diesel(sql_type = sql_types::Nullable<crate::schema::sql_types::RunOutcome>)]
    pub outcome: Option<RunOutcome>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Timestamptz>)]
    pub finished_at: Option<DateTime<Utc>>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Text>)]
    pub last_error: Option<String>,
}
