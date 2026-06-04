use std::num::{NonZeroI64, NonZeroU32};
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

/// Max single-attempt runtime / lease length. A positive, microsecond-exact
/// duration: the persisted PG interval always equals what the caller asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseDuration {
    micros: NonZeroI64, // invariant: always in 1..=i64::MAX, enforced by TryFrom
}

/// Why a `Duration` was rejected as a `LeaseDuration`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LeaseDurationError {
    #[error("lease duration must be greater than zero")]
    Zero,
    #[error("lease duration must be a whole number of microseconds")]
    PrecisionLoss,
    #[error("lease duration exceeds the maximum of i64::MAX microseconds")]
    TooLarge,
}

impl TryFrom<Duration> for LeaseDuration {
    type Error = LeaseDurationError;

    fn try_from(d: Duration) -> Result<Self, LeaseDurationError> {
        // Reject any sub-microsecond remainder first (no silent floor). Whole
        // seconds are always whole microseconds, so only the subsec component
        // can carry a fractional-microsecond part.
        if !d.subsec_nanos().is_multiple_of(1000) {
            return Err(LeaseDurationError::PrecisionLoss);
        }
        let micros = i64::try_from(d.as_micros()).map_err(|_| LeaseDurationError::TooLarge)?;
        let micros = NonZeroI64::new(micros).ok_or(LeaseDurationError::Zero)?;
        Ok(LeaseDuration { micros })
    }
}

impl MaxAttempts {
    /// Convert to the `i32` the column stores; errors if it exceeds `i32::MAX`.
    pub fn to_i32(self) -> Result<i32, crate::error::SchedulerError> {
        i32::try_from(self.0.get()).map_err(|_| {
            crate::error::SchedulerError::Config("max_attempts exceeds i32::MAX".into())
        })
    }
}

impl LeaseDuration {
    /// Total, infallible: `micros` is already a validated positive i64.
    pub fn to_pg_interval(self) -> diesel::pg::data_types::PgInterval {
        diesel::pg::data_types::PgInterval::from_microseconds(self.micros.get())
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

#[cfg(test)]
mod lease_duration_tests {
    use super::{LeaseDuration, LeaseDurationError};
    use std::time::Duration;

    #[test]
    fn zero_is_rejected() {
        assert_eq!(
            LeaseDuration::try_from(Duration::ZERO),
            Err(LeaseDurationError::Zero)
        );
    }

    #[test]
    fn sub_microsecond_is_precision_loss() {
        assert_eq!(
            LeaseDuration::try_from(Duration::from_nanos(500)),
            Err(LeaseDurationError::PrecisionLoss)
        );
    }

    #[test]
    fn fractional_microsecond_is_precision_loss() {
        assert_eq!(
            LeaseDuration::try_from(Duration::from_nanos(1500)),
            Err(LeaseDurationError::PrecisionLoss)
        );
    }

    #[test]
    fn one_microsecond_is_accepted() {
        let ld = LeaseDuration::try_from(Duration::from_micros(1)).expect("1us is valid");
        let iv = ld.to_pg_interval();
        assert_eq!(iv.microseconds, 1);
        assert_eq!(iv.days, 0);
        assert_eq!(iv.months, 0);
    }

    #[test]
    fn whole_seconds_convert_to_microseconds() {
        let ld = LeaseDuration::try_from(Duration::from_secs(60)).expect("60s is valid");
        assert_eq!(ld.to_pg_interval().microseconds, 60_000_000);
    }

    #[test]
    fn max_i64_microseconds_is_accepted() {
        let micros = u64::try_from(i64::MAX).unwrap();
        let ld =
            LeaseDuration::try_from(Duration::from_micros(micros)).expect("i64::MAX us is valid");
        assert_eq!(ld.to_pg_interval().microseconds, i64::MAX);
    }

    #[test]
    fn one_past_max_i64_microseconds_is_too_large() {
        let micros = u64::try_from(i64::MAX).unwrap() + 1;
        assert_eq!(
            LeaseDuration::try_from(Duration::from_micros(micros)),
            Err(LeaseDurationError::TooLarge)
        );
    }
}
