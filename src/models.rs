use std::num::{NonZeroI32, NonZeroI64, NonZeroU32};
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
// JobLifecycle — named two-state lifecycle (replaces the stored `is_paused` bool)
// ---------------------------------------------------------------------------

/// A job's lifecycle state. Used at both the creation and read boundaries; the
/// stored `is_paused` boolean never escapes the row mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobLifecycle {
    Active,
    Paused,
}

impl JobLifecycle {
    /// Boundary helper for the `is_paused` column. `pub(crate)` because `jobs`
    /// and `admin` (sibling modules) call it — a plain `fn` is private to `models`.
    pub(crate) fn is_paused(self) -> bool {
        matches!(self, JobLifecycle::Paused)
    }

    pub(crate) fn from_paused(paused: bool) -> Self {
        if paused {
            JobLifecycle::Paused
        } else {
            JobLifecycle::Active
        }
    }
}

// ---------------------------------------------------------------------------
// Validated config types (input boundary)
// ---------------------------------------------------------------------------

/// Max claim attempts. Private `NonZeroI32` with the invariant `>= 1`, enforced by
/// every constructor — so the stored-`i32` conversion is total.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaxAttempts(NonZeroI32);

/// Why a value was rejected as a [`MaxAttempts`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MaxAttemptsError {
    #[error("max_attempts must be greater than zero")]
    NonPositive,
    #[error("max_attempts exceeds i32::MAX")]
    TooLarge,
}

impl TryFrom<u32> for MaxAttempts {
    type Error = MaxAttemptsError;
    fn try_from(v: u32) -> Result<Self, MaxAttemptsError> {
        let i = i32::try_from(v).map_err(|_| MaxAttemptsError::TooLarge)?; // v > i32::MAX
        let nz = NonZeroI32::new(i).ok_or(MaxAttemptsError::NonPositive)?; // v == 0
        Ok(MaxAttempts(nz)) // i > 0: v is unsigned and non-zero
    }
}

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
    #[error("lease interval has calendar components (months={months}, days={days}); only microseconds are valid")]
    CalendarComponent { months: i32, days: i32 },
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
    /// Read boundary: parse the stored signed count. Rejects `<= 0`.
    pub(crate) fn from_db_i32(i: i32) -> Result<Self, MaxAttemptsError> {
        NonZeroI32::new(i)
            .filter(|n| n.get() > 0)
            .map(MaxAttempts)
            .ok_or(MaxAttemptsError::NonPositive)
    }

    /// Total: the inner invariant guarantees `1..=i32::MAX`. No cast, no unwrap.
    pub(crate) fn to_i32(self) -> i32 {
        self.0.get()
    }
}

impl LeaseDuration {
    /// Total, infallible: `micros` is already a validated positive i64.
    pub(crate) fn to_pg_interval(self) -> diesel::pg::data_types::PgInterval {
        diesel::pg::data_types::PgInterval::from_microseconds(self.micros.get())
    }

    /// Read boundary: parse a stored interval back into the domain value. Rejects
    /// any month/day component (the scheduler only ever writes pure microseconds via
    /// `from_microseconds`) and non-positive microseconds.
    pub(crate) fn from_pg_interval(
        iv: diesel::pg::data_types::PgInterval,
    ) -> Result<Self, LeaseDurationError> {
        if iv.months != 0 || iv.days != 0 {
            return Err(LeaseDurationError::CalendarComponent {
                months: iv.months,
                days: iv.days,
            });
        }
        let micros = NonZeroI64::new(iv.microseconds).ok_or(LeaseDurationError::Zero)?;
        if micros.get() < 0 {
            return Err(LeaseDurationError::Zero);
        }
        Ok(LeaseDuration { micros })
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
mod max_attempts_tests {
    use super::{MaxAttempts, MaxAttemptsError};

    #[test]
    fn try_from_zero_is_non_positive() {
        assert_eq!(MaxAttempts::try_from(0u32), Err(MaxAttemptsError::NonPositive));
    }

    #[test]
    fn try_from_above_i32_max_is_too_large() {
        let v = u32::try_from(i32::MAX).unwrap() + 1;
        assert_eq!(MaxAttempts::try_from(v), Err(MaxAttemptsError::TooLarge));
    }

    #[test]
    fn try_from_i32_max_is_accepted() {
        let m = MaxAttempts::try_from(u32::try_from(i32::MAX).unwrap()).expect("i32::MAX valid");
        assert_eq!(m.to_i32(), i32::MAX);
    }

    #[test]
    fn try_from_typical_round_trips() {
        assert_eq!(MaxAttempts::try_from(3u32).unwrap().to_i32(), 3);
    }

    #[test]
    fn from_db_rejects_non_positive() {
        assert_eq!(MaxAttempts::from_db_i32(0), Err(MaxAttemptsError::NonPositive));
        assert_eq!(MaxAttempts::from_db_i32(-1), Err(MaxAttemptsError::NonPositive));
    }

    #[test]
    fn from_db_accepts_positive() {
        assert_eq!(MaxAttempts::from_db_i32(3).unwrap().to_i32(), 3);
    }
}

#[cfg(test)]
mod job_lifecycle_tests {
    use super::JobLifecycle;

    #[test]
    fn from_paused_round_trips() {
        assert_eq!(JobLifecycle::from_paused(true), JobLifecycle::Paused);
        assert_eq!(JobLifecycle::from_paused(false), JobLifecycle::Active);
        assert!(JobLifecycle::Paused.is_paused());
        assert!(!JobLifecycle::Active.is_paused());
    }
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

    #[test]
    fn from_pg_interval_round_trips() {
        let ld = LeaseDuration::try_from(Duration::from_secs(300)).unwrap();
        let back = LeaseDuration::from_pg_interval(ld.to_pg_interval()).unwrap();
        assert_eq!(back, ld);
    }

    #[test]
    fn from_pg_interval_rejects_days() {
        let iv = diesel::pg::data_types::PgInterval { microseconds: 0, days: 1, months: 0 };
        assert_eq!(
            LeaseDuration::from_pg_interval(iv),
            Err(LeaseDurationError::CalendarComponent { months: 0, days: 1 })
        );
    }

    #[test]
    fn from_pg_interval_rejects_months() {
        let iv = diesel::pg::data_types::PgInterval { microseconds: 0, days: 0, months: 1 };
        assert_eq!(
            LeaseDuration::from_pg_interval(iv),
            Err(LeaseDurationError::CalendarComponent { months: 1, days: 0 })
        );
    }

    #[test]
    fn from_pg_interval_rejects_zero_micros() {
        let iv = diesel::pg::data_types::PgInterval { microseconds: 0, days: 0, months: 0 };
        assert_eq!(LeaseDuration::from_pg_interval(iv), Err(LeaseDurationError::Zero));
    }
}
