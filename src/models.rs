use std::num::{NonZeroI32, NonZeroUsize};
use std::time::Duration;

use diesel::pg::data_types::PgInterval;

use chrono::{DateTime, Utc};

use crate::ids::{LockToken, TaskId, TaskKey, TaskName, WorkerId};
#[cfg(feature = "cron")]
use {
    crate::cron::CronExpression, crate::error::SchedulerError, crate::ids::JobId,
    crate::schema::scheduler_jobs, diesel::prelude::*,
};

/// Lifecycle state of a `queue_tasks` row (stored as TEXT).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Queued,
    Running,
    Done,
    Dead,
}

/// Outcome recorded on a closed `queue_task_attempts` row (stored as TEXT).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptOutcome {
    Done,
    Retry,
    Dead,
    LeaseExpired,
}

/// A stored status/outcome string that matches no known variant.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown {kind}: {value:?}")]
pub struct ParseEnumError {
    kind: &'static str,
    value: String,
}

macro_rules! text_enum {
    ($ty:ident, $kind:literal, { $($variant:ident => $s:literal),+ $(,)? }) => {
        impl $ty {
            pub fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $s),+ }
            }
        }

        impl std::str::FromStr for $ty {
            type Err = ParseEnumError;
            fn from_str(s: &str) -> Result<Self, ParseEnumError> {
                match s {
                    $($s => Ok(Self::$variant),)+
                    other => Err(ParseEnumError { kind: $kind, value: other.to_owned() }),
                }
            }
        }
    };
}

text_enum!(TaskStatus, "task status", {
    Queued => "queued", Running => "running", Done => "done", Dead => "dead",
});
text_enum!(AttemptOutcome, "attempt outcome", {
    Done => "done", Retry => "retry", Dead => "dead", LeaseExpired => "lease_expired",
});

/// A task handed to a worker by `store::claim`.
#[derive(Debug, Clone)]
pub struct ClaimedTask {
    pub lease: TaskLease,
    pub task_key: TaskKey,
    pub task_name: TaskName,
    pub payload: serde_json::Value,
    pub max_attempts: i32,
    pub due_at: DateTime<Utc>,
    pub claimed_at: DateTime<Utc>,
    pub locked_until: DateTime<Utc>,
}

/// What `store::retry` did with the task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryOutcome {
    /// Requeued; due again at `due_at` (PostgreSQL time).
    Queued { due_at: DateTime<Utc> },
    /// Attempts exhausted; the task is now dead.
    Dead,
}

/// A command written to `queue_outbox` atomically with task completion.
#[derive(Debug, Clone)]
pub struct OutboxCommand {
    pub effect_name: String,
    pub command_key: String,
    pub topic: String,
    pub payload: serde_json::Value,
}

/// Every `queue_tasks` column as a domain type.
#[derive(Debug, Clone)]
pub struct TaskView {
    pub id: TaskId,
    pub task_key: TaskKey,
    pub task_name: TaskName,
    pub payload: serde_json::Value,
    pub status: TaskStatus,
    pub priority: i32,
    pub due_at: DateTime<Utc>,
    pub attempts: i32,
    pub max_attempts: i32,
    pub locked_by: Option<WorkerId>,
    pub lock_token: LockToken,
    pub locked_until: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

/// One `queue_task_attempts` row.
#[derive(Debug, Clone)]
pub struct Attempt {
    pub attempt_no: i32,
    pub lock_token: LockToken,
    pub worker_id: WorkerId,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub outcome: Option<AttemptOutcome>,
    pub error: Option<String>,
}

/// Proof of ownership of one claimed attempt; every worker-owned mutation is
/// guarded by it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskLease {
    pub task_id: TaskId,
    pub worker_id: WorkerId,
    pub lock_token: LockToken,
    pub attempt_no: i32,
}

/// What one `store::reap_expired` pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReapSummary {
    pub requeued: usize,
    pub dead: usize,
}

/// Rows deleted by one `store::cleanup` pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CleanupSummary {
    pub tasks: usize,
    pub outbox: usize,
}

const DEFAULT_CLEANUP_BATCH: NonZeroUsize = NonZeroUsize::new(1000).unwrap();

/// How long terminal rows are kept; `batch_size` bounds each delete statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    pub done: Duration,
    pub dead: Duration,
    pub idempotency_horizon: Duration,
    pub outbox_published: Duration,
    pub batch_size: NonZeroUsize,
}

impl Default for Retention {
    fn default() -> Self {
        const DAY: u64 = 86_400;
        Self {
            done: Duration::from_secs(30 * DAY),
            dead: Duration::from_secs(30 * DAY),
            idempotency_horizon: Duration::from_secs(7 * DAY),
            outbox_published: Duration::from_secs(7 * DAY),
            batch_size: DEFAULT_CLEANUP_BATCH,
        }
    }
}

/// Queued tasks of one name and priority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueDepth {
    pub task_name: TaskName,
    pub priority: i32,
    pub queued: i64,
}

/// Point-in-time queue health from `store::queue_stats`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueStats {
    pub queued: i64,
    pub running: i64,
    pub done: i64,
    pub dead: i64,
    /// Queued tasks whose `due_at` has passed.
    pub due: i64,
    pub oldest_due_age: Option<Duration>,
    pub depth: Vec<QueueDepth>,
    pub outbox_unpublished: i64,
    pub oldest_unpublished_age: Option<Duration>,
}

/// `Duration` to a pure-microsecond PG interval, saturating at `i64::MAX`.
pub(crate) fn pg_interval(d: Duration) -> PgInterval {
    PgInterval::from_microseconds(i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
}

/// PG interval to `Duration` (days count as 86 400 s); negative becomes zero.
pub(crate) fn interval_to_duration(iv: PgInterval) -> Duration {
    let micros = u64::try_from(iv.microseconds).unwrap_or(0);
    let days = u64::try_from(iv.days).unwrap_or(0);
    Duration::from_micros(micros) + Duration::from_secs(days * 86_400)
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

impl MaxAttempts {
    pub(crate) const fn default_value() -> Self {
        Self(NonZeroI32::new(3).unwrap())
    }
    /// Read boundary: parse the stored signed count. Rejects `<= 0`.
    #[cfg_attr(not(feature = "cron"), allow(dead_code))]
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

    /// The configured attempt count. Total: the inner is a validated `1..=i32::MAX`,
    /// so `i32::unsigned_abs` is an exact i32->u32 — no `as` cast, no `unwrap`.
    pub fn get(self) -> u32 {
        self.0.get().unsigned_abs()
    }
}

#[cfg(test)]
mod task_enum_tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn status_round_trips() {
        for s in [
            TaskStatus::Queued,
            TaskStatus::Running,
            TaskStatus::Done,
            TaskStatus::Dead,
        ] {
            assert_eq!(TaskStatus::from_str(s.as_str()), Ok(s));
        }
        assert!(TaskStatus::from_str("pending").is_err());
    }

    #[test]
    fn outcome_round_trips() {
        for o in [
            AttemptOutcome::Done,
            AttemptOutcome::Retry,
            AttemptOutcome::Dead,
            AttemptOutcome::LeaseExpired,
        ] {
            assert_eq!(AttemptOutcome::from_str(o.as_str()), Ok(o));
        }
        assert_eq!(AttemptOutcome::LeaseExpired.as_str(), "lease_expired");
    }

    #[test]
    fn interval_conversions() {
        assert_eq!(
            interval_to_duration(pg_interval(Duration::from_millis(1500))),
            Duration::from_millis(1500)
        );
        assert_eq!(pg_interval(Duration::MAX).microseconds, i64::MAX);
        assert_eq!(
            interval_to_duration(PgInterval::from_microseconds(-5)),
            Duration::ZERO
        );
        let day = PgInterval::new(0, 1, 0);
        assert_eq!(interval_to_duration(day), Duration::from_secs(86_400));
    }
}

#[cfg(test)]
mod max_attempts_tests {
    use super::{MaxAttempts, MaxAttemptsError};

    #[test]
    fn try_from_zero_is_non_positive() {
        assert_eq!(
            MaxAttempts::try_from(0u32),
            Err(MaxAttemptsError::NonPositive)
        );
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
        assert_eq!(
            MaxAttempts::from_db_i32(0),
            Err(MaxAttemptsError::NonPositive)
        );
        assert_eq!(
            MaxAttempts::from_db_i32(-1),
            Err(MaxAttemptsError::NonPositive)
        );
    }

    #[test]
    fn from_db_accepts_positive() {
        assert_eq!(MaxAttempts::from_db_i32(3).unwrap().to_i32(), 3);
    }

    #[test]
    fn get_returns_u32() {
        assert_eq!(MaxAttempts::try_from(3u32).unwrap().get(), 3);
    }
}

#[cfg(feature = "cron")]
/// A job's lifecycle state. Used at both the creation and read boundaries; the
/// stored `is_paused` boolean never escapes the row mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobLifecycle {
    Active,
    Paused,
}

#[cfg(feature = "cron")]
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

#[cfg(feature = "cron")]
/// Public domain projection of a `scheduler_jobs` row. Every field is a domain
/// type; `job_args` is intentionally omitted (reading is untyped — the handler
/// argument type is not recoverable from a runtime `TaskName`). Not `Serialize`:
/// it must not become an accidental wire contract.
#[derive(Debug, Clone)]
pub struct Job {
    pub id: JobId,
    pub name: TaskName,
    pub cron: CronExpression,
    pub max_attempts: MaxAttempts,
    pub lifecycle: JobLifecycle,
    pub next_run_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[cfg(feature = "cron")]
impl TryFrom<SchedulerJob> for Job {
    type Error = SchedulerError;
    fn try_from(row: SchedulerJob) -> Result<Self, SchedulerError> {
        let job_id = row.id;
        let cron = CronExpression::parse_stored(job_id, &row.cron_expression)?;
        let max_attempts =
            MaxAttempts::from_db_i32(row.max_attempts).map_err(|e| SchedulerError::CorruptJob {
                job_id,
                source: e.into(),
            })?;
        Ok(Job {
            id: job_id,
            name: row.name,
            cron,
            max_attempts,
            lifecycle: JobLifecycle::from_paused(row.is_paused),
            next_run_at: row.next_run_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

#[cfg(feature = "cron")]
/// Faithful `scheduler_jobs` row.
#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = scheduler_jobs)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct SchedulerJob {
    pub id: JobId,
    pub name: TaskName,
    pub cron_expression: String,
    pub job_args: serde_json::Value,
    pub next_run_at: DateTime<Utc>,
    pub max_attempts: i32,
    pub is_paused: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[cfg(feature = "cron")]
#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = scheduler_jobs)]
pub struct NewJob {
    pub name: TaskName,
    pub cron_expression: String,
    pub job_args: serde_json::Value,
    pub next_run_at: DateTime<Utc>,
    pub max_attempts: i32,
    pub is_paused: bool,
}

#[cfg(all(test, feature = "cron"))]
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
