use std::num::NonZeroI32;
use std::num::NonZeroU32;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use diesel::pg::data_types::PgInterval;
#[cfg(feature = "cron")]
use diesel::prelude::*;

#[cfg(feature = "cron")]
use crate::cron::CronExpression;
use crate::error::SchedulerError;
use crate::ids::CommandKey;
use crate::ids::EffectName;
#[cfg(feature = "cron")]
use crate::ids::JobId;
use crate::ids::LockToken;
use crate::ids::QueueName;
#[cfg(feature = "cron")]
use crate::ids::ScheduleName;
use crate::ids::TaskId;
use crate::ids::TaskKey;
use crate::ids::TaskName;
use crate::ids::Topic;
use crate::ids::WorkerId;
#[cfg(feature = "cron")]
use crate::schema::scheduler_jobs;

/// Lifecycle state of a `queue_tasks` row (stored as TEXT).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    /// Waiting for its `due_at` and a worker that serves its queue and name.
    Queued,
    /// Claimed; a worker holds its lease.
    Running,
    /// The handler succeeded. Terminal.
    Done,
    /// Failed permanently or used up its attempts. Terminal; replayable.
    Dead,
    /// Cancelled by an operator. Terminal; replayable.
    Cancelled,
}

/// Outcome recorded on a closed `queue_task_attempts` row (stored as TEXT).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptOutcome {
    /// The handler succeeded.
    Done,
    /// The handler failed and the task was requeued.
    Retry,
    /// The handler failed and the task was marked `dead`.
    Dead,
    /// The lease expired before the worker finished; the reaper closed the attempt.
    LeaseExpired,
    /// Released at shutdown. The attempt is not counted.
    Released,
    /// An operator cancelled the task while it ran.
    Cancelled,
}

/// A stored status/outcome string that matches no known variant.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown {kind}: {value:?}")]
pub struct ParseEnumError {
    pub(crate) kind: &'static str,
    pub(crate) value: String,
}

macro_rules! text_enum {
    ($ty:ident, $kind:literal, { $($variant:ident => $s:literal),+ $(,)? }) => {
        impl $ty {
            /// The value stored in the database.
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
pub(crate) use text_enum;

text_enum!(TaskStatus, "task status", {
    Queued => "queued", Running => "running", Done => "done", Dead => "dead",
    Cancelled => "cancelled",
});
text_enum!(AttemptOutcome, "attempt outcome", {
    Done => "done", Retry => "retry", Dead => "dead", LeaseExpired => "lease_expired",
    Released => "released", Cancelled => "cancelled",
});

/// How a finished task ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalStatus {
    /// The handler succeeded.
    Done,
    /// Failed permanently or used up its attempts; replayable.
    Dead,
    /// Cancelled by an operator; replayable.
    Cancelled,
}

/// A task's lifecycle state with the columns that state carries: an owner and lease
/// while running, a completion time once finished. Mirrors the `queue_tasks` CHECKs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskState {
    /// Waiting for its `due_at` and a worker that serves its queue and name.
    Queued,
    /// Claimed; a worker holds its lease.
    Running {
        /// The worker holding the lease.
        locked_by: WorkerId,
        /// Lease expiry.
        locked_until: DateTime<Utc>,
    },
    /// Done, dead or cancelled.
    Finished {
        /// How it ended.
        status: TerminalStatus,
        /// When it finished. Retention counts from here.
        completed_at: DateTime<Utc>,
    },
}

impl TaskState {
    /// The stored status.
    pub fn status(&self) -> TaskStatus {
        match self {
            Self::Queued => TaskStatus::Queued,
            Self::Running { .. } => TaskStatus::Running,
            Self::Finished { status, .. } => match status {
                TerminalStatus::Done => TaskStatus::Done,
                TerminalStatus::Dead => TaskStatus::Dead,
                TerminalStatus::Cancelled => TaskStatus::Cancelled,
            },
        }
    }

    /// Read boundary: every shape the `queue_tasks` CHECKs forbid is an `Invariant`.
    pub(crate) fn from_columns(
        status: &str,
        locked_by: Option<WorkerId>,
        locked_until: Option<DateTime<Utc>>,
        completed_at: Option<DateTime<Utc>>,
    ) -> Result<Self, SchedulerError> {
        let status: TaskStatus = status
            .parse()
            .map_err(|_| SchedulerError::Invariant("unknown task status"))?;
        let finished = |status, completed_at| Self::Finished {
            status,
            completed_at,
        };
        Ok(match (status, locked_by, locked_until, completed_at) {
            (TaskStatus::Queued, None, None, None) => Self::Queued,
            (TaskStatus::Running, Some(locked_by), Some(locked_until), None) => Self::Running {
                locked_by,
                locked_until,
            },
            (TaskStatus::Done, None, None, Some(at)) => finished(TerminalStatus::Done, at),
            (TaskStatus::Dead, None, None, Some(at)) => finished(TerminalStatus::Dead, at),
            (TaskStatus::Cancelled, None, None, Some(at)) => {
                finished(TerminalStatus::Cancelled, at)
            }
            _ => {
                return Err(SchedulerError::Invariant(
                    "task lock or completion columns do not match its status",
                ));
            }
        })
    }
}

/// Whether an attempt is still running. Mirrors the `queue_task_attempts` CHECK that
/// `finished_at` and `outcome` are set together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptState {
    /// Still running.
    Open,
    /// Closed.
    Closed {
        /// When it closed.
        finished_at: DateTime<Utc>,
        /// How it closed.
        outcome: AttemptOutcome,
    },
}

impl AttemptState {
    /// Read boundary: an unknown outcome, or only one of the pair set, is an `Invariant`.
    pub(crate) fn from_columns(
        finished_at: Option<DateTime<Utc>>,
        outcome: Option<&str>,
    ) -> Result<Self, SchedulerError> {
        match (finished_at, outcome) {
            (None, None) => Ok(Self::Open),
            (Some(finished_at), Some(outcome)) => Ok(Self::Closed {
                finished_at,
                outcome: outcome
                    .parse()
                    .map_err(|_| SchedulerError::Invariant("unknown attempt outcome"))?,
            }),
            _ => Err(SchedulerError::Invariant(
                "attempt finished_at and outcome are not set together",
            )),
        }
    }
}

/// A task's current attempt (the one whose `lock_token` is the task's).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttemptRun {
    /// When it was claimed.
    pub started_at: DateTime<Utc>,
    /// Open or closed.
    pub state: AttemptState,
}

/// A task handed to a worker by `store::claim`.
#[derive(Debug, Clone)]
pub struct ClaimedTask {
    /// Ownership of this attempt; every later transition checks it.
    pub lease: TaskLease,
    /// The queue it was claimed from.
    pub queue: QueueName,
    /// The producer's idempotency key.
    pub task_key: TaskKey,
    /// Selects the handler.
    pub task_name: TaskName,
    /// The task's arguments as JSON.
    pub payload: serde_json::Value,
    /// Attempts allowed in total.
    pub max_attempts: MaxAttempts,
    /// When the task became due.
    pub due_at: DateTime<Utc>,
    /// Database time of the claim.
    pub claimed_at: DateTime<Utc>,
    /// Lease expiry as of the claim.
    pub locked_until: DateTime<Utc>,
}

/// What `store::retry` did with the task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryOutcome {
    /// Requeued; due again at `due_at` (PostgreSQL time).
    Queued {
        /// When the task is due again.
        due_at: DateTime<Utc>,
    },
    /// Attempts exhausted; the task is now dead.
    Dead,
}

/// A command written to `queue_outbox` atomically with task completion.
#[derive(Debug, Clone)]
pub struct OutboxCommand {
    /// Names the effect, for example `"order-shipped"`. A task records each effect at
    /// most once: the row is unique on `(task_key, effect_name)`.
    pub effect_name: EffectName,
    /// Unique across the outbox. Receivers deduplicate by it, so derive it from the
    /// `task_key` and `effect_name`.
    pub command_key: CommandKey,
    /// Where the application's relay publishes the command.
    pub topic: Topic,
    /// The command body.
    pub payload: serde_json::Value,
}

/// What `store::complete_with_outbox` did with the outbox row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxWrite {
    /// A new outbox row was written.
    Inserted,
    /// The task already had this effect with identical content; nothing was written.
    AlreadyPresent,
}

/// Every `queue_tasks` column as a domain type.
#[derive(Debug, Clone)]
pub struct TaskView {
    /// Row id.
    pub id: TaskId,
    /// The producer's idempotency key.
    pub task_key: TaskKey,
    /// Selects the handler.
    pub task_name: TaskName,
    /// The queue it runs in.
    pub queue: QueueName,
    /// The task's arguments as JSON.
    pub payload: serde_json::Value,
    /// Lifecycle state, with the lease while running and the completion time once finished.
    pub state: TaskState,
    /// Higher is claimed first, within its queue.
    pub priority: i32,
    /// Earliest time a worker may claim it.
    pub due_at: DateTime<Utc>,
    /// Attempts counted so far.
    pub attempts: u32,
    /// Attempts allowed in total.
    pub max_attempts: MaxAttempts,
    /// Fencing token of the latest claim.
    pub lock_token: LockToken,
    /// Message of the latest failure.
    pub last_error: Option<String>,
    /// When it was enqueued.
    pub created_at: DateTime<Utc>,
    /// When the row last changed.
    pub updated_at: DateTime<Utc>,
    /// The admin action that last cancelled or replayed this task.
    pub admin_action_id: Option<i64>,
}

/// One `queue_task_attempts` row.
#[derive(Debug, Clone)]
pub struct Attempt {
    /// 1 for the first attempt.
    pub attempt_no: AttemptNo,
    /// Fencing token of the claim that started it.
    pub lock_token: LockToken,
    /// The worker that ran it.
    pub worker_id: WorkerId,
    /// When it was claimed.
    pub started_at: DateTime<Utc>,
    /// Open, or when and how it closed.
    pub state: AttemptState,
    /// The failure message, if it failed.
    pub error: Option<String>,
}

/// Proof of ownership of one claimed attempt; every worker-owned mutation is
/// guarded by it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskLease {
    /// The claimed task.
    pub task_id: TaskId,
    /// The worker that claimed it.
    pub worker_id: WorkerId,
    /// The claim's fencing token.
    pub lock_token: LockToken,
    /// Which attempt this is, from 1.
    pub attempt_no: AttemptNo,
}

/// What one `store::reap_expired` pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReapSummary {
    /// Expired tasks put back to `queued`.
    pub requeued: usize,
    /// Expired tasks with no attempts left, marked `dead`.
    pub dead: usize,
}

/// Rows deleted by one `store::cleanup` pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CleanupSummary {
    /// `done`, `dead` and `cancelled` task rows deleted, with their attempts.
    pub tasks: usize,
    /// Published outbox rows deleted.
    pub outbox: usize,
}

const DEFAULT_CLEANUP_BATCH: NonZeroU32 = NonZeroU32::new(1000).unwrap();

/// How long terminal rows are kept; `batch_size` bounds each delete statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    /// How long `done` tasks are kept after completion; their `task_key` stays reserved for
    /// this long. Default 30 days.
    pub done: Duration,
    /// How long `dead` and `cancelled` tasks are kept after completion; their `task_key`
    /// stays reserved for this long. Default 30 days.
    pub dead: Duration,
    /// How long published outbox rows are kept after `published_at`. Default 7 days.
    pub outbox_published: Duration,
    /// Most rows one delete statement removes. Default 1000.
    pub batch_size: NonZeroU32,
}

impl Default for Retention {
    fn default() -> Self {
        const DAY: u64 = 86_400;
        Self {
            done: Duration::from_secs(30 * DAY),
            dead: Duration::from_secs(30 * DAY),
            outbox_published: Duration::from_secs(7 * DAY),
            batch_size: DEFAULT_CLEANUP_BATCH,
        }
    }
}

/// Queued tasks of one queue, name and priority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueDepth {
    /// The queue.
    pub queue: QueueName,
    /// The task name.
    pub task_name: TaskName,
    /// The priority.
    pub priority: i32,
    /// Queued tasks in this group, due or not.
    pub queued: i64,
    /// Of `queued`, those whose `due_at` has passed.
    pub due: i64,
    /// Age of the oldest due task; `None` if none is due.
    pub oldest_due_age: Option<Duration>,
}

/// Point-in-time queue health from `store::queue_stats`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueStats {
    /// Tasks in `queued`, across all queues.
    pub queued: i64,
    /// Tasks in `running`.
    pub running: i64,
    /// Retained `done` tasks.
    pub done: i64,
    /// Retained `dead` tasks.
    pub dead: i64,
    /// Queued tasks whose `due_at` has passed.
    pub due: i64,
    /// Age of the oldest due task; `None` if none is due.
    pub oldest_due_age: Option<Duration>,
    /// Queued tasks broken down by queue, name and priority.
    pub depth: Vec<QueueDepth>,
    /// Outbox rows not yet published.
    pub outbox_unpublished: i64,
    /// Age of the oldest unpublished outbox row; `None` if there is none.
    pub oldest_unpublished_age: Option<Duration>,
}

/// `Duration` to a pure-microsecond PG interval (sub-microsecond parts truncate). Fails on
/// more than `i64::MAX` microseconds, which no interval holds.
pub(crate) fn pg_interval(d: Duration) -> Result<PgInterval, SchedulerError> {
    i64::try_from(d.as_micros())
        .map(PgInterval::from_microseconds)
        .map_err(|_| SchedulerError::Config("duration too large for a PostgreSQL interval".into()))
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

/// Longest duration the scheduler accepts for any setting: 5 years (5 × 365 days). Every
/// duration becomes a PostgreSQL interval added to or subtracted from `now()`, and a far
/// larger one (such as `Duration::MAX`) fails each such statement with
/// "timestamp out of range".
pub(crate) const MAX_DURATION: Duration = Duration::from_secs(5 * 365 * 86_400);

/// How long a claim or heartbeat leases a task, in PostgreSQL time: from 1 microsecond to
/// 5 years. Build one with `LeaseDuration::try_from(Duration::from_secs(60))`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseDuration(Duration);

/// Why a `Duration` was rejected as a [`LeaseDuration`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LeaseDurationError {
    /// Under 1 microsecond, which a PostgreSQL interval truncates to zero.
    #[error("lease_duration must be at least 1 microsecond")]
    BelowOneMicrosecond,
    /// Over 5 years.
    #[error("lease_duration must be at most 5 years")]
    AboveFiveYears,
}

impl TryFrom<Duration> for LeaseDuration {
    type Error = LeaseDurationError;
    fn try_from(d: Duration) -> Result<Self, LeaseDurationError> {
        if d.as_micros() == 0 {
            Err(LeaseDurationError::BelowOneMicrosecond)
        } else if d > MAX_DURATION {
            Err(LeaseDurationError::AboveFiveYears)
        } else {
            Ok(Self(d))
        }
    }
}

impl LeaseDuration {
    /// 60 seconds, the builder's default.
    pub(crate) const DEFAULT: Self = Self(Duration::from_secs(60));

    /// The lease length.
    pub fn get(self) -> Duration {
        self.0
    }
}

/// How many attempts a task gets before it is marked `dead`; at least 1. Build one
/// with `MaxAttempts::try_from(5u32)`. The default is 3.
//
// Private `NonZeroI32` with the invariant `>= 1`, enforced by every constructor — so the
// stored-`i32` conversion is total.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaxAttempts(NonZeroI32);

/// Why a value was rejected as a [`MaxAttempts`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MaxAttemptsError {
    /// Zero, or a stored value that is not positive.
    #[error("max_attempts must be greater than zero")]
    NonPositive,
    /// Above `i32::MAX`, the largest value the database column holds.
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

/// Which attempt of a task this is, counted from 1.
//
// Private `NonZeroI32` with the invariant `>= 1`, like [`MaxAttempts`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttemptNo(NonZeroI32);

/// Why a value was rejected as an [`AttemptNo`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AttemptNoError {
    /// Zero, or a stored value that is not positive.
    #[error("attempt_no must be greater than zero")]
    NonPositive,
    /// Above `i32::MAX`, the largest value the database column holds.
    #[error("attempt_no exceeds i32::MAX")]
    TooLarge,
}

impl TryFrom<u32> for AttemptNo {
    type Error = AttemptNoError;
    fn try_from(v: u32) -> Result<Self, AttemptNoError> {
        let i = i32::try_from(v).map_err(|_| AttemptNoError::TooLarge)?;
        NonZeroI32::new(i)
            .map(AttemptNo)
            .ok_or(AttemptNoError::NonPositive)
    }
}

impl AttemptNo {
    /// The first attempt.
    pub const FIRST: AttemptNo = AttemptNo(NonZeroI32::new(1).unwrap());

    /// Read boundary: parse the stored signed number. Rejects `<= 0`.
    pub(crate) fn from_db_i32(i: i32) -> Result<Self, AttemptNoError> {
        NonZeroI32::new(i)
            .filter(|n| n.get() > 0)
            .map(AttemptNo)
            .ok_or(AttemptNoError::NonPositive)
    }

    /// Total: the invariant guarantees `1..=i32::MAX`.
    #[cfg(test)]
    pub(crate) fn to_i32(self) -> i32 {
        self.0.get()
    }

    /// The attempt number, from 1.
    pub fn get(self) -> u32 {
        self.0.get().unsigned_abs()
    }
}

impl std::fmt::Display for AttemptNo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.get().fmt(f)
    }
}

impl MaxAttempts {
    pub(crate) const fn default_value() -> Self {
        Self(NonZeroI32::new(3).unwrap())
    }
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

    /// The attempt count.
    //
    // Total: the inner is a validated `1..=i32::MAX`, so `i32::unsigned_abs` is an exact
    // i32->u32 — no `as` cast, no `unwrap`.
    pub fn get(self) -> u32 {
        self.0.get().unsigned_abs()
    }
}

#[cfg(test)]
mod task_enum_tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn status_round_trips() {
        for s in [
            TaskStatus::Queued,
            TaskStatus::Running,
            TaskStatus::Done,
            TaskStatus::Dead,
            TaskStatus::Cancelled,
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
            AttemptOutcome::Released,
            AttemptOutcome::Cancelled,
        ] {
            assert_eq!(AttemptOutcome::from_str(o.as_str()), Ok(o));
        }
        assert_eq!(AttemptOutcome::LeaseExpired.as_str(), "lease_expired");
    }

    #[test]
    fn interval_conversions() {
        assert_eq!(
            interval_to_duration(pg_interval(Duration::from_millis(1500)).unwrap()),
            Duration::from_millis(1500)
        );
        assert_eq!(
            interval_to_duration(PgInterval::from_microseconds(-5)),
            Duration::ZERO
        );
        let day = PgInterval::new(0, 1, 0);
        assert_eq!(interval_to_duration(day), Duration::from_secs(86_400));
    }

    #[test]
    fn lease_duration_bounds() {
        assert_eq!(
            LeaseDuration::try_from(Duration::from_nanos(999)),
            Err(LeaseDurationError::BelowOneMicrosecond)
        );
        assert!(LeaseDuration::try_from(Duration::from_micros(1)).is_ok());
        assert_eq!(
            LeaseDuration::try_from(Duration::from_secs(157_680_001)),
            Err(LeaseDurationError::AboveFiveYears)
        );
    }

    #[test]
    fn pg_interval_rejects_what_it_cannot_represent() {
        assert!(pg_interval(Duration::MAX).is_err());
        assert_eq!(
            pg_interval(Duration::from_millis(1500))
                .unwrap()
                .microseconds,
            1_500_000
        );
    }
}

#[cfg(test)]
mod state_tests {
    use super::*;

    #[test]
    fn task_state_accepts_each_checked_shape() {
        let w = WorkerId::try_from("w").unwrap();
        let t = Utc::now();
        assert_eq!(
            TaskState::from_columns("queued", None, None, None).unwrap(),
            TaskState::Queued
        );
        assert!(matches!(
            TaskState::from_columns("running", Some(w), Some(t), None).unwrap(),
            TaskState::Running { .. }
        ));
        assert_eq!(
            TaskState::from_columns("cancelled", None, None, Some(t)).unwrap(),
            TaskState::Finished {
                status: TerminalStatus::Cancelled,
                completed_at: t
            }
        );
    }

    #[test]
    fn task_state_rejects_shapes_the_checks_forbid() {
        let t = Utc::now();
        for (s, by, until, done) in [
            ("running", None, Some(t), None),
            ("queued", None, None, Some(t)),
            ("done", None, None, None),
            ("pending", None, None, None),
        ] {
            let r = TaskState::from_columns(s, by, until, done);
            assert!(matches!(r, Err(SchedulerError::Invariant(_))), "{s}: {r:?}");
        }
    }

    #[test]
    fn attempt_state_pairs_finish_and_outcome() {
        let t = Utc::now();
        assert_eq!(
            AttemptState::from_columns(None, None).unwrap(),
            AttemptState::Open
        );
        assert!(AttemptState::from_columns(Some(t), None).is_err());
        assert!(AttemptState::from_columns(None, Some("done")).is_err());
    }
}

#[cfg(test)]
mod max_attempts_tests {
    use super::MaxAttempts;
    use super::MaxAttemptsError;

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

    #[test]
    fn attempt_no_rejects_zero_and_above_i32_max() {
        use super::AttemptNo;
        use super::AttemptNoError;
        assert_eq!(AttemptNo::try_from(0u32), Err(AttemptNoError::NonPositive));
        assert_eq!(AttemptNo::try_from(u32::MAX), Err(AttemptNoError::TooLarge));
        assert_eq!(AttemptNo::from_db_i32(0), Err(AttemptNoError::NonPositive));
        assert_eq!(AttemptNo::from_db_i32(-1), Err(AttemptNoError::NonPositive));
        assert_eq!(AttemptNo::from_db_i32(2).unwrap().get(), 2);
        assert_eq!(AttemptNo::FIRST.get(), 1);
        assert_eq!(AttemptNo::try_from(7u32).unwrap().to_i32(), 7);
    }
}

#[cfg(feature = "cron")]
/// A job's lifecycle state. Used at both the creation and read boundaries; it is
/// `Paused` exactly when the job has a `scheduler_job_pauses` row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobLifecycle {
    /// Creates a task for each occurrence.
    Active,
    /// Creates no tasks until resumed.
    Paused,
}

#[cfg(feature = "cron")]
/// Public domain projection of a `scheduler_jobs` row and its pause state. Every
/// field is a domain type; `job_args` is intentionally omitted (reading is untyped —
/// the handler argument type is not recoverable from a runtime `TaskName`). Not
/// `Serialize`: it must not become an accidental wire contract.
#[derive(Debug, Clone)]
pub struct Job {
    /// Row id.
    pub id: JobId,
    /// The schedule's unique name.
    pub name: ScheduleName,
    /// The task each occurrence runs; many schedules may share one.
    pub task_name: TaskName,
    /// When it fires.
    pub cron: CronExpression,
    /// Attempts each task it creates gets.
    pub max_attempts: MaxAttempts,
    /// The queue its tasks go to.
    pub queue: QueueName,
    /// Active or paused.
    pub lifecycle: JobLifecycle,
    /// The next occurrence to turn into a task.
    pub next_run_at: DateTime<Utc>,
    /// When it was created.
    pub created_at: DateTime<Utc>,
    /// When the row last changed (pausing and resuming do not change it).
    pub updated_at: DateTime<Utc>,
}

#[cfg(feature = "cron")]
impl Job {
    /// Project a row whose pause state the caller read alongside it.
    pub(crate) fn from_row(
        row: SchedulerJob,
        lifecycle: JobLifecycle,
    ) -> Result<Self, SchedulerError> {
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
            task_name: row.task_name,
            cron,
            max_attempts,
            queue: row.queue,
            lifecycle,
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
    pub name: ScheduleName,
    pub task_name: TaskName,
    pub cron_expression: String,
    pub job_args: serde_json::Value,
    pub next_run_at: DateTime<Utc>,
    pub max_attempts: i32,
    pub queue: QueueName,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[cfg(feature = "cron")]
#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = scheduler_jobs)]
pub struct NewJob {
    pub name: ScheduleName,
    pub task_name: TaskName,
    pub cron_expression: String,
    pub job_args: serde_json::Value,
    pub next_run_at: DateTime<Utc>,
    pub max_attempts: i32,
    pub queue: QueueName,
}
