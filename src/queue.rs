//! Transactional, idempotent task enqueueing.

use chrono::DateTime;
use chrono::Utc;
use diesel::sql_types;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use serde::de::DeserializeOwned;

use crate::error::SchedulerError;
use crate::ids::QueueName;
use crate::ids::SCHEDULE_KEY_PREFIX;
use crate::ids::TaskId;
use crate::ids::TaskKey;
use crate::ids::TaskName;
use crate::models::MaxAttempts;
use crate::models::TaskStatus;

/// Connects a stable queue name to its serialized argument type. The same type is
/// used for enqueueing and handler registration, preventing name/payload drift in
/// normal Rust use.
pub trait Task: Send + Sync + 'static {
    /// The task name stored with each task and matched to its handler. Must be a valid
    /// [`TaskName`].
    const NAME: &'static str;
    /// Queue this task type is enqueued to unless [`EnqueueOptions::queue`] overrides it.
    /// Workers serve it only if they run that queue.
    const QUEUE: &'static str = "default";
    /// The task's arguments, stored as JSON. `PartialEq` lets producers check that the
    /// arguments decode back equal to the caller's value, since JSON cannot represent every
    /// value (serde_json writes a non-finite `f64` as `null`, which an `Option<f64>` decodes as
    /// `None`); [`enqueue`] and `CreateJob::for_task` reject those with
    /// [`SchedulerError::PayloadNotPreserved`].
    type Args: serde::Serialize + DeserializeOwned + PartialEq + Send + 'static;
}

/// Higher value is claimed first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Priority(i32);

impl Priority {
    /// Priority 0, the default.
    pub const NORMAL: Self = Self(0);

    /// A priority of `value`; negative values run after [`Priority::NORMAL`].
    pub const fn new(value: i32) -> Self {
        Self(value)
    }

    /// The numeric value.
    pub const fn get(self) -> i32 {
        self.0
    }
}

/// When and how to run an enqueued task. Start from [`immediate`](Self::immediate) or
/// [`at`](Self::at).
#[derive(Debug, Clone)]
pub struct EnqueueOptions {
    due_at: Option<DateTime<Utc>>,
    priority: Priority,
    max_attempts: MaxAttempts,
    queue: Option<QueueName>,
}

impl Default for EnqueueOptions {
    fn default() -> Self {
        Self {
            due_at: None,
            priority: Priority::NORMAL,
            max_attempts: MaxAttempts::default_value(),
            queue: None,
        }
    }
}

impl EnqueueOptions {
    /// Due now, priority 0, 3 attempts, in `Task::QUEUE`.
    pub fn immediate() -> Self {
        Self::default()
    }

    /// Like [`immediate`](Self::immediate), but not claimed before `due_at`. The instant is
    /// rounded up to the next whole microsecond (PostgreSQL's resolution), so the task is
    /// never eligible before it; the returned `due_at` is the effective instant. A leap-second
    /// instant (`:60.f`) is stored as the same fraction of the following second, later and
    /// never earlier.
    pub fn at(due_at: DateTime<Utc>) -> Self {
        Self {
            due_at: Some(due_at),
            ..Self::default()
        }
    }

    /// Sets the priority; higher is claimed first within the queue.
    pub fn priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    /// Sets how many attempts the task gets before it is marked `dead` (default 3).
    pub fn max_attempts(mut self, max_attempts: MaxAttempts) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Enqueue to `queue` instead of `Task::QUEUE`.
    pub fn queue(mut self, queue: QueueName) -> Self {
        self.queue = Some(queue);
        self
    }
}

/// Result of [`enqueue`]. `inserted` is false when the key already existed with
/// the same task name and equal arguments (the existing task is returned).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Enqueued {
    /// The task's id.
    pub id: TaskId,
    /// Its status now; for an existing task this may be any status.
    pub status: TaskStatus,
    /// When it is due.
    pub due_at: DateTime<Utc>,
    /// `true` if this call created the task.
    pub inserted: bool,
}

#[derive(diesel::QueryableByName)]
struct EnqueuedRow {
    #[diesel(sql_type = sql_types::BigInt)]
    id: TaskId,
    #[diesel(sql_type = sql_types::Text)]
    status: String,
    #[diesel(sql_type = sql_types::Timestamptz)]
    due_at: DateTime<Utc>,
}

#[derive(diesel::QueryableByName)]
struct ExistingRow {
    #[diesel(sql_type = sql_types::BigInt)]
    id: TaskId,
    #[diesel(sql_type = sql_types::Text)]
    status: String,
    #[diesel(sql_type = sql_types::Timestamptz)]
    due_at: DateTime<Utc>,
    #[diesel(sql_type = sql_types::Bool)]
    same_name: bool,
    #[diesel(sql_type = sql_types::Text)]
    stored: String,
}

impl EnqueuedRow {
    fn into_enqueued(self, inserted: bool) -> Result<Enqueued, SchedulerError> {
        Ok(Enqueued {
            id: self.id,
            status: self
                .status
                .parse()
                .map_err(|_| SchedulerError::Invariant("unknown task status"))?,
            due_at: self.due_at,
            inserted,
        })
    }
}

// The wakeup is part of the statement (here and in `REPLAY`, `REPLAY_BATCH` and `RESUME`), so
// it commits or fails with the change: a failing `pg_notify` rolls the change back instead of
// turning a committed change into an error. It runs once per changed row and not at all when
// nothing changed; PostgreSQL delivers one notification per transaction however many are sent.
const INSERT: &str = r#"
    WITH ins AS (
        INSERT INTO queue_tasks (task_key, task_name, payload, priority, due_at, max_attempts, queue)
        VALUES ($1, $2, $3, $4, COALESCE($5, statement_timestamp()), $6, $7)
        ON CONFLICT (task_key) DO NOTHING
        RETURNING id, status, due_at
    )
    SELECT id, status, due_at, pg_notify('queue_available', '') FROM ins
"#;

// A retry matches when the task name is the same and the stored payload, decoded as the worker
// decodes it (text, then `Value`, then `T::Args`), equals the new arguments by `PartialEq`.
// Equal sets match in any serialization order, and `Value` still tells `1` from `1.0`.
const EXISTING: &str = r#"
    SELECT id, status, due_at, task_name = $2 AS same_name, payload::text AS stored
    FROM queue_tasks
    WHERE task_key = $1
"#;

/// Enqueue a task on the caller's connection. If that connection is in a
/// transaction, the task (and its notification) commit or roll back with the
/// caller's data. Re-enqueueing the same `key` with the same task name and arguments returns
/// the existing task: the stored payload, decoded as `T::Args` the way the worker decodes it,
/// must equal `args` by `PartialEq` (equal `HashSet`s match whatever order they serialize in).
/// Anything else, including a stored payload that does not decode, is an
/// `IdempotencyConflict`. The queue (`options.queue`, else `T::QUEUE`) is not part of
/// that comparison, like priority.
/// `T::Args` must be `PartialEq`: the arguments are stored only if they decode back equal to
/// the caller's value, both from the JSON sent and from what PostgreSQL stores. JSON cannot
/// represent every value (`Some(f64::INFINITY)` is written as `null` and decodes as `None`), and
/// JSONB normalizes numbers (an `f64` of `1e18` reads back as the integer
/// `1000000000000000000`); each is [`SchedulerError::PayloadNotPreserved`] when the arguments
/// then decode as a different value (an untagged enum picks another variant; a plain `f64`
/// field decodes equal and is accepted), and nothing is written. Arguments that do not
/// serialize to JSON the worker can parse back and decode as `T::Args` at all (nested deeper than
/// 128, or a bare `f64` NaN, which serializes as `null`) are [`SchedulerError::Serde`]. With
/// `Args = serde_json::Value` this rejects any `f64` of magnitude 1e16 or more in the 64-bit
/// integer range, such as `json!({"x": 1e18})`: `serde_json` writes it in exponent form, and
/// the handler would receive an integer. That check costs one extra round trip.
/// A key starting with `schedule:` is reserved for the scheduler and returns
/// [`SchedulerError::ReservedTaskKey`]. If concurrent cleanup keeps deleting the conflicting
/// task before it can be read, three passes later it returns [`SchedulerError::Contention`].
pub async fn enqueue<T: Task>(
    conn: &mut AsyncPgConnection,
    key: TaskKey,
    args: T::Args,
    options: EnqueueOptions,
) -> Result<Enqueued, SchedulerError> {
    if key.as_str().starts_with(SCHEDULE_KEY_PREFIX) {
        return Err(SchedulerError::ReservedTaskKey(key));
    }
    let name = TaskName::try_from(T::NAME)?;
    let queue = match options.queue {
        Some(q) => q,
        None => QueueName::try_from(T::QUEUE)?,
    };
    let due_at = options
        .due_at
        .map(crate::models::round_up_to_micros)
        .transpose()?;
    let payload = typed_payload(&args)?;
    check_stored::<T::Args>(conn, &payload).await?;

    // The incumbent can be deleted (retention cleanup) between the conflicting insert and
    // the read-back; the next pass then simply inserts.
    for _ in 0..3 {
        let inserted = diesel::sql_query(INSERT)
            .bind::<sql_types::Text, _>(&key)
            .bind::<sql_types::Text, _>(&name)
            .bind::<sql_types::Jsonb, _>(&payload)
            .bind::<sql_types::Integer, _>(options.priority.get())
            .bind::<sql_types::Nullable<sql_types::Timestamptz>, _>(due_at)
            .bind::<sql_types::Integer, _>(options.max_attempts.to_i32())
            .bind::<sql_types::Text, _>(&queue)
            .get_results::<EnqueuedRow>(conn)
            .await?
            .pop();
        if let Some(row) = inserted {
            return row.into_enqueued(true);
        }

        let existing = diesel::sql_query(EXISTING)
            .bind::<sql_types::Text, _>(&key)
            .bind::<sql_types::Text, _>(&name)
            .get_results::<ExistingRow>(conn)
            .await?
            .pop();
        let Some(existing) = existing else {
            continue;
        };
        if !existing.same_name
            || !decode_stored::<T::Args>(&existing.stored).is_ok_and(|stored| stored == args)
        {
            return Err(SchedulerError::IdempotencyConflict(key));
        }
        return EnqueuedRow {
            id: existing.id,
            status: existing.status,
            due_at: existing.due_at,
        }
        .into_enqueued(false);
    }
    Err(SchedulerError::Contention(
        "task_key vanished during enqueue",
    ))
}

// Administrative replay: dead or cancelled → queued with the same task_key, and lock_token + 1
// fences a very late worker. The audit row and its one membership row (the task it changed,
// queue_admin_action_tasks) commit with the transition.
#[doc(hidden)]
pub const REPLAY: &str = r#"
    WITH target AS (
        SELECT id, task_name, queue FROM queue_tasks
        WHERE id = $1 AND status IN ('dead', 'cancelled')
        FOR UPDATE
    ), act AS (
        INSERT INTO queue_admin_actions (action, task_name, queue, operator, reason)
        SELECT 'replay', task_name, queue, $2, $3 FROM target
        RETURNING id
    ), replayed AS (
        UPDATE queue_tasks t
        SET status = 'queued', attempts = 0, due_at = statement_timestamp(), locked_by = NULL,
            lock_token = lock_token + 1, locked_until = NULL, last_error = NULL,
            completed_at = NULL, updated_at = statement_timestamp()
        FROM target
        WHERE t.id = target.id
        RETURNING t.id
    ), member AS (
        INSERT INTO queue_admin_action_tasks (action_id, action, task_id)
        SELECT act.id, 'replay', replayed.id FROM act, replayed
    )
    SELECT pg_notify('queue_available', '') FROM replayed
"#;

/// Requeue a dead or cancelled task with a fresh attempt budget and audit it. `false` if the task
/// is missing or neither. Keeps the same `task_key`; use a new key for a genuinely new effect.
pub async fn replay(
    conn: &mut AsyncPgConnection,
    id: TaskId,
    operator: &str,
    reason: &str,
) -> Result<bool, SchedulerError> {
    let n = diesel::sql_query(REPLAY)
        .bind::<sql_types::BigInt, _>(id)
        .bind::<sql_types::Text, _>(operator)
        .bind::<sql_types::Text, _>(reason)
        .execute(conn)
        .await?;
    Ok(n == 1)
}

/// Serialize `args` to the JSON the worker will later parse. `serde_json::to_value`
/// has no nesting limit but `from_str` (used when claiming) does, so a value is
/// accepted only if its text parses back.
pub(crate) fn to_payload<T: serde::Serialize>(
    args: &T,
) -> Result<serde_json::Value, SchedulerError> {
    let text = serde_json::to_string(args)?;
    Ok(serde_json::from_str(&text)?)
}

/// [`to_payload`] for a typed producer: the payload is accepted only if it decodes back as
/// `A` equal to `args`, which catches values JSON cannot represent (a non-finite `f64` is
/// written as `null`). A payload that does not decode at all is [`SchedulerError::Serde`].
pub(crate) fn typed_payload<A: serde::Serialize + DeserializeOwned + PartialEq>(
    args: &A,
) -> Result<serde_json::Value, SchedulerError> {
    let payload = to_payload(args)?;
    if serde_json::from_value::<A>(payload.clone())? != *args {
        return Err(SchedulerError::PayloadNotPreserved);
    }
    Ok(payload)
}

#[derive(diesel::QueryableByName)]
struct StoredRow {
    #[diesel(sql_type = sql_types::Text)]
    stored: String,
}

/// Decode stored payload text as the worker does: parsed as a `Value`, then as `A`.
fn decode_stored<A: DeserializeOwned>(text: &str) -> Result<A, serde_json::Error> {
    serde_json::from_value(serde_json::from_str::<serde_json::Value>(text)?)
}

/// Check that `payload` decodes as the same `A` once stored as JSONB, as the worker will
/// decode it. JSONB normalizes numbers, so a value can decode differently (`1e18` reads back
/// as an integer). A decode failure is [`SchedulerError::Serde`]; a different value is
/// [`SchedulerError::PayloadNotPreserved`].
pub(crate) async fn check_stored<A: DeserializeOwned + PartialEq>(
    conn: &mut AsyncPgConnection,
    payload: &serde_json::Value,
) -> Result<(), SchedulerError> {
    let text = diesel::sql_query("SELECT $1::jsonb::text AS stored")
        .bind::<sql_types::Jsonb, _>(payload)
        .get_result::<StoredRow>(conn)
        .await?
        .stored;
    let decoded = decode_stored::<A>(&text)?;
    // JSONB stores `-0.0` as `0.0`, and `-0.0 == 0.0`, so a negative zero is accepted.
    if decoded != serde_json::from_value::<A>(payload.clone())? {
        return Err(SchedulerError::PayloadNotPreserved);
    }
    Ok(())
}
