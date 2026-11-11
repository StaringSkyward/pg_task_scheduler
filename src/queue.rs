//! Transactional, idempotent task enqueueing.

use chrono::{DateTime, Utc};
use diesel::sql_types;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde::de::DeserializeOwned;

use crate::error::SchedulerError;
use crate::ids::{QueueName, TaskId, TaskKey, TaskName};
use crate::models::{MaxAttempts, TaskStatus};

/// Connects a stable queue name to its serialized argument type. The same type is
/// used for enqueueing and handler registration, preventing name/payload drift in
/// normal Rust use.
pub trait Task: Send + Sync + 'static {
    const NAME: &'static str;
    /// Queue this task type is enqueued to unless [`EnqueueOptions::queue`] overrides it.
    /// Workers serve it only if they run that queue.
    const QUEUE: &'static str = "default";
    type Args: serde::Serialize + DeserializeOwned + Send + 'static;
}

/// Higher value is claimed first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Priority(i32);

impl Priority {
    pub const NORMAL: Self = Self(0);

    pub const fn new(value: i32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> i32 {
        self.0
    }
}

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
    pub fn immediate() -> Self {
        Self::default()
    }

    pub fn at(due_at: DateTime<Utc>) -> Self {
        Self {
            due_at: Some(due_at),
            ..Self::default()
        }
    }

    pub fn priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

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
/// the same task name and payload (the existing task is returned).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Enqueued {
    pub id: TaskId,
    pub status: TaskStatus,
    pub due_at: DateTime<Utc>,
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
    same: bool,
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

const INSERT: &str = r#"
    INSERT INTO queue_tasks (task_key, task_name, payload, priority, due_at, max_attempts, queue)
    VALUES ($1, $2, $3, $4, COALESCE($5, now()), $6, $7)
    ON CONFLICT (task_key) DO NOTHING
    RETURNING id, status, due_at
"#;

// `payload = $3` is JSONB equality: key order and whitespace do not matter.
const EXISTING: &str = r#"
    SELECT id, status, due_at, (task_name = $2 AND payload = $3) AS same
    FROM queue_tasks
    WHERE task_key = $1
"#;

/// Enqueue a task on the caller's connection. If that connection is in a
/// transaction, the task (and its notification) commit or roll back with the
/// caller's data. Re-enqueueing the same `key` with the same task name and payload
/// returns the existing task; different content is an `IdempotencyConflict`. The queue
/// (`options.queue`, else `T::QUEUE`) is not part of that comparison, like priority.
pub async fn enqueue<T: Task>(
    conn: &mut AsyncPgConnection,
    key: TaskKey,
    args: T::Args,
    options: EnqueueOptions,
) -> Result<Enqueued, SchedulerError> {
    let name = TaskName::try_from(T::NAME)?;
    let queue = match options.queue {
        Some(q) => q,
        None => QueueName::try_from(T::QUEUE)?,
    };
    let payload = serde_json::to_value(args)?;

    let inserted = diesel::sql_query(INSERT)
        .bind::<sql_types::Text, _>(&key)
        .bind::<sql_types::Text, _>(&name)
        .bind::<sql_types::Jsonb, _>(&payload)
        .bind::<sql_types::Integer, _>(options.priority.get())
        .bind::<sql_types::Nullable<sql_types::Timestamptz>, _>(options.due_at)
        .bind::<sql_types::Integer, _>(options.max_attempts.to_i32())
        .bind::<sql_types::Text, _>(&queue)
        .get_results::<EnqueuedRow>(conn)
        .await?
        .pop();
    if let Some(row) = inserted {
        diesel::sql_query("SELECT pg_notify('queue_available', '')")
            .execute(conn)
            .await?;
        return row.into_enqueued(true);
    }

    let existing = diesel::sql_query(EXISTING)
        .bind::<sql_types::Text, _>(&key)
        .bind::<sql_types::Text, _>(&name)
        .bind::<sql_types::Jsonb, _>(&payload)
        .get_results::<ExistingRow>(conn)
        .await?
        .pop()
        .ok_or(SchedulerError::Invariant(
            "conflicting task_key not visible",
        ))?;
    if !existing.same {
        return Err(SchedulerError::IdempotencyConflict(key));
    }
    EnqueuedRow {
        id: existing.id,
        status: existing.status,
        due_at: existing.due_at,
    }
    .into_enqueued(false)
}

// §18 replay; the audit row carries the new token. No FK, so it outlives retention.
const REPLAY: &str = r#"
    WITH replayed AS (
        UPDATE queue_tasks
        SET status = 'queued', attempts = 0, due_at = now(), locked_by = NULL,
            lock_token = lock_token + 1, locked_until = NULL, last_error = NULL,
            completed_at = NULL, updated_at = now()
        WHERE id = $1 AND status = 'dead'
        RETURNING id, task_key, lock_token
    )
    INSERT INTO queue_task_replays (task_id, task_key, lock_token, operator, reason)
    SELECT id, task_key, lock_token, $2, $3 FROM replayed
"#;

/// Requeue a dead task with a fresh attempt budget and audit it. `false` if the task is missing
/// or not dead. Keeps the same `task_key`; use a new key for a genuinely new effect (§18).
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
    if n == 1 {
        diesel::sql_query("SELECT pg_notify('queue_available', '')")
            .execute(conn)
            .await?;
    }
    Ok(n == 1)
}
