//! Transactional immediate and delayed task enqueueing.

use chrono::{DateTime, Utc};
use diesel::sql_types;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde::de::DeserializeOwned;

use crate::error::SchedulerError;
use crate::ids::{DeduplicationKey, JobName, RunId};
use crate::models::{LeaseDuration, MaxAttempts, RetryBackoff};

/// Connects a stable queue name to its serialized argument type. The same type is
/// used for enqueueing and handler registration, preventing name/payload drift in
/// normal Rust use.
pub trait Task: Send + Sync + 'static {
    const NAME: &'static str;
    type Args: serde::Serialize + DeserializeOwned + Send + 'static;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Priority(i16);

impl Priority {
    pub const NORMAL: Self = Self(0);

    pub const fn new(value: i16) -> Self {
        Self(value)
    }

    pub const fn get(self) -> i16 {
        self.0
    }
}

#[derive(Debug, Clone)]
pub enum Availability {
    Immediate,
    At(DateTime<Utc>),
}

#[derive(Debug, Clone, Default)]
pub enum Deduplication {
    #[default]
    None,
    Key(DeduplicationKey),
}

#[derive(Debug, Clone)]
pub struct EnqueueOptions {
    availability: Availability,
    priority: Priority,
    lease_duration: LeaseDuration,
    max_attempts: MaxAttempts,
    retry_backoff: RetryBackoff,
    deduplication: Deduplication,
}

impl Default for EnqueueOptions {
    fn default() -> Self {
        Self {
            availability: Availability::Immediate,
            priority: Priority::NORMAL,
            lease_duration: LeaseDuration::default_value(),
            max_attempts: MaxAttempts::default_value(),
            retry_backoff: RetryBackoff::default(),
            deduplication: Deduplication::None,
        }
    }
}

impl EnqueueOptions {
    pub fn immediate() -> Self {
        Self::default()
    }

    pub fn at(available_at: DateTime<Utc>) -> Self {
        Self {
            availability: Availability::At(available_at),
            ..Self::default()
        }
    }

    pub fn priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    pub fn lease_duration(mut self, lease_duration: LeaseDuration) -> Self {
        self.lease_duration = lease_duration;
        self
    }

    pub fn max_attempts(mut self, max_attempts: MaxAttempts) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    pub fn retry_backoff(mut self, retry_backoff: RetryBackoff) -> Self {
        self.retry_backoff = retry_backoff;
        self
    }

    pub fn deduplicate(mut self, key: DeduplicationKey) -> Self {
        self.deduplication = Deduplication::Key(key);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnqueuedTask {
    pub task_id: RunId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    Cancelled,
    AlreadyTerminal,
    NotFound,
}

#[derive(diesel::QueryableByName)]
struct EnqueuedRow {
    #[diesel(sql_type = sql_types::Uuid)]
    id: RunId,
}

#[derive(diesel::QueryableByName)]
struct ExistsRow {
    #[diesel(sql_type = sql_types::Bool)]
    exists: bool,
}

const ENQUEUE: &str = r#"
    INSERT INTO scheduler_runs (
        job_id, job_name, job_args, scheduled_for, available_at, priority,
        max_attempts, lease_duration, retry_backoff, deduplication_key
    ) VALUES (
        NULL, $1, $2, COALESCE($3, now()), COALESCE($3, now()), $4, $5, $6, $7, $8
    )
    ON CONFLICT (job_name, deduplication_key) WHERE deduplication_key IS NOT NULL
    DO UPDATE SET deduplication_key = EXCLUDED.deduplication_key
    RETURNING id
"#;

/// Enqueue work using the caller's existing Diesel connection. If that connection
/// is in a transaction, the task commits or rolls back with the caller's data.
pub async fn enqueue<T: Task>(
    conn: &mut AsyncPgConnection,
    args: T::Args,
    options: EnqueueOptions,
) -> Result<EnqueuedTask, SchedulerError> {
    let name = JobName::try_from(T::NAME)?;
    let args = serde_json::to_value(args)?;
    let available_at = match options.availability {
        Availability::Immediate => None,
        Availability::At(at) => Some(at),
    };
    let deduplication_key = match options.deduplication {
        Deduplication::None => None,
        Deduplication::Key(key) => Some(key),
    };

    let row = diesel::sql_query(ENQUEUE)
        .bind::<sql_types::Text, _>(name)
        .bind::<sql_types::Jsonb, _>(args)
        .bind::<sql_types::Nullable<sql_types::Timestamptz>, _>(available_at)
        .bind::<sql_types::SmallInt, _>(options.priority.get())
        .bind::<sql_types::Integer, _>(options.max_attempts.to_i32())
        .bind::<sql_types::Interval, _>(options.lease_duration.to_pg_interval())
        .bind::<sql_types::Interval, _>(options.retry_backoff.to_pg_interval())
        .bind::<sql_types::Nullable<sql_types::Text>, _>(deduplication_key)
        .get_result::<EnqueuedRow>(conn)
        .await?;

    diesel::sql_query("SELECT pg_notify('pg_task_scheduler', '')")
        .execute(conn)
        .await?;
    Ok(EnqueuedTask { task_id: row.id })
}

const CANCEL: &str = r#"
    WITH target AS (
        SELECT id, state, lease_token
        FROM scheduler_runs
        WHERE id = $1
        FOR UPDATE
    ), attempt AS (
        UPDATE scheduler_run_attempts AS a
        SET finished_at = clock_timestamp(),
            outcome = 'cancelled'::scheduler_attempt_outcome,
            error = NULL
        FROM target AS t
        WHERE t.state = 'running'::scheduler_run_state
          AND a.lease_token = t.lease_token
          AND a.outcome IS NULL
        RETURNING a.run_id
    )
    UPDATE scheduler_runs AS r
    SET state = 'cancelled'::scheduler_run_state,
        worker_id = NULL, lease_token = NULL, lease_expires_at = NULL, started_at = NULL,
        finished_at = clock_timestamp(), last_error = NULL, updated_at = clock_timestamp()
    FROM target AS t
    WHERE r.id = t.id
      AND t.state IN ('pending'::scheduler_run_state, 'running'::scheduler_run_state)
      AND (
          t.state = 'pending'::scheduler_run_state
          OR EXISTS (SELECT 1 FROM attempt WHERE attempt.run_id = r.id)
      )
    RETURNING r.id
"#;

/// Cancels pending or running work. A running attempt is closed atomically; its
/// worker is fenced by the task-row state transition.
pub async fn cancel(
    conn: &mut AsyncPgConnection,
    task_id: RunId,
) -> Result<CancelOutcome, SchedulerError> {
    let changed = diesel::sql_query(CANCEL)
        .bind::<sql_types::Uuid, _>(task_id)
        .get_results::<EnqueuedRow>(conn)
        .await?;
    if !changed.is_empty() {
        return Ok(CancelOutcome::Cancelled);
    }
    let exists =
        diesel::sql_query("SELECT EXISTS (SELECT 1 FROM scheduler_runs WHERE id = $1) AS exists")
            .bind::<sql_types::Uuid, _>(task_id)
            .get_result::<ExistsRow>(conn)
            .await?
            .exists;
    Ok(if exists {
        CancelOutcome::AlreadyTerminal
    } else {
        CancelOutcome::NotFound
    })
}

const PRUNE: &str = r#"
    WITH doomed AS (
        SELECT id
        FROM scheduler_runs
        WHERE state IN (
            'completed'::scheduler_run_state,
            'failed'::scheduler_run_state,
            'cancelled'::scheduler_run_state
        )
          AND finished_at < $1
        ORDER BY finished_at, id
        FOR UPDATE SKIP LOCKED
        LIMIT $2
    )
    DELETE FROM scheduler_runs AS r
    USING doomed
    WHERE r.id = doomed.id
"#;

/// Deletes a bounded batch of terminal history older than `before`. Pending and
/// running tasks can never match this operation.
pub async fn prune_terminal(
    conn: &mut AsyncPgConnection,
    before: DateTime<Utc>,
    limit: std::num::NonZeroUsize,
) -> Result<usize, SchedulerError> {
    let limit = i64::try_from(limit.get())
        .map_err(|_| SchedulerError::Config("retention batch is too large".into()))?;
    Ok(diesel::sql_query(PRUNE)
        .bind::<sql_types::Timestamptz, _>(before)
        .bind::<sql_types::BigInt, _>(limit)
        .execute(conn)
        .await?)
}
