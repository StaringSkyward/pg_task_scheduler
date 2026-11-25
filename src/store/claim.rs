use std::num::NonZeroU32;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use diesel::sql_types;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;

use crate::error::SchedulerError;
use crate::ids::LockToken;
use crate::ids::QueueName;
use crate::ids::TaskId;
use crate::ids::TaskKey;
use crate::ids::TaskName;
use crate::ids::WorkerId;
use crate::models::AttemptNo;
use crate::models::ClaimedTask;
use crate::models::LeaseDuration;
use crate::models::MaxAttempts;
use crate::models::TaskLease;
use crate::models::interval_to_duration;
use crate::models::pg_interval;
use crate::store::transitions::fail_owned;

#[derive(diesel::QueryableByName)]
struct ClaimedRow {
    #[diesel(sql_type = sql_types::BigInt)]
    id: TaskId,
    /// Raw: see [`claim`] for why a bad key must not fail the decode.
    #[diesel(sql_type = sql_types::Text)]
    task_key: String,
    #[diesel(sql_type = sql_types::Text)]
    task_name: TaskName,
    #[diesel(sql_type = sql_types::Text)]
    queue: QueueName,
    /// Raw text, parsed per row: see [`claim`].
    #[diesel(sql_type = sql_types::Text)]
    payload: String,
    #[diesel(sql_type = sql_types::Integer)]
    attempts: i32,
    #[diesel(sql_type = sql_types::Integer)]
    max_attempts: i32,
    #[diesel(sql_type = sql_types::BigInt)]
    lock_token: LockToken,
    #[diesel(sql_type = sql_types::Timestamptz)]
    locked_until: DateTime<Utc>,
    #[diesel(sql_type = sql_types::Timestamptz)]
    due_at: DateTime<Utc>,
    #[diesel(sql_type = sql_types::Timestamptz)]
    claimed_at: DateTime<Utc>,
}

// The claim and its attempt rows commit together (one statement).
const CLAIM: &str = r#"
    WITH candidate AS (
        SELECT id FROM queue_tasks
        WHERE status = 'queued' AND due_at <= statement_timestamp() AND attempts < max_attempts
          AND queue = $5 AND task_name = ANY($1)
          AND NOT EXISTS (SELECT 1 FROM queue_pauses WHERE queue = $5)
        ORDER BY priority DESC, due_at ASC, id ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
    ), claimed AS (
        UPDATE queue_tasks AS q
        SET status = 'running', attempts = q.attempts + 1, locked_by = $3,
            lock_token = q.lock_token + 1,
            locked_until = clock_timestamp() + $4::interval, updated_at = statement_timestamp()
        FROM candidate AS c WHERE q.id = c.id
        RETURNING q.id, q.task_key, q.task_name, q.queue, q.payload::text AS payload, q.attempts, q.max_attempts,
                  q.lock_token, q.locked_until, q.due_at
    ), recorded AS (
        INSERT INTO queue_task_attempts (task_id, attempt_no, lock_token, worker_id)
        SELECT id, attempts, lock_token, $3 FROM claimed
        RETURNING task_id
    )
    SELECT claimed.*, statement_timestamp() AS claimed_at FROM claimed
    WHERE EXISTS (SELECT 1 FROM recorded WHERE recorded.task_id = claimed.id)
    ORDER BY id
"#;

/// What one [`claim`] leased: the rows ready to run, and the rows it could not convert.
#[derive(Debug, Clone)]
pub struct ClaimBatch {
    /// Leased rows that converted cleanly, ready for their handlers.
    pub ready: Vec<ClaimedTask>,
    /// Leased rows that failed conversion; [`dead_letter`] each one.
    pub poisoned: Vec<PoisonedClaim>,
}

/// A row [`claim`] leased but could not turn into a [`ClaimedTask`]: a bad `task_key`, a
/// payload `serde_json` rejects, or a stored count that is not positive. It is still
/// `running` under this lease.
#[derive(Debug, Clone)]
pub struct PoisonedClaim {
    /// The leased task.
    pub task_id: TaskId,
    /// The worker that holds the lease.
    pub worker_id: WorkerId,
    /// The lease's token.
    pub lock_token: LockToken,
    /// Why the row cannot run, recorded as its error when dead-lettered.
    pub reason: String,
}

/// Atomically claim up to `limit` due tasks of `queue` whose name is in `task_names`,
/// leasing each for `lease` of PostgreSQL time.
///
/// Rows that fail conversion come back in [`ClaimBatch::poisoned`], still leased and
/// untouched, so the healthy rows are never held up behind them. Callers of `claim` itself
/// should [`dead_letter`] each one after dispatching the ready rows; one they leave stays
/// `running` until its lease expires, the reaper requeues it, and a later claim poisons it
/// again until its attempts run out. [`Scheduler`](crate::Scheduler) dead-letters them for you, one batch at a time; rows
/// claimed while it is busy are left to the reaper.
pub async fn claim(
    conn: &mut AsyncPgConnection,
    worker: &WorkerId,
    queue: &QueueName,
    task_names: &[TaskName],
    limit: NonZeroU32,
    lease: LeaseDuration,
) -> Result<ClaimBatch, SchedulerError> {
    let rows = diesel::sql_query(CLAIM)
        .bind::<sql_types::Array<sql_types::Text>, _>(task_names)
        .bind::<sql_types::BigInt, _>(i64::from(limit.get()))
        .bind::<sql_types::Text, _>(worker)
        .bind::<sql_types::Interval, _>(pg_interval(lease.get())?)
        .bind::<sql_types::Text, _>(queue)
        .get_results::<ClaimedRow>(conn)
        .await?;
    let mut batch = ClaimBatch {
        ready: Vec::with_capacity(rows.len()),
        poisoned: Vec::new(),
    };
    for r in rows {
        // The claim has already committed, so a row that fails conversion must not fail the call:
        // that would strand the whole batch running until its leases expire. JSONB accepts numbers
        // serde_json rejects, so a raw-SQL producer can store such rows; a bad task_key needs the
        // identifier CHECK dropped by hand. Return that one row as poisoned, and the rest as ready.
        match convert(r, worker) {
            Ok(task) => batch.ready.push(task),
            Err(poisoned) => batch.poisoned.push(poisoned),
        }
    }
    Ok(batch)
}

fn convert(r: ClaimedRow, worker: &WorkerId) -> Result<ClaimedTask, PoisonedClaim> {
    let poisoned = |reason: String| PoisonedClaim {
        task_id: r.id,
        worker_id: worker.clone(),
        lock_token: r.lock_token,
        reason,
    };
    // Unreachable through the schema (`attempts >= 0` and the claim increments it); kept as a
    // row-local guard.
    let attempt_no = AttemptNo::from_db_i32(r.attempts)
        .map_err(|_| poisoned("invalid attempt_no: stored value is not positive".into()))?;
    let task_key =
        TaskKey::try_from(r.task_key).map_err(|e| poisoned(format!("invalid task_key: {e}")))?;
    let payload =
        serde_json::from_str(&r.payload).map_err(|e| poisoned(format!("invalid payload: {e}")))?;
    let max_attempts = MaxAttempts::from_db_i32(r.max_attempts)
        .map_err(|_| poisoned("invalid max_attempts: stored value is not positive".into()))?;
    Ok(ClaimedTask {
        lease: TaskLease {
            task_id: r.id,
            worker_id: worker.clone(),
            lock_token: r.lock_token,
            attempt_no,
        },
        task_key,
        task_name: r.task_name,
        queue: r.queue,
        payload,
        max_attempts,
        due_at: r.due_at,
        claimed_at: r.claimed_at,
        locked_until: r.locked_until,
    })
}

/// Fail a [`PoisonedClaim`] permanently (`dead`), with its reason as the error, while its
/// lease still holds. [`SchedulerError::LostOwnership`] once the lease has expired or moved on.
///
#[doc = bounded_doc!()]
pub async fn dead_letter(
    conn: &mut AsyncPgConnection,
    claim: &PoisonedClaim,
) -> Result<(), SchedulerError> {
    fail_owned(
        conn,
        claim.task_id,
        &claim.worker_id,
        claim.lock_token,
        &claim.reason,
    )
    .await
}

#[derive(diesel::QueryableByName)]
struct WaitRow {
    #[diesel(sql_type = sql_types::Nullable<sql_types::Interval>)]
    wait: Option<diesel::pg::data_types::PgInterval>,
}

// Bare `min(due_at)` per queue lets PostgreSQL use queue_tasks_due_idx (an expression inside min()
// would scan every queued row, and `queue = ANY(..)` would scan every row of those queues). NULL on
// no rows is preserved; `interval_to_duration` clamps overdue to zero.
/// Time until the earliest queued task in `queues` for `task_names` is due (zero if
/// one already is), or `None` if there is none.
pub async fn next_due_in(
    conn: &mut AsyncPgConnection,
    queues: &[QueueName],
    task_names: &[TaskName],
) -> Result<Option<Duration>, SchedulerError> {
    let row = diesel::sql_query(
        "SELECT min(next.due_at) - now() AS wait \
         FROM (SELECT queue FROM unnest($1::text[]) AS u(queue) \
               WHERE NOT EXISTS (SELECT 1 FROM queue_pauses p WHERE p.queue = u.queue)) AS q \
         CROSS JOIN LATERAL ( \
             SELECT min(due_at) AS due_at FROM queue_tasks \
             WHERE status = 'queued' AND queue = q.queue \
               AND attempts < max_attempts AND task_name = ANY($2) \
         ) AS next",
    )
    .bind::<sql_types::Array<sql_types::Text>, _>(
        queues.iter().map(QueueName::as_str).collect::<Vec<_>>(),
    )
    .bind::<sql_types::Array<sql_types::Text>, _>(task_names)
    .get_result::<WaitRow>(conn)
    .await?;
    Ok(row.wait.map(interval_to_duration))
}
