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
        WHERE status = 'queued' AND due_at <= now() AND attempts < max_attempts
          AND queue = $5 AND task_name = ANY($1)
          AND NOT EXISTS (SELECT 1 FROM queue_pauses WHERE queue = $5)
        ORDER BY priority DESC, due_at ASC, id ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
    ), claimed AS (
        UPDATE queue_tasks AS q
        SET status = 'running', attempts = q.attempts + 1, locked_by = $3,
            lock_token = q.lock_token + 1,
            locked_until = clock_timestamp() + $4::interval, updated_at = now()
        FROM candidate AS c WHERE q.id = c.id
        RETURNING q.id, q.task_key, q.task_name, q.queue, q.payload::text AS payload, q.attempts, q.max_attempts,
                  q.lock_token, q.locked_until, q.due_at
    ), recorded AS (
        INSERT INTO queue_task_attempts (task_id, attempt_no, lock_token, worker_id)
        SELECT id, attempts, lock_token, $3 FROM claimed
        RETURNING task_id
    )
    SELECT claimed.*, now() AS claimed_at FROM claimed
    WHERE EXISTS (SELECT 1 FROM recorded WHERE recorded.task_id = claimed.id)
    ORDER BY id
"#;

/// Atomically claim up to `limit` due tasks of `queue` whose name is in `task_names`,
/// leasing each for `lease` of PostgreSQL time.
pub async fn claim(
    conn: &mut AsyncPgConnection,
    worker: &WorkerId,
    queue: &QueueName,
    task_names: &[TaskName],
    limit: NonZeroU32,
    lease: LeaseDuration,
) -> Result<Vec<ClaimedTask>, SchedulerError> {
    let rows = diesel::sql_query(CLAIM)
        .bind::<sql_types::Array<sql_types::Text>, _>(task_names)
        .bind::<sql_types::BigInt, _>(i64::from(limit.get()))
        .bind::<sql_types::Text, _>(worker)
        .bind::<sql_types::Interval, _>(pg_interval(lease.get())?)
        .bind::<sql_types::Text, _>(queue)
        .get_results::<ClaimedRow>(conn)
        .await?;
    let mut claimed = Vec::with_capacity(rows.len());
    for r in rows {
        // The claim has already committed, so a row that fails conversion must not fail the call:
        // that would strand the whole batch running until its leases expire. `queue_tasks` has no
        // identifier CHECK and JSONB accepts numbers serde_json rejects, so a raw-SQL producer can
        // store such rows. Dead-letter that one row (by ownership key) and return the rest.
        let (id, lock_token) = (r.id, r.lock_token);
        // Unreachable through the schema (`attempts >= 0` and the claim increments it); kept as a
        // row-local guard.
        let Ok(attempt_no) = AttemptNo::from_db_i32(r.attempts) else {
            dead_letter(
                conn,
                id,
                worker,
                lock_token,
                "invalid attempt_no: stored value is not positive",
            )
            .await;
            continue;
        };
        let task_key = match TaskKey::try_from(r.task_key) {
            Ok(key) => key,
            Err(e) => {
                dead_letter(
                    conn,
                    id,
                    worker,
                    lock_token,
                    &format!("invalid task_key: {e}"),
                )
                .await;
                continue;
            }
        };
        let payload = match serde_json::from_str(&r.payload) {
            Ok(payload) => payload,
            Err(e) => {
                dead_letter(
                    conn,
                    id,
                    worker,
                    lock_token,
                    &format!("invalid payload: {e}"),
                )
                .await;
                continue;
            }
        };
        let max_attempts = match MaxAttempts::from_db_i32(r.max_attempts) {
            Ok(max) => max,
            Err(_) => {
                dead_letter(
                    conn,
                    id,
                    worker,
                    lock_token,
                    "invalid max_attempts: stored value is not positive",
                )
                .await;
                continue;
            }
        };
        let lease = TaskLease {
            task_id: id,
            worker_id: worker.clone(),
            lock_token,
            attempt_no,
        };
        claimed.push(ClaimedTask {
            lease,
            task_key,
            task_name: r.task_name,
            queue: r.queue,
            payload,
            max_attempts,
            due_at: r.due_at,
            claimed_at: r.claimed_at,
            locked_until: r.locked_until,
        });
    }
    Ok(claimed)
}

/// Dead-letter a claimed row that failed conversion. Logs and swallows its own error so the
/// healthy rows of the batch are always returned.
async fn dead_letter(
    conn: &mut AsyncPgConnection,
    task_id: TaskId,
    worker: &WorkerId,
    lock_token: LockToken,
    reason: &str,
) {
    tracing::error!(task_id = task_id.0, %reason, "claimed task is unusable; marking it dead");
    if let Err(e) = fail_owned(conn, task_id, worker, lock_token, reason).await {
        tracing::error!(task_id = task_id.0, error = %e, "could not dead-letter the task; skipping it");
    }
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
