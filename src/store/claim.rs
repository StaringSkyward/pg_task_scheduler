use std::num::NonZeroUsize;
use std::time::Duration;

use chrono::{DateTime, Utc};
use diesel::sql_types;
use diesel_async::{AsyncPgConnection, RunQueryDsl};

use crate::error::SchedulerError;
use crate::ids::{LockToken, QueueName, TaskId, TaskKey, TaskName, WorkerId};
use crate::models::{ClaimedTask, TaskLease, interval_to_duration, pg_interval};

#[derive(diesel::QueryableByName)]
struct ClaimedRow {
    #[diesel(sql_type = sql_types::BigInt)]
    id: TaskId,
    #[diesel(sql_type = sql_types::Text)]
    task_key: TaskKey,
    #[diesel(sql_type = sql_types::Text)]
    task_name: TaskName,
    #[diesel(sql_type = sql_types::Text)]
    queue: QueueName,
    #[diesel(sql_type = sql_types::Jsonb)]
    payload: serde_json::Value,
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
        RETURNING q.id, q.task_key, q.task_name, q.queue, q.payload, q.attempts, q.max_attempts,
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
    task_names: &[String],
    limit: NonZeroUsize,
    lease: Duration,
) -> Result<Vec<ClaimedTask>, SchedulerError> {
    let limit = i64::try_from(limit.get()).unwrap_or(i64::MAX);
    let rows = diesel::sql_query(CLAIM)
        .bind::<sql_types::Array<sql_types::Text>, _>(task_names)
        .bind::<sql_types::BigInt, _>(limit)
        .bind::<sql_types::Text, _>(worker)
        .bind::<sql_types::Interval, _>(pg_interval(lease))
        .bind::<sql_types::Text, _>(queue)
        .get_results::<ClaimedRow>(conn)
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| ClaimedTask {
            lease: TaskLease {
                task_id: r.id,
                worker_id: worker.clone(),
                lock_token: r.lock_token,
                attempt_no: r.attempts,
            },
            task_key: r.task_key,
            task_name: r.task_name,
            queue: r.queue,
            payload: r.payload,
            max_attempts: r.max_attempts,
            due_at: r.due_at,
            claimed_at: r.claimed_at,
            locked_until: r.locked_until,
        })
        .collect())
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
    task_names: &[String],
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
