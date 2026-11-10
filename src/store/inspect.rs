use chrono::{DateTime, Utc};
use diesel::pg::data_types::PgInterval;
use diesel::sql_types;
use diesel_async::{AsyncPgConnection, RunQueryDsl};

use crate::error::SchedulerError;
use crate::ids::{LockToken, TaskId, TaskKey, TaskName, WorkerId};
use crate::models::{Attempt, QueueDepth, QueueStats, TaskView, interval_to_duration};

#[derive(diesel::QueryableByName)]
struct TaskRow {
    #[diesel(sql_type = sql_types::BigInt)]
    id: TaskId,
    #[diesel(sql_type = sql_types::Text)]
    task_key: TaskKey,
    #[diesel(sql_type = sql_types::Text)]
    task_name: TaskName,
    #[diesel(sql_type = sql_types::Jsonb)]
    payload: serde_json::Value,
    #[diesel(sql_type = sql_types::Text)]
    status: String,
    #[diesel(sql_type = sql_types::Integer)]
    priority: i32,
    #[diesel(sql_type = sql_types::Timestamptz)]
    due_at: DateTime<Utc>,
    #[diesel(sql_type = sql_types::Integer)]
    attempts: i32,
    #[diesel(sql_type = sql_types::Integer)]
    max_attempts: i32,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Text>)]
    locked_by: Option<WorkerId>,
    #[diesel(sql_type = sql_types::BigInt)]
    lock_token: LockToken,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Timestamptz>)]
    locked_until: Option<DateTime<Utc>>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Text>)]
    last_error: Option<String>,
    #[diesel(sql_type = sql_types::Timestamptz)]
    created_at: DateTime<Utc>,
    #[diesel(sql_type = sql_types::Timestamptz)]
    updated_at: DateTime<Utc>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Timestamptz>)]
    completed_at: Option<DateTime<Utc>>,
}

#[derive(diesel::QueryableByName)]
struct AttemptRow {
    #[diesel(sql_type = sql_types::Integer)]
    attempt_no: i32,
    #[diesel(sql_type = sql_types::BigInt)]
    lock_token: LockToken,
    #[diesel(sql_type = sql_types::Text)]
    worker_id: WorkerId,
    #[diesel(sql_type = sql_types::Timestamptz)]
    started_at: DateTime<Utc>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Timestamptz>)]
    finished_at: Option<DateTime<Utc>>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Text>)]
    outcome: Option<String>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Text>)]
    error: Option<String>,
}

/// Read one task, or `None` if it does not exist.
pub async fn task(
    conn: &mut AsyncPgConnection,
    id: TaskId,
) -> Result<Option<TaskView>, SchedulerError> {
    let row = diesel::sql_query(
        "SELECT id, task_key, task_name, payload, status, priority, due_at, attempts, \
         max_attempts, locked_by, lock_token, locked_until, last_error, created_at, \
         updated_at, completed_at FROM queue_tasks WHERE id = $1",
    )
    .bind::<sql_types::BigInt, _>(id)
    .get_results::<TaskRow>(conn)
    .await?
    .pop();
    row.map(|r| {
        Ok(TaskView {
            id: r.id,
            task_key: r.task_key,
            task_name: r.task_name,
            payload: r.payload,
            status: r
                .status
                .parse()
                .map_err(|_| SchedulerError::Invariant("unknown task status"))?,
            priority: r.priority,
            due_at: r.due_at,
            attempts: r.attempts,
            max_attempts: r.max_attempts,
            locked_by: r.locked_by,
            lock_token: r.lock_token,
            locked_until: r.locked_until,
            last_error: r.last_error,
            created_at: r.created_at,
            updated_at: r.updated_at,
            completed_at: r.completed_at,
        })
    })
    .transpose()
}

/// Attempt history of a task, oldest claim first.
pub async fn attempts(
    conn: &mut AsyncPgConnection,
    id: TaskId,
) -> Result<Vec<Attempt>, SchedulerError> {
    diesel::sql_query(
        "SELECT attempt_no, lock_token, worker_id, started_at, finished_at, outcome, error \
         FROM queue_task_attempts WHERE task_id = $1 ORDER BY lock_token",
    )
    .bind::<sql_types::BigInt, _>(id)
    .get_results::<AttemptRow>(conn)
    .await?
    .into_iter()
    .map(|r| {
        Ok(Attempt {
            attempt_no: r.attempt_no,
            lock_token: r.lock_token,
            worker_id: r.worker_id,
            started_at: r.started_at,
            finished_at: r.finished_at,
            outcome: r
                .outcome
                .map(|o| o.parse())
                .transpose()
                .map_err(|_| SchedulerError::Invariant("unknown attempt outcome"))?,
            error: r.error,
        })
    })
    .collect()
}

#[derive(diesel::QueryableByName)]
struct CountsRow {
    #[diesel(sql_type = sql_types::BigInt)]
    queued: i64,
    #[diesel(sql_type = sql_types::BigInt)]
    running: i64,
    #[diesel(sql_type = sql_types::BigInt)]
    done: i64,
    #[diesel(sql_type = sql_types::BigInt)]
    dead: i64,
    #[diesel(sql_type = sql_types::BigInt)]
    due: i64,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Interval>)]
    oldest_due_age: Option<PgInterval>,
}

#[derive(diesel::QueryableByName)]
struct DepthRow {
    #[diesel(sql_type = sql_types::Text)]
    task_name: TaskName,
    #[diesel(sql_type = sql_types::Integer)]
    priority: i32,
    #[diesel(sql_type = sql_types::BigInt)]
    queued: i64,
}

#[derive(diesel::QueryableByName)]
struct OutboxStatsRow {
    #[diesel(sql_type = sql_types::BigInt)]
    unpublished: i64,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Interval>)]
    oldest_age: Option<PgInterval>,
}

/// Queue counts, per-name depth and outbox backlog.
pub async fn queue_stats(conn: &mut AsyncPgConnection) -> Result<QueueStats, SchedulerError> {
    // ponytail: full scan of retained rows; keep a counter table if this gets slow
    let c = diesel::sql_query(
        "SELECT count(*) FILTER (WHERE status = 'queued') AS queued, \
                count(*) FILTER (WHERE status = 'running') AS running, \
                count(*) FILTER (WHERE status = 'done') AS done, \
                count(*) FILTER (WHERE status = 'dead') AS dead, \
                count(*) FILTER (WHERE status = 'queued' AND due_at <= now()) AS due, \
                now() - min(due_at) FILTER (WHERE status = 'queued' AND due_at <= now()) \
                    AS oldest_due_age \
         FROM queue_tasks",
    )
    .get_result::<CountsRow>(conn)
    .await?;
    let depth = diesel::sql_query(
        "SELECT task_name, priority, count(*) AS queued FROM queue_tasks \
         WHERE status = 'queued' GROUP BY task_name, priority ORDER BY task_name, priority DESC",
    )
    .get_results::<DepthRow>(conn)
    .await?;
    let o = diesel::sql_query(
        "SELECT count(*) AS unpublished, now() - min(created_at) AS oldest_age \
         FROM queue_outbox WHERE published_at IS NULL",
    )
    .get_result::<OutboxStatsRow>(conn)
    .await?;
    Ok(QueueStats {
        queued: c.queued,
        running: c.running,
        done: c.done,
        dead: c.dead,
        due: c.due,
        oldest_due_age: c.oldest_due_age.map(interval_to_duration),
        depth: depth
            .into_iter()
            .map(|r| QueueDepth {
                task_name: r.task_name,
                priority: r.priority,
                queued: r.queued,
            })
            .collect(),
        outbox_unpublished: o.unpublished,
        oldest_unpublished_age: o.oldest_age.map(interval_to_duration),
    })
}
