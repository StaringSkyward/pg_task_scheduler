//! The display-only worker registry (`queue_workers`). Claims and ownership never read it.

use std::collections::HashMap;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use diesel::pg::data_types::PgInterval;
use diesel::sql_types;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;

use crate::error::SchedulerError;
use crate::ids::QueueName;
use crate::ids::TaskName;
use crate::ids::WorkerId;
use crate::models::ParseEnumError;
use crate::models::pg_interval;
use crate::models::text_enum;

/// A registered worker's state, from its health.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerStatus {
    /// Running, with no claim cycle finished yet.
    Starting,
    /// Its last claim cycle succeeded.
    Healthy,
    /// Its last claim cycle failed.
    Degraded,
    /// Shutting down; it claims nothing more.
    Stopping,
}

text_enum!(WorkerStatus, "worker status", {
    Starting => "starting", Healthy => "healthy", Degraded => "degraded", Stopping => "stopping",
});

/// One served queue as a worker reports it.
#[derive(Debug, Clone)]
pub struct QueueSlots {
    /// The queue.
    pub name: QueueName,
    /// Most handlers the worker runs at once for it.
    pub limit: usize,
    /// Handlers running now.
    pub in_use: usize,
    /// Time since one of this worker's dispatches for the queue last ended, measured locally.
    pub finished_ago: Option<Duration>,
}

/// What a worker writes to its registry row.
#[derive(Debug, Clone)]
pub struct WorkerRegistration {
    /// The worker's id.
    pub worker_id: WorkerId,
    /// Display name; the worker id is shown when `None`.
    pub label: Option<String>,
    /// Its state.
    pub status: WorkerStatus,
    /// Its latest health error.
    pub last_error: Option<String>,
    /// The queues it serves.
    pub queues: Vec<QueueSlots>,
    /// The task names it has handlers for.
    pub task_names: Vec<TaskName>,
    /// The row reads as lost once this long passes without another write.
    pub lost_after: Duration,
}

const UPSERT_WORKER: &str = r#"
    INSERT INTO queue_workers (worker_id, label, lost_after, status, last_error)
    VALUES ($1, $2, now() + $3, $4, $5)
    ON CONFLICT (worker_id) DO UPDATE
    SET label = excluded.label, last_seen_at = now(), lost_after = excluded.lost_after,
        status = excluded.status, last_error = excluded.last_error
"#;

const DROP_QUEUES: &str =
    "DELETE FROM queue_worker_queues WHERE worker_id = $1 AND queue <> ALL($2)";

// Binds: $1 worker_id, $2..$5 the queues' names, limits, in_use and finished_ago. Times are
// PostgreSQL's, so host clock skew doesn't matter.
const UPSERT_QUEUES: &str = r#"
    INSERT INTO queue_worker_queues (worker_id, queue, slot_limit, in_use, last_finished_at)
    SELECT $1, name, lim, in_use, now() - ago
    FROM unnest($2::text[], $3::bigint[], $4::bigint[], $5::interval[])
        AS q (name, lim, in_use, ago)
    ON CONFLICT (worker_id, queue) DO UPDATE
    SET slot_limit = excluded.slot_limit, in_use = excluded.in_use,
        last_finished_at = excluded.last_finished_at
"#;

const DROP_TASK_NAMES: &str =
    "DELETE FROM queue_worker_task_names WHERE worker_id = $1 AND task_name <> ALL($2)";

const INSERT_TASK_NAMES: &str = "INSERT INTO queue_worker_task_names (worker_id, task_name) \
     SELECT $1, unnest($2::text[]) ON CONFLICT DO NOTHING";

fn big(n: usize) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

/// Insert or refresh this worker's row, its queue rows and its task-name rows, in one
/// transaction; queues and names it no longer reports are deleted.
pub async fn upsert_worker(
    conn: &mut AsyncPgConnection,
    reg: &WorkerRegistration,
) -> Result<(), SchedulerError> {
    let q = &reg.queues;
    let queue_names: Vec<&str> = q.iter().map(|s| s.name.as_str()).collect();
    let task_names: Vec<&str> = reg.task_names.iter().map(TaskName::as_str).collect();
    conn.transaction::<(), SchedulerError, _>(async |conn| {
        diesel::sql_query(UPSERT_WORKER)
            .bind::<sql_types::Text, _>(&reg.worker_id)
            .bind::<sql_types::Nullable<sql_types::Text>, _>(&reg.label)
            .bind::<sql_types::Interval, _>(pg_interval(reg.lost_after))
            .bind::<sql_types::Text, _>(reg.status.as_str())
            .bind::<sql_types::Nullable<sql_types::Text>, _>(&reg.last_error)
            .execute(conn)
            .await?;
        diesel::sql_query(DROP_QUEUES)
            .bind::<sql_types::Text, _>(&reg.worker_id)
            .bind::<sql_types::Array<sql_types::Text>, _>(&queue_names)
            .execute(conn)
            .await?;
        diesel::sql_query(UPSERT_QUEUES)
            .bind::<sql_types::Text, _>(&reg.worker_id)
            .bind::<sql_types::Array<sql_types::Text>, _>(&queue_names)
            .bind::<sql_types::Array<sql_types::BigInt>, _>(
                q.iter().map(|s| big(s.limit)).collect::<Vec<_>>(),
            )
            .bind::<sql_types::Array<sql_types::BigInt>, _>(
                q.iter().map(|s| big(s.in_use)).collect::<Vec<_>>(),
            )
            .bind::<sql_types::Array<sql_types::Nullable<sql_types::Interval>>, _>(
                q.iter()
                    .map(|s| s.finished_ago.map(pg_interval))
                    .collect::<Vec<Option<PgInterval>>>(),
            )
            .execute(conn)
            .await?;
        diesel::sql_query(DROP_TASK_NAMES)
            .bind::<sql_types::Text, _>(&reg.worker_id)
            .bind::<sql_types::Array<sql_types::Text>, _>(&task_names)
            .execute(conn)
            .await?;
        diesel::sql_query(INSERT_TASK_NAMES)
            .bind::<sql_types::Text, _>(&reg.worker_id)
            .bind::<sql_types::Array<sql_types::Text>, _>(&task_names)
            .execute(conn)
            .await?;
        Ok(())
    })
    .await
}

/// Delete a worker's row (a clean shutdown).
pub async fn remove_worker(
    conn: &mut AsyncPgConnection,
    id: &WorkerId,
) -> Result<(), SchedulerError> {
    diesel::sql_query("DELETE FROM queue_workers WHERE worker_id = $1")
        .bind::<sql_types::Text, _>(id)
        .execute(conn)
        .await?;
    Ok(())
}

/// Delete rows not written for 24 h; returns how many.
pub async fn prune_workers(conn: &mut AsyncPgConnection) -> Result<usize, SchedulerError> {
    Ok(diesel::sql_query(
        "DELETE FROM queue_workers WHERE last_seen_at < now() - interval '24 hours'",
    )
    .execute(conn)
    .await?)
}

/// One queue a worker serves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerQueueView {
    /// The queue.
    pub name: QueueName,
    /// Most handlers the worker runs at once for it.
    pub limit: i64,
    /// Handlers running at the last write.
    pub in_use: i64,
    /// Whether the queue is paused now (read from `queue_pauses` when listed).
    pub paused: bool,
    /// When one of the worker's dispatches for the queue last ended.
    pub last_finished_at: Option<DateTime<Utc>>,
}

/// A `queue_workers` row.
#[derive(Debug, Clone)]
pub struct WorkerView {
    /// The worker's id.
    pub worker_id: WorkerId,
    /// Display name; show the worker id when `None`.
    pub label: Option<String>,
    /// When the row was first written.
    pub started_at: DateTime<Utc>,
    /// When the row was last written.
    pub last_seen_at: DateTime<Utc>,
    /// The worker's state at the last write.
    pub status: WorkerStatus,
    /// Its latest health error at the last write.
    pub last_error: Option<String>,
    /// The queues it serves.
    pub queues: Vec<WorkerQueueView>,
    /// The task names it has handlers for.
    pub task_names: Vec<TaskName>,
    /// Not written within 3 × its `registry_interval`: it never counts as serving a queue.
    pub lost: bool,
}

#[derive(diesel::QueryableByName)]
struct WorkerRow {
    #[diesel(sql_type = sql_types::Text)]
    worker_id: WorkerId,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Text>)]
    label: Option<String>,
    #[diesel(sql_type = sql_types::Timestamptz)]
    started_at: DateTime<Utc>,
    #[diesel(sql_type = sql_types::Timestamptz)]
    last_seen_at: DateTime<Utc>,
    #[diesel(sql_type = sql_types::Text)]
    status: String,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Text>)]
    last_error: Option<String>,
    #[diesel(sql_type = sql_types::Bool)]
    lost: bool,
}

#[derive(diesel::QueryableByName)]
struct QueueRow {
    #[diesel(sql_type = sql_types::Text)]
    worker_id: WorkerId,
    #[diesel(sql_type = sql_types::Text)]
    queue: QueueName,
    #[diesel(sql_type = sql_types::BigInt)]
    slot_limit: i64,
    #[diesel(sql_type = sql_types::BigInt)]
    in_use: i64,
    #[diesel(sql_type = sql_types::Bool)]
    paused: bool,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Timestamptz>)]
    last_finished_at: Option<DateTime<Utc>>,
}

#[derive(diesel::QueryableByName)]
struct TaskNameRow {
    #[diesel(sql_type = sql_types::Text)]
    worker_id: WorkerId,
    #[diesel(sql_type = sql_types::Text)]
    task_name: TaskName,
}

/// Every registered worker: live ones first, then by label (or id when unlabelled). Each
/// worker's queues are ordered by queue name; `paused` is read live.
pub async fn workers(conn: &mut AsyncPgConnection) -> Result<Vec<WorkerView>, SchedulerError> {
    let rows = diesel::sql_query(
        "SELECT worker_id, label, started_at, last_seen_at, status, last_error, \
                lost_after < now() AS lost \
         FROM queue_workers ORDER BY lost, coalesce(label, worker_id), worker_id",
    )
    .get_results::<WorkerRow>(conn)
    .await?;
    let queue_rows = diesel::sql_query(
        "SELECT q.worker_id, q.queue, q.slot_limit, q.in_use, q.last_finished_at, \
                EXISTS (SELECT 1 FROM queue_pauses p WHERE p.queue = q.queue) AS paused \
         FROM queue_worker_queues q ORDER BY q.worker_id, q.queue",
    )
    .get_results::<QueueRow>(conn)
    .await?;
    let name_rows = diesel::sql_query(
        "SELECT worker_id, task_name FROM queue_worker_task_names ORDER BY worker_id, task_name",
    )
    .get_results::<TaskNameRow>(conn)
    .await?;
    let mut queues: HashMap<WorkerId, Vec<WorkerQueueView>> = HashMap::new();
    for r in queue_rows {
        queues
            .entry(r.worker_id)
            .or_default()
            .push(WorkerQueueView {
                name: r.queue,
                limit: r.slot_limit,
                in_use: r.in_use,
                paused: r.paused,
                last_finished_at: r.last_finished_at,
            });
    }
    let mut names: HashMap<WorkerId, Vec<TaskName>> = HashMap::new();
    for r in name_rows {
        names.entry(r.worker_id).or_default().push(r.task_name);
    }
    rows.into_iter()
        .map(|r| {
            Ok(WorkerView {
                queues: queues.remove(&r.worker_id).unwrap_or_default(),
                task_names: names.remove(&r.worker_id).unwrap_or_default(),
                worker_id: r.worker_id,
                label: r.label,
                started_at: r.started_at,
                last_seen_at: r.last_seen_at,
                status: r.status.parse().map_err(|_: ParseEnumError| {
                    SchedulerError::Invariant("unknown worker status")
                })?,
                last_error: r.last_error,
                lost: r.lost,
            })
        })
        .collect()
}
