//! The display-only worker registry (`queue_workers`). Claims and ownership never read it.

use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use diesel::pg::data_types::PgInterval;
use diesel::sql_types;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;

use crate::error::SchedulerError;
use crate::ids::QueueName;
use crate::ids::WorkerId;
use crate::models::ParseEnumError;
use crate::models::pg_interval;
use crate::models::text_enum;

/// A registered worker's state, from its health.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerStatus {
    Starting,
    Healthy,
    Degraded,
    Stopping,
}

text_enum!(WorkerStatus, "worker status", {
    Starting => "starting", Healthy => "healthy", Degraded => "degraded", Stopping => "stopping",
});

/// One served queue as a worker reports it.
#[derive(Debug, Clone)]
pub struct QueueSlots {
    pub name: QueueName,
    pub limit: usize,
    pub in_use: usize,
    /// Time since one of this worker's dispatches for the queue last ended, measured locally.
    pub finished_ago: Option<Duration>,
}

/// What a worker writes to its registry row.
#[derive(Debug, Clone)]
pub struct WorkerRegistration {
    pub worker_id: WorkerId,
    pub label: Option<String>,
    pub status: WorkerStatus,
    pub last_error: Option<String>,
    pub queues: Vec<QueueSlots>,
    pub task_names: Vec<String>,
    /// The row reads as lost once this long passes without another write.
    pub lost_after: Duration,
}

// Binds: $1 worker_id, $2 label, $3 status, $4 last_error, $5..$8 the queues' names, limits,
// in_use and finished_ago, $9 task_names, $10 lost_after. Times are PostgreSQL's, so host
// clock skew doesn't matter; `paused` is read here from `queue_pauses`.
const UPSERT: &str = r#"
    INSERT INTO queue_workers
        (worker_id, label, lost_after, status, last_error, queues, task_names)
    SELECT $1, $2, now() + $10, $3, $4,
           coalesce(jsonb_agg(jsonb_build_object(
               'name', q.name, 'limit', q.lim, 'in_use', q.in_use,
               'paused', EXISTS (SELECT 1 FROM queue_pauses p WHERE p.queue = q.name),
               'last_finished_at', now() - q.ago
           ) ORDER BY q.ord), '[]'),
           $9
    FROM unnest($5::text[], $6::bigint[], $7::bigint[], $8::interval[])
        WITH ORDINALITY AS q (name, lim, in_use, ago, ord)
    ON CONFLICT (worker_id) DO UPDATE
    SET label = excluded.label, last_seen_at = now(), lost_after = excluded.lost_after,
        status = excluded.status, last_error = excluded.last_error, queues = excluded.queues,
        task_names = excluded.task_names
"#;

fn big(n: usize) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

/// Insert or refresh this worker's row.
pub async fn upsert_worker(
    conn: &mut AsyncPgConnection,
    reg: &WorkerRegistration,
) -> Result<(), SchedulerError> {
    let q = &reg.queues;
    diesel::sql_query(UPSERT)
        .bind::<sql_types::Text, _>(&reg.worker_id)
        .bind::<sql_types::Nullable<sql_types::Text>, _>(&reg.label)
        .bind::<sql_types::Text, _>(reg.status.as_str())
        .bind::<sql_types::Nullable<sql_types::Text>, _>(&reg.last_error)
        .bind::<sql_types::Array<sql_types::Text>, _>(
            q.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(),
        )
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
        .bind::<sql_types::Array<sql_types::Text>, _>(&reg.task_names)
        .bind::<sql_types::Interval, _>(pg_interval(reg.lost_after))
        .execute(conn)
        .await?;
    Ok(())
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

/// One entry of a worker row's `queues`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct WorkerQueueView {
    pub name: QueueName,
    pub limit: i64,
    pub in_use: i64,
    pub paused: bool,
    pub last_finished_at: Option<DateTime<Utc>>,
}

/// A `queue_workers` row.
#[derive(Debug, Clone)]
pub struct WorkerView {
    pub worker_id: WorkerId,
    pub label: Option<String>,
    pub started_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub status: WorkerStatus,
    pub last_error: Option<String>,
    pub queues: Vec<WorkerQueueView>,
    pub task_names: Vec<String>,
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
    #[diesel(sql_type = sql_types::Jsonb)]
    queues: serde_json::Value,
    #[diesel(sql_type = sql_types::Array<sql_types::Text>)]
    task_names: Vec<String>,
    #[diesel(sql_type = sql_types::Bool)]
    lost: bool,
}

/// Every registered worker: live ones first, then by label (or id when unlabelled).
pub async fn workers(conn: &mut AsyncPgConnection) -> Result<Vec<WorkerView>, SchedulerError> {
    diesel::sql_query(
        "SELECT worker_id, label, started_at, last_seen_at, status, last_error, queues, \
                task_names, lost_after < now() AS lost \
         FROM queue_workers ORDER BY lost, coalesce(label, worker_id), worker_id",
    )
    .get_results::<WorkerRow>(conn)
    .await?
    .into_iter()
    .map(|r| {
        Ok(WorkerView {
            worker_id: r.worker_id,
            label: r.label,
            started_at: r.started_at,
            last_seen_at: r.last_seen_at,
            status: r
                .status
                .parse()
                .map_err(|_: ParseEnumError| SchedulerError::Invariant("unknown worker status"))?,
            last_error: r.last_error,
            queues: serde_json::from_value(r.queues)?,
            task_names: r.task_names,
            lost: r.lost,
        })
    })
    .collect()
}
