//! Everything the health-first overview shows, in one call. Each statement is bounded by
//! `count_cap`, the number of queues or workers, or a rare state, however large the tables grow.
//! Verdicts (healthy, stalled, ...) are the caller's: this only gathers numbers.

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use chrono::{DateTime, Utc};
use diesel::pg::data_types::PgInterval;
use diesel::sql_types;
use diesel_async::{AsyncPgConnection, RunQueryDsl};

use super::{AdminAction, QueuePause, WorkerView, admin_actions, paused_queues, workers};
use crate::error::SchedulerError;
use crate::ids::{QueueName, TaskName};
use crate::models::{interval_to_duration, pg_interval};

#[derive(Debug, Clone)]
pub struct OverviewParams {
    /// How far back the done/dead/failure figures look.
    pub window: Duration,
    /// Rows any one count or window scan reads.
    pub count_cap: i64,
    /// Oldest due rows per queue checked for names no worker handles.
    pub no_handler_probe: i64,
    pub failure_groups: i64,
    pub recent_actions: i64,
}

impl Default for OverviewParams {
    fn default() -> Self {
        Self {
            window: Duration::from_secs(3600),
            count_cap: 10_000,
            no_handler_probe: 100,
            failure_groups: 20,
            recent_actions: 10,
        }
    }
}

/// A count that is exact unless `capped`, when it means "at least `value`".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Count {
    pub value: i64,
    pub capped: bool,
}

#[derive(Debug, Clone)]
pub struct QueueOverview {
    pub queue: QueueName,
    pub queued: Count,
    pub due: Count,
    pub oldest_due_age: Option<Duration>,
    pub running: i64,
    /// Done and dead within the window (see `Overview::done_covered`).
    pub done: Count,
    pub dead: Count,
}

#[derive(Debug, Clone)]
pub struct FailureGroup {
    pub task_name: TaskName,
    pub error: String,
    pub dead: Count,
    pub failed_attempts: Count,
}

/// Due task names in `queue` that no live worker serving it handles.
#[derive(Debug, Clone)]
pub struct NoHandler {
    pub queue: QueueName,
    pub task_names: Vec<TaskName>,
}

#[derive(Debug, Clone)]
pub struct Overview {
    /// The database's `now()`.
    pub generated_at: DateTime<Utc>,
    pub window: Duration,
    /// How far back the done / dead counts really reach: `window`, or less when `count_cap`
    /// cut the scan short.
    pub done_covered: Duration,
    pub dead_covered: Duration,
    pub queues: Vec<QueueOverview>,
    pub workers: Vec<WorkerView>,
    pub paused: Vec<QueuePause>,
    pub failure_groups: Vec<FailureGroup>,
    pub no_handler: Vec<NoHandler>,
    pub recent_actions: Vec<AdminAction>,
}

// Skip-scan: each step jumps to the next distinct queue through a queued-row index that leads
// with `queue` (the planner picks queue_tasks_due_idx, the smaller of it and the claim index).
#[doc(hidden)]
pub const OVERVIEW_QUEUES: &str = "\
    WITH RECURSIVE q AS ( \
        (SELECT queue FROM queue_tasks WHERE status = 'queued' ORDER BY queue LIMIT 1) \
        UNION ALL \
        SELECT (SELECT t.queue FROM queue_tasks t WHERE t.status = 'queued' AND t.queue > q.queue \
                ORDER BY t.queue LIMIT 1) \
        FROM q WHERE q.queue IS NOT NULL) \
    SELECT queue FROM q WHERE queue IS NOT NULL";

// Binds: $1 queue names, $2 count_cap. The oldest due age uses the bare `min(due_at)` of
// `next_due_in` (queue_tasks_due_idx), NULL if the earliest queued task isn't due yet.
#[doc(hidden)]
pub const OVERVIEW_COUNTS: &str = "\
    SELECT u.queue, \
        (SELECT count(*) FROM (SELECT 1 FROM queue_tasks \
            WHERE status = 'queued' AND queue = u.queue LIMIT $2) s) AS queued, \
        (SELECT count(*) FROM (SELECT 1 FROM queue_tasks \
            WHERE status = 'queued' AND queue = u.queue AND due_at <= now() LIMIT $2) s) AS due, \
        CASE WHEN m.due_at <= now() THEN now() - m.due_at END AS oldest_due_age \
    FROM unnest($1::text[]) AS u(queue) \
    CROSS JOIN LATERAL (SELECT min(due_at) AS due_at FROM queue_tasks \
        WHERE status = 'queued' AND queue = u.queue) AS m";

const OVERVIEW_RUNNING: &str =
    "SELECT queue, count(*) AS n FROM queue_tasks WHERE status = 'running' GROUP BY queue";

// Newest `count_cap` finished rows in the window, by queue. Binds: $1 window, $2 count_cap.
macro_rules! window_scan {
    ($status:literal) => {
        concat!(
            "SELECT queue, count(*) AS n, min(completed_at) AS oldest FROM (\
                SELECT queue, completed_at FROM queue_tasks \
                WHERE status = '",
            $status,
            "' AND completed_at > now() - $1 \
                ORDER BY completed_at DESC, id DESC LIMIT $2) s GROUP BY queue"
        )
    };
}
#[doc(hidden)]
pub const OVERVIEW_DONE: &str = window_scan!("done");
#[doc(hidden)]
pub const OVERVIEW_DEAD: &str = window_scan!("dead");

// Binds: $1 window, $2 count_cap.
const DEAD_GROUPS: &str = "\
    SELECT task_name, coalesce(last_error, '') AS error, count(*) AS n FROM (\
        SELECT task_name, last_error FROM queue_tasks \
        WHERE status = 'dead' AND completed_at > now() - $1 \
        ORDER BY completed_at DESC, id DESC LIMIT $2) s GROUP BY 1, 2";

#[doc(hidden)]
pub const OVERVIEW_FAILED_ATTEMPTS: &str = "\
    SELECT t.task_name, coalesce(a.error, '') AS error, count(*) AS n FROM (\
        SELECT task_id, error FROM queue_task_attempts \
        WHERE outcome IN ('retry', 'lease_expired') AND finished_at > now() - $1 \
        ORDER BY finished_at DESC LIMIT $2) a \
    JOIN queue_tasks t ON t.id = a.task_id GROUP BY 1, 2";

// Binds: $1 queue names, $2 probe size: the oldest due rows of each.
const NO_HANDLER: &str = "\
    SELECT u.queue, s.task_name FROM unnest($1::text[]) AS u(queue) \
    CROSS JOIN LATERAL (SELECT DISTINCT task_name FROM (SELECT task_name FROM queue_tasks \
        WHERE status = 'queued' AND queue = u.queue AND due_at <= now() \
        ORDER BY due_at, id LIMIT $2) p) s";

#[derive(diesel::QueryableByName)]
struct NowRow {
    #[diesel(sql_type = sql_types::Timestamptz)]
    now: DateTime<Utc>,
}

#[derive(diesel::QueryableByName)]
struct QueueRow {
    #[diesel(sql_type = sql_types::Text)]
    queue: QueueName,
}

#[derive(diesel::QueryableByName)]
struct CountsRow {
    #[diesel(sql_type = sql_types::Text)]
    queue: QueueName,
    #[diesel(sql_type = sql_types::BigInt)]
    queued: i64,
    #[diesel(sql_type = sql_types::BigInt)]
    due: i64,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Interval>)]
    oldest_due_age: Option<PgInterval>,
}

#[derive(diesel::QueryableByName)]
struct RunningRow {
    #[diesel(sql_type = sql_types::Text)]
    queue: QueueName,
    #[diesel(sql_type = sql_types::BigInt)]
    n: i64,
}

#[derive(diesel::QueryableByName)]
struct WindowRow {
    #[diesel(sql_type = sql_types::Text)]
    queue: QueueName,
    #[diesel(sql_type = sql_types::BigInt)]
    n: i64,
    #[diesel(sql_type = sql_types::Timestamptz)]
    oldest: DateTime<Utc>,
}

#[derive(diesel::QueryableByName)]
struct GroupRow {
    #[diesel(sql_type = sql_types::Text)]
    task_name: TaskName,
    #[diesel(sql_type = sql_types::Text)]
    error: String,
    #[diesel(sql_type = sql_types::BigInt)]
    n: i64,
}

#[derive(diesel::QueryableByName)]
struct NameRow {
    #[diesel(sql_type = sql_types::Text)]
    queue: QueueName,
    #[diesel(sql_type = sql_types::Text)]
    task_name: TaskName,
}

fn names(queues: &BTreeSet<String>) -> Vec<&str> {
    queues.iter().map(String::as_str).collect()
}

/// Finished rows per queue plus how far back the scan really reached.
struct Window {
    per_queue: HashMap<QueueName, i64>,
    capped: bool,
    covered: Duration,
}

async fn window_counts(
    conn: &mut AsyncPgConnection,
    sql: &str,
    p: &OverviewParams,
    now: DateTime<Utc>,
) -> Result<Window, SchedulerError> {
    let rows = diesel::sql_query(sql)
        .bind::<sql_types::Interval, _>(pg_interval(p.window))
        .bind::<sql_types::BigInt, _>(p.count_cap)
        .get_results::<WindowRow>(conn)
        .await?;
    let capped = rows.iter().map(|r| r.n).sum::<i64>() >= p.count_cap;
    let covered = match rows.iter().map(|r| r.oldest).min() {
        Some(oldest) if capped => (now - oldest).to_std().unwrap_or_default().min(p.window),
        _ => p.window,
    };
    Ok(Window {
        per_queue: rows.into_iter().map(|r| (r.queue, r.n)).collect(),
        capped,
        covered,
    })
}

/// Dead tasks and failed attempts by `(task_name, error)`, worst first.
async fn failure_groups(
    conn: &mut AsyncPgConnection,
    p: &OverviewParams,
) -> Result<Vec<FailureGroup>, SchedulerError> {
    let mut merged: HashMap<(TaskName, String), FailureGroup> = HashMap::new();
    for (sql, dead) in [(DEAD_GROUPS, true), (OVERVIEW_FAILED_ATTEMPTS, false)] {
        let rows = diesel::sql_query(sql)
            .bind::<sql_types::Interval, _>(pg_interval(p.window))
            .bind::<sql_types::BigInt, _>(p.count_cap)
            .get_results::<GroupRow>(conn)
            .await?;
        let capped = rows.iter().map(|r| r.n).sum::<i64>() >= p.count_cap;
        for r in rows {
            let zero = Count {
                value: 0,
                capped: false,
            };
            let g = merged
                .entry((r.task_name.clone(), r.error.clone()))
                .or_insert(FailureGroup {
                    task_name: r.task_name,
                    error: r.error,
                    dead: zero,
                    failed_attempts: zero,
                });
            let count = Count { value: r.n, capped };
            if dead {
                g.dead = count;
            } else {
                g.failed_attempts = count;
            }
        }
    }
    let mut groups: Vec<_> = merged.into_values().collect();
    groups.sort_by(|a, b| {
        let total = |g: &FailureGroup| g.dead.value + g.failed_attempts.value;
        total(b)
            .cmp(&total(a))
            .then_with(|| a.task_name.as_str().cmp(b.task_name.as_str()))
            .then_with(|| a.error.cmp(&b.error))
    });
    groups.truncate(p.failure_groups.max(0) as usize);
    Ok(groups)
}

/// Due names no live worker of the queue lists. Queues nobody serves are skipped: the health
/// rule reports those.
async fn no_handler(
    conn: &mut AsyncPgConnection,
    p: &OverviewParams,
    due_queues: &BTreeSet<String>,
    workers: &[WorkerView],
) -> Result<Vec<NoHandler>, SchedulerError> {
    let served: BTreeSet<String> = due_queues
        .iter()
        .filter(|q| {
            workers
                .iter()
                .any(|w| !w.lost && w.queues.iter().any(|wq| wq.name.as_str() == q.as_str()))
        })
        .cloned()
        .collect();
    if served.is_empty() {
        return Ok(Vec::new());
    }
    let rows = diesel::sql_query(NO_HANDLER)
        .bind::<sql_types::Array<sql_types::Text>, _>(names(&served))
        .bind::<sql_types::BigInt, _>(p.no_handler_probe)
        .get_results::<NameRow>(conn)
        .await?;
    let mut out: Vec<NoHandler> = Vec::new();
    for queue in &served {
        let handled: BTreeSet<&str> = workers
            .iter()
            .filter(|w| !w.lost && w.queues.iter().any(|wq| wq.name.as_str() == queue))
            .flat_map(|w| w.task_names.iter().map(String::as_str))
            .collect();
        let mut task_names: Vec<TaskName> = rows
            .iter()
            .filter(|r| r.queue.as_str() == queue && !handled.contains(r.task_name.as_str()))
            .map(|r| r.task_name.clone())
            .collect();
        task_names.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        if let Some(r) = rows.iter().find(|r| r.queue.as_str() == queue)
            && !task_names.is_empty()
        {
            out.push(NoHandler {
                queue: r.queue.clone(),
                task_names,
            });
        }
    }
    Ok(out)
}

pub async fn overview(
    conn: &mut AsyncPgConnection,
    p: &OverviewParams,
) -> Result<Overview, SchedulerError> {
    let generated_at = diesel::sql_query("SELECT now() AS now")
        .get_result::<NowRow>(conn)
        .await?
        .now;
    let workers = workers(conn).await?;
    let paused = paused_queues(conn).await?;

    // Queue set: queued rows (skip-scan), running rows, live workers, paused queues.
    let mut set = BTreeSet::new();
    let queued = diesel::sql_query(OVERVIEW_QUEUES)
        .get_results::<QueueRow>(conn)
        .await?;
    let running = diesel::sql_query(OVERVIEW_RUNNING)
        .get_results::<RunningRow>(conn)
        .await?;
    set.extend(queued.into_iter().map(|r| r.queue.as_str().to_owned()));
    set.extend(running.iter().map(|r| r.queue.as_str().to_owned()));
    for w in workers.iter().filter(|w| !w.lost) {
        set.extend(w.queues.iter().map(|q| q.name.as_str().to_owned()));
    }
    set.extend(paused.iter().map(|q| q.queue.as_str().to_owned()));

    let counts = diesel::sql_query(OVERVIEW_COUNTS)
        .bind::<sql_types::Array<sql_types::Text>, _>(names(&set))
        .bind::<sql_types::BigInt, _>(p.count_cap)
        .get_results::<CountsRow>(conn)
        .await?;
    let done = window_counts(conn, OVERVIEW_DONE, p, generated_at).await?;
    let dead = window_counts(conn, OVERVIEW_DEAD, p, generated_at).await?;

    let cap = |value: i64| Count {
        value,
        capped: value >= p.count_cap,
    };
    let windowed = |w: &Window, q: &QueueName| Count {
        value: w.per_queue.get(q).copied().unwrap_or(0),
        capped: w.capped,
    };
    let running: HashMap<_, _> = running.into_iter().map(|r| (r.queue, r.n)).collect();
    let due_queues = counts
        .iter()
        .filter(|r| r.due > 0)
        .map(|r| r.queue.as_str().to_owned());
    let due_queues: BTreeSet<String> = due_queues.collect();
    let queues = counts
        .into_iter()
        .map(|r| QueueOverview {
            queued: cap(r.queued),
            due: cap(r.due),
            oldest_due_age: r.oldest_due_age.map(interval_to_duration),
            running: running.get(&r.queue).copied().unwrap_or(0),
            done: windowed(&done, &r.queue),
            dead: windowed(&dead, &r.queue),
            queue: r.queue,
        })
        .collect();

    Ok(Overview {
        generated_at,
        window: p.window,
        done_covered: done.covered,
        dead_covered: dead.covered,
        queues,
        failure_groups: failure_groups(conn, p).await?,
        no_handler: no_handler(conn, p, &due_queues, &workers).await?,
        recent_actions: admin_actions(conn, None, p.recent_actions).await?,
        workers,
        paused,
    })
}
