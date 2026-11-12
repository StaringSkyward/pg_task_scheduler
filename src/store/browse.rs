//! Bounded, keyset-paged task lists and task detail for the admin pages. Every list reads at
//! most `limit + 1` rows through an index, however large `queue_tasks` grows.

use chrono::{DateTime, TimeZone, Utc};
use diesel::sql_types;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde::{Deserialize, Serialize};

use super::{AdminAction, attempts, task, task_actions};
use crate::error::SchedulerError;
use crate::ids::{QueueName, TaskId, TaskName, WorkerId};
use crate::models::{Attempt, TaskStatus, TaskView};

/// One row of a task list.
#[derive(Debug, Clone)]
pub struct TaskRow {
    pub id: TaskId,
    pub task_name: TaskName,
    pub queue: QueueName,
    pub status: TaskStatus,
    pub priority: i32,
    pub due_at: DateTime<Utc>,
    pub attempts: i32,
    pub max_attempts: i32,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub locked_by: Option<WorkerId>,
    pub locked_until: Option<DateTime<Utc>>,
    /// From the attempt whose `lock_token` is the task's current one.
    pub attempt_started_at: Option<DateTime<Utc>>,
    pub attempt_finished_at: Option<DateTime<Utc>>,
}

/// One page and, if more rows follow, the cursor for the next.
#[derive(Debug, Clone)]
pub struct Page<T, C> {
    pub items: Vec<T>,
    pub next: Option<C>,
}

/// Resume point in the queued list: due tasks in claim order, then not-yet-due tasks.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueuedCursor {
    /// Resume in the due segment (else the not-yet-due one).
    pub due: bool,
    pub priority: i32,
    pub due_at: DateTime<Utc>,
    pub id: i64,
    /// Rows before the next page.
    pub position: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunningCursor {
    pub locked_until: DateTime<Utc>,
    pub id: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FinishedCursor {
    pub completed_at: DateTime<Utc>,
    pub id: i64,
}

#[derive(diesel::QueryableByName)]
struct Row {
    #[diesel(sql_type = sql_types::BigInt)]
    id: TaskId,
    #[diesel(sql_type = sql_types::Text)]
    task_name: TaskName,
    #[diesel(sql_type = sql_types::Text)]
    queue: QueueName,
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
    last_error: Option<String>,
    #[diesel(sql_type = sql_types::Timestamptz)]
    created_at: DateTime<Utc>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Timestamptz>)]
    completed_at: Option<DateTime<Utc>>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Text>)]
    locked_by: Option<WorkerId>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Timestamptz>)]
    locked_until: Option<DateTime<Utc>>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Timestamptz>)]
    attempt_started_at: Option<DateTime<Utc>>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Timestamptz>)]
    attempt_finished_at: Option<DateTime<Utc>>,
}

impl TryFrom<Row> for TaskRow {
    type Error = SchedulerError;
    fn try_from(r: Row) -> Result<Self, SchedulerError> {
        Ok(TaskRow {
            id: r.id,
            task_name: r.task_name,
            queue: r.queue,
            status: r
                .status
                .parse()
                .map_err(|_| SchedulerError::Invariant("unknown task status"))?,
            priority: r.priority,
            due_at: r.due_at,
            attempts: r.attempts,
            max_attempts: r.max_attempts,
            last_error: r.last_error,
            created_at: r.created_at,
            completed_at: r.completed_at,
            locked_by: r.locked_by,
            locked_until: r.locked_until,
            attempt_started_at: r.attempt_started_at,
            attempt_finished_at: r.attempt_finished_at,
        })
    }
}

/// Columns of `Row`: `t` is the task, `a` its current attempt (primary-key join).
macro_rules! select {
    ($from:expr, $order:literal) => {
        concat!(
            "SELECT t.id, t.task_name, t.queue, t.status, t.priority, t.due_at, t.attempts, \
             t.max_attempts, t.last_error, t.created_at, t.completed_at, t.locked_by, \
             t.locked_until, a.started_at AS attempt_started_at, \
             a.finished_at AS attempt_finished_at FROM ",
            $from,
            " AS t LEFT JOIN queue_task_attempts a \
             ON a.task_id = t.id AND a.lock_token = t.lock_token ORDER BY ",
            $order
        )
    };
}

// Due segment, exactly the claim order. A mixed-direction keyset needs the expanded predicate;
// each branch is its own ordered index scan (equal priority, then lower priority), so neither
// reads past `limit` rows. Binds: $1 queue, $2 priority, $3 due_at, $4 id, $5 limit.
#[doc(hidden)]
pub const QUEUED_DUE: &str = select!(
    "((SELECT * FROM queue_tasks WHERE status = 'queued' AND queue = $1 AND due_at <= now() \
        AND priority = $2 AND (due_at, id) > ($3, $4) \
        ORDER BY priority DESC, due_at, id LIMIT $5) \
      UNION ALL \
      (SELECT * FROM queue_tasks WHERE status = 'queued' AND queue = $1 AND due_at <= now() \
        AND priority < $2 ORDER BY priority DESC, due_at, id LIMIT $5) \
      ORDER BY priority DESC, due_at, id LIMIT $5)",
    "t.priority DESC, t.due_at, t.id"
);

// Not-yet-due segment. Binds: $1 queue, $2 due_at, $3 id, $4 limit.
#[doc(hidden)]
pub const QUEUED_FUTURE: &str = select!(
    "(SELECT * FROM queue_tasks WHERE status = 'queued' AND queue = $1 AND due_at > now() \
      AND (due_at, id) > ($2, $3) ORDER BY due_at, id LIMIT $4)",
    "t.due_at, t.id"
);

// Binds: $1 locked_until, $2 id, $3 limit.
#[doc(hidden)]
pub const RUNNING: &str = select!(
    "(SELECT * FROM queue_tasks WHERE status = 'running' AND (locked_until, id) > ($1, $2) \
      ORDER BY locked_until, id LIMIT $3)",
    "t.locked_until, t.id"
);

// Binds: $1 completed_at, $2 id, $3 limit.
#[doc(hidden)]
pub const DONE: &str = select!(
    "(SELECT * FROM queue_tasks WHERE status = 'done' AND (completed_at, id) < ($1, $2) \
      ORDER BY completed_at DESC, id DESC LIMIT $3)",
    "t.completed_at DESC, t.id DESC"
);

// Binds: $1 status, $2 last_error (NULL = any), $3 completed_at, $4 id, $5 limit. The literal
// `status IN (...)` lets a generic plan (where `$1` is unknown) still prove the partial index's
// predicate; without it, a repeated prepared statement can fall back to a seq scan.
#[doc(hidden)]
pub const FAILED: &str = select!(
    "(SELECT * FROM queue_tasks WHERE status = $1 AND status IN ('dead', 'cancelled') \
      AND ($2::text IS NULL OR last_error = $2) \
      AND (completed_at, id) < ($3, $4) ORDER BY completed_at DESC, id DESC LIMIT $5)",
    "t.completed_at DESC, t.id DESC"
);

// As FAILED plus $6 task_name; the name leads `queue_tasks_failed_name_idx`.
#[doc(hidden)]
pub const FAILED_BY_NAME: &str = select!(
    "(SELECT * FROM queue_tasks WHERE task_name = $6 AND status = $1 \
      AND status IN ('dead', 'cancelled') AND ($2::text IS NULL OR last_error = $2) \
      AND (completed_at, id) < ($3, $4) ORDER BY completed_at DESC, id DESC LIMIT $5)",
    "t.completed_at DESC, t.id DESC"
);

/// Stand-ins for "no cursor" that keep every keyset predicate a plain index bound.
fn earliest() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(1, 1, 1, 0, 0, 0).unwrap()
}
fn latest() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(9999, 12, 31, 0, 0, 0).unwrap()
}

fn rows(raw: Vec<Row>) -> Result<Vec<TaskRow>, SchedulerError> {
    raw.into_iter().map(TaskRow::try_from).collect()
}

/// Keep `limit` of the `limit + 1` fetched rows; `next` is set iff one was left over.
fn page<C>(
    mut items: Vec<TaskRow>,
    limit: i64,
    cursor: impl Fn(&TaskRow, usize) -> C,
) -> Page<TaskRow, C> {
    let more = items.len() as i64 > limit;
    items.truncate(limit.max(0) as usize);
    let next = more
        .then(|| items.last().map(|r| cursor(r, items.len() - 1)))
        .flatten();
    Page { items, next }
}

/// Queued tasks of one queue in dequeue order: due tasks by `(priority DESC, due_at, id)` (the
/// claim order), then not-yet-due tasks by `(due_at, id)`.
pub async fn queued_page(
    conn: &mut AsyncPgConnection,
    queue: &QueueName,
    after: Option<&QueuedCursor>,
    limit: i64,
) -> Result<Page<TaskRow, QueuedCursor>, SchedulerError> {
    let want = limit + 1;
    let mut items = Vec::new();
    let mut due_len = 0;
    if after.is_none_or(|c| c.due) {
        // No cursor: priority "above everything" makes the second branch the whole segment.
        let (p, d, i) = after.map_or((i64::from(i32::MAX) + 1, earliest(), 0), |c| {
            (i64::from(c.priority), c.due_at, c.id)
        });
        items = rows(
            diesel::sql_query(QUEUED_DUE)
                .bind::<sql_types::Text, _>(queue)
                .bind::<sql_types::BigInt, _>(p)
                .bind::<sql_types::Timestamptz, _>(d)
                .bind::<sql_types::BigInt, _>(i)
                .bind::<sql_types::BigInt, _>(want)
                .get_results::<Row>(conn)
                .await?,
        )?;
        due_len = items.len();
    }
    if (items.len() as i64) < want {
        let (d, i) = after
            .filter(|c| !c.due)
            .map_or((earliest(), 0), |c| (c.due_at, c.id));
        items.extend(rows(
            diesel::sql_query(QUEUED_FUTURE)
                .bind::<sql_types::Text, _>(queue)
                .bind::<sql_types::Timestamptz, _>(d)
                .bind::<sql_types::BigInt, _>(i)
                .bind::<sql_types::BigInt, _>(want - items.len() as i64)
                .get_results::<Row>(conn)
                .await?,
        )?);
    }
    let base = after.map_or(0, |c| c.position);
    Ok(page(items, limit, |r, idx| QueuedCursor {
        due: idx < due_len,
        priority: r.priority,
        due_at: r.due_at,
        id: r.id.0,
        position: base + idx as i64 + 1,
    }))
}

/// Running tasks by lease expiry, soonest first.
pub async fn running_page(
    conn: &mut AsyncPgConnection,
    after: Option<&RunningCursor>,
    limit: i64,
) -> Result<Page<TaskRow, RunningCursor>, SchedulerError> {
    let (u, i) = after.map_or((earliest(), 0), |c| (c.locked_until, c.id));
    let items = rows(
        diesel::sql_query(RUNNING)
            .bind::<sql_types::Timestamptz, _>(u)
            .bind::<sql_types::BigInt, _>(i)
            .bind::<sql_types::BigInt, _>(limit + 1)
            .get_results::<Row>(conn)
            .await?,
    )?;
    Ok(page(items, limit, |r, _| RunningCursor {
        locked_until: r.locked_until.unwrap_or_else(earliest),
        id: r.id.0,
    }))
}

fn finished(r: &TaskRow, _: usize) -> FinishedCursor {
    FinishedCursor {
        completed_at: r.completed_at.unwrap_or_else(latest),
        id: r.id.0,
    }
}

/// Done tasks, newest completion first.
pub async fn done_page(
    conn: &mut AsyncPgConnection,
    after: Option<&FinishedCursor>,
    limit: i64,
) -> Result<Page<TaskRow, FinishedCursor>, SchedulerError> {
    let (c, i) = after.map_or((latest(), i64::MAX), |c| (c.completed_at, c.id));
    let items = rows(
        diesel::sql_query(DONE)
            .bind::<sql_types::Timestamptz, _>(c)
            .bind::<sql_types::BigInt, _>(i)
            .bind::<sql_types::BigInt, _>(limit + 1)
            .get_results::<Row>(conn)
            .await?,
    )?;
    Ok(page(items, limit, finished))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailedStatus {
    Dead,
    Cancelled,
}

#[derive(Debug, Clone, Default)]
pub struct FailedFilter {
    pub task_name: Option<TaskName>,
    /// Exact match, as bulk replay's filter.
    pub last_error: Option<String>,
}

/// Dead or cancelled tasks, newest completion first.
pub async fn failed_page(
    conn: &mut AsyncPgConnection,
    status: FailedStatus,
    filter: &FailedFilter,
    after: Option<&FinishedCursor>,
    limit: i64,
) -> Result<Page<TaskRow, FinishedCursor>, SchedulerError> {
    let (c, i) = after.map_or((latest(), i64::MAX), |c| (c.completed_at, c.id));
    let status = match status {
        FailedStatus::Dead => TaskStatus::Dead,
        FailedStatus::Cancelled => TaskStatus::Cancelled,
    };
    let q = diesel::sql_query(if filter.task_name.is_some() {
        FAILED_BY_NAME
    } else {
        FAILED
    })
    .bind::<sql_types::Text, _>(status.as_str())
    .bind::<sql_types::Nullable<sql_types::Text>, _>(filter.last_error.as_deref())
    .bind::<sql_types::Timestamptz, _>(c)
    .bind::<sql_types::BigInt, _>(i)
    .bind::<sql_types::BigInt, _>(limit + 1);
    let items = match &filter.task_name {
        Some(n) => rows(
            q.bind::<sql_types::Text, _>(n)
                .get_results::<Row>(conn)
                .await?,
        )?,
        None => rows(q.get_results::<Row>(conn).await?)?,
    };
    Ok(page(items, limit, finished))
}

/// A task with its attempt history and the admin actions that touched it.
#[derive(Debug, Clone)]
pub struct TaskDetail {
    pub task: TaskView,
    pub attempts: Vec<Attempt>,
    pub actions: Vec<AdminAction>,
}

pub async fn task_detail(
    conn: &mut AsyncPgConnection,
    id: TaskId,
) -> Result<Option<TaskDetail>, SchedulerError> {
    let Some(task) = task(conn, id).await? else {
        return Ok(None);
    };
    let attempts = attempts(conn, id).await?;
    let actions = task_actions(conn, &task).await?;
    Ok(Some(TaskDetail {
        task,
        attempts,
        actions,
    }))
}
