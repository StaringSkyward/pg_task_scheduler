//! Bounded, keyset-paged task lists and task detail for the admin pages. Every list reads at
//! most `limit + 1` rows through an index, however large `queue_tasks` grows.

use std::num::NonZeroU32;

use chrono::DateTime;
use chrono::Utc;
use diesel::sql_types;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use serde::Deserialize;
use serde::Serialize;

use super::AdminAction;
use super::attempts;
use super::task;
use super::task_actions;
use crate::error::SchedulerError;
use crate::ids::QueueName;
use crate::ids::TaskId;
use crate::ids::TaskName;
use crate::ids::WorkerId;
use crate::models::Attempt;
use crate::models::AttemptRun;
use crate::models::AttemptState;
use crate::models::MaxAttempts;
use crate::models::TaskState;
use crate::models::TaskStatus;
use crate::models::TaskView;

/// One row of a task list.
#[derive(Debug, Clone)]
pub struct TaskRow {
    /// Row id.
    pub id: TaskId,
    /// Selects the handler.
    pub task_name: TaskName,
    /// The queue it runs in.
    pub queue: QueueName,
    /// Lifecycle state, with the lease while running and the completion time once finished.
    pub state: TaskState,
    /// Higher is claimed first, within its queue.
    pub priority: i32,
    /// Earliest time a worker may claim it.
    pub due_at: DateTime<Utc>,
    /// Attempts counted so far.
    pub attempts: u32,
    /// Attempts allowed in total.
    pub max_attempts: MaxAttempts,
    /// Message of the latest failure.
    pub last_error: Option<String>,
    /// When it was enqueued.
    pub created_at: DateTime<Utc>,
    /// The current attempt; `None` if no attempt carries the task's current `lock_token` (never
    /// claimed, or replayed since).
    pub attempt: Option<AttemptRun>,
}

/// One page and, if more rows follow, the cursor for the next.
#[derive(Debug, Clone)]
pub struct Page<T, C> {
    /// At most `limit` rows.
    pub items: Vec<T>,
    /// Pass as `after` to get the next page; `None` on the last page.
    pub next: Option<C>,
}

/// Resume point in the queued list: due tasks in claim order, then not-yet-due tasks.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueuedCursor {
    /// Resume in the due segment (else the not-yet-due one).
    pub due: bool,
    /// Last row's priority.
    pub priority: i32,
    /// Last row's `due_at`.
    pub due_at: DateTime<Utc>,
    /// Last row's id.
    pub id: i64,
    /// Rows before the next page.
    pub position: i64,
}

/// Resume point in the running list.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunningCursor {
    /// Last row's `locked_until`.
    pub locked_until: DateTime<Utc>,
    /// Last row's id.
    pub id: i64,
}

/// Resume point in the done, dead and cancelled lists.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FinishedCursor {
    /// Last row's `completed_at`.
    pub completed_at: DateTime<Utc>,
    /// Last row's id.
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
    #[diesel(sql_type = sql_types::Nullable<sql_types::Text>)]
    attempt_outcome: Option<String>,
}

impl TryFrom<Row> for TaskRow {
    type Error = SchedulerError;
    fn try_from(r: Row) -> Result<Self, SchedulerError> {
        Ok(TaskRow {
            id: r.id,
            task_name: r.task_name,
            queue: r.queue,
            state: TaskState::from_columns(&r.status, r.locked_by, r.locked_until, r.completed_at)?,
            priority: r.priority,
            due_at: r.due_at,
            attempts: u32::try_from(r.attempts)
                .map_err(|_| SchedulerError::Invariant("stored attempts is negative"))?,
            max_attempts: MaxAttempts::from_db_i32(r.max_attempts)
                .map_err(|_| SchedulerError::Invariant("stored max_attempts is not positive"))?,
            last_error: r.last_error,
            created_at: r.created_at,
            // A join miss nulls all three; `started_at` is NOT NULL, so it alone decides.
            attempt: match r.attempt_started_at {
                Some(started_at) => Some(AttemptRun {
                    started_at,
                    state: AttemptState::from_columns(
                        r.attempt_finished_at,
                        r.attempt_outcome.as_deref(),
                    )?,
                }),
                None => None,
            },
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
             a.finished_at AS attempt_finished_at, a.outcome AS attempt_outcome FROM ",
            $from,
            " AS t LEFT JOIN queue_task_attempts a \
             ON a.task_id = t.id AND a.lock_token = t.lock_token ORDER BY ",
            $order
        )
    };
}

// Due segment, exactly the claim order. A mixed-direction keyset needs the expanded predicate;
// each branch is its own ordered index scan (equal priority, then lower priority), so neither
// reads past `limit` rows. Binds: $1 queue, $2 priority, $3 due_at (NULL = first page),
// $4 id, $5 limit.
#[doc(hidden)]
pub const QUEUED_DUE: &str = select!(
    "((SELECT * FROM queue_tasks WHERE status = 'queued' AND queue = $1 AND due_at <= now() \
        AND priority = $2 AND (due_at, id) > (COALESCE($3, '-infinity'::timestamptz), $4) \
        ORDER BY priority DESC, due_at, id LIMIT $5) \
      UNION ALL \
      (SELECT * FROM queue_tasks WHERE status = 'queued' AND queue = $1 AND due_at <= now() \
        AND priority < $2 ORDER BY priority DESC, due_at, id LIMIT $5) \
      ORDER BY priority DESC, due_at, id LIMIT $5)",
    "t.priority DESC, t.due_at, t.id"
);

// Not-yet-due segment. Binds: $1 queue, $2 due_at (NULL = first page), $3 id, $4 limit.
#[doc(hidden)]
pub const QUEUED_FUTURE: &str = select!(
    "(SELECT * FROM queue_tasks WHERE status = 'queued' AND queue = $1 AND due_at > now() \
      AND (due_at, id) > (COALESCE($2, '-infinity'::timestamptz), $3) \
      ORDER BY due_at, id LIMIT $4)",
    "t.due_at, t.id"
);

// Binds: $1 locked_until (NULL = first page), $2 id, $3 limit.
#[doc(hidden)]
pub const RUNNING: &str = select!(
    "(SELECT * FROM queue_tasks WHERE status = 'running' \
      AND (locked_until, id) > (COALESCE($1, '-infinity'::timestamptz), $2) \
      ORDER BY locked_until, id LIMIT $3)",
    "t.locked_until, t.id"
);

// Binds: $1 completed_at (NULL = first page), $2 id, $3 limit.
#[doc(hidden)]
pub const DONE: &str = select!(
    "(SELECT * FROM queue_tasks WHERE status = 'done' \
      AND (completed_at, id) < (COALESCE($1, 'infinity'::timestamptz), $2) \
      ORDER BY completed_at DESC, id DESC LIMIT $3)",
    "t.completed_at DESC, t.id DESC"
);

// Binds: $1 status, $2 last_error (NULL = any), $3 completed_at (NULL = first page), $4 id,
// $5 limit. The literal `status IN (...)` lets a generic plan (where `$1` is unknown) still
// prove the partial index's predicate; without it, a repeated prepared statement can fall back
// to a seq scan.
#[doc(hidden)]
pub const FAILED: &str = select!(
    "(SELECT * FROM queue_tasks WHERE status = $1 AND status IN ('dead', 'cancelled') \
      AND ($2::text IS NULL OR last_error = $2) \
      AND (completed_at, id) < (COALESCE($3, 'infinity'::timestamptz), $4) \
      ORDER BY completed_at DESC, id DESC LIMIT $5)",
    "t.completed_at DESC, t.id DESC"
);

// As FAILED plus $6 task_name; the name leads `queue_tasks_failed_name_idx`.
#[doc(hidden)]
pub const FAILED_BY_NAME: &str = select!(
    "(SELECT * FROM queue_tasks WHERE task_name = $6 AND status = $1 \
      AND status IN ('dead', 'cancelled') AND ($2::text IS NULL OR last_error = $2) \
      AND (completed_at, id) < (COALESCE($3, 'infinity'::timestamptz), $4) \
      ORDER BY completed_at DESC, id DESC LIMIT $5)",
    "t.completed_at DESC, t.id DESC"
);

/// The `limit + 1` rows fetched so the page can tell whether more follow. Cannot overflow.
fn lookahead(limit: NonZeroU32) -> i64 {
    i64::from(limit.get()) + 1
}

fn rows_len(items: &[TaskRow]) -> Result<i64, SchedulerError> {
    i64::try_from(items.len()).map_err(|_| SchedulerError::Invariant("row count exceeds i64"))
}

fn rows(raw: Vec<Row>) -> Result<Vec<TaskRow>, SchedulerError> {
    raw.into_iter().map(TaskRow::try_from).collect()
}

/// Keep `limit` of the `limit + 1` fetched rows; `next` is set iff one was left over.
fn page<C>(
    mut items: Vec<TaskRow>,
    limit: NonZeroU32,
    cursor: impl Fn(&TaskRow, usize) -> Result<C, SchedulerError>,
) -> Result<Page<TaskRow, C>, SchedulerError> {
    let limit = usize::try_from(limit.get())
        .map_err(|_| SchedulerError::Invariant("page limit does not fit in usize"))?;
    let more = items.len() > limit;
    items.truncate(limit);
    let next = match items.last() {
        Some(r) if more => Some(cursor(r, items.len() - 1)?),
        _ => None,
    };
    Ok(Page { items, next })
}

/// Queued tasks of one queue in dequeue order: due tasks by `(priority DESC, due_at, id)` (the
/// claim order), then not-yet-due tasks by `(due_at, id)`.
pub async fn queued_page(
    conn: &mut AsyncPgConnection,
    queue: &QueueName,
    after: Option<&QueuedCursor>,
    limit: NonZeroU32,
) -> Result<Page<TaskRow, QueuedCursor>, SchedulerError> {
    let want = lookahead(limit);
    let mut items = Vec::new();
    let mut due_len = 0;
    if after.is_none_or(|c| c.due) {
        // No cursor: priority "above everything" makes the second branch the whole segment.
        // A page binds NULL for its timestamp, which the SQL reads as `-infinity` or `infinity`,
        // the type's own bounds. The id and priority stand-ins are in-domain bounds: `i64::MAX`
        // for `bigint`, `i32::MAX + 1` above every `integer` and `0` below every identity id
        // (they start at 1). Every keyset predicate stays a plain index bound.
        let (p, d, i) = after.map_or((i64::from(i32::MAX) + 1, None, 0), |c| {
            (i64::from(c.priority), Some(c.due_at), c.id)
        });
        items = rows(
            diesel::sql_query(QUEUED_DUE)
                .bind::<sql_types::Text, _>(queue)
                .bind::<sql_types::BigInt, _>(p)
                .bind::<sql_types::Nullable<sql_types::Timestamptz>, _>(d)
                .bind::<sql_types::BigInt, _>(i)
                .bind::<sql_types::BigInt, _>(want)
                .get_results::<Row>(conn)
                .await?,
        )?;
        due_len = items.len();
    }
    if rows_len(&items)? < want {
        let (d, i) = after
            .filter(|c| !c.due)
            .map_or((None, 0), |c| (Some(c.due_at), c.id));
        items.extend(rows(
            diesel::sql_query(QUEUED_FUTURE)
                .bind::<sql_types::Text, _>(queue)
                .bind::<sql_types::Nullable<sql_types::Timestamptz>, _>(d)
                .bind::<sql_types::BigInt, _>(i)
                .bind::<sql_types::BigInt, _>(want - rows_len(&items)?)
                .get_results::<Row>(conn)
                .await?,
        )?);
    }
    let base = after.map_or(0, |c| c.position);
    page(items, limit, |r, idx| {
        Ok(QueuedCursor {
            due: idx < due_len,
            priority: r.priority,
            due_at: r.due_at,
            id: r.id.0,
            position: i64::try_from(idx)
                .ok()
                .and_then(|i| base.checked_add(i)?.checked_add(1))
                .ok_or(SchedulerError::InvalidCursor(
                    "queued cursor position overflow",
                ))?,
        })
    })
}

/// Running tasks by lease expiry, soonest first.
pub async fn running_page(
    conn: &mut AsyncPgConnection,
    after: Option<&RunningCursor>,
    limit: NonZeroU32,
) -> Result<Page<TaskRow, RunningCursor>, SchedulerError> {
    let (u, i) = after.map_or((None, 0), |c| (Some(c.locked_until), c.id));
    let items = rows(
        diesel::sql_query(RUNNING)
            .bind::<sql_types::Nullable<sql_types::Timestamptz>, _>(u)
            .bind::<sql_types::BigInt, _>(i)
            .bind::<sql_types::BigInt, _>(lookahead(limit))
            .get_results::<Row>(conn)
            .await?,
    )?;
    page(items, limit, running)
}

fn running(r: &TaskRow, _: usize) -> Result<RunningCursor, SchedulerError> {
    match r.state {
        TaskState::Running { locked_until, .. } => Ok(RunningCursor {
            locked_until,
            id: r.id.0,
        }),
        _ => Err(SchedulerError::Invariant(
            "running page returned a task in the wrong state",
        )),
    }
}

fn finished(r: &TaskRow, _: usize) -> Result<FinishedCursor, SchedulerError> {
    match r.state {
        TaskState::Finished { completed_at, .. } => Ok(FinishedCursor {
            completed_at,
            id: r.id.0,
        }),
        _ => Err(SchedulerError::Invariant(
            "finished page returned a task in the wrong state",
        )),
    }
}

/// Done tasks, newest completion first.
pub async fn done_page(
    conn: &mut AsyncPgConnection,
    after: Option<&FinishedCursor>,
    limit: NonZeroU32,
) -> Result<Page<TaskRow, FinishedCursor>, SchedulerError> {
    let (c, i) = after.map_or((None, i64::MAX), |c| (Some(c.completed_at), c.id));
    let items = rows(
        diesel::sql_query(DONE)
            .bind::<sql_types::Nullable<sql_types::Timestamptz>, _>(c)
            .bind::<sql_types::BigInt, _>(i)
            .bind::<sql_types::BigInt, _>(lookahead(limit))
            .get_results::<Row>(conn)
            .await?,
    )?;
    page(items, limit, finished)
}

/// Which terminal failure [`failed_page`] lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailedStatus {
    /// `dead` tasks.
    Dead,
    /// `cancelled` tasks.
    Cancelled,
}

/// Narrows [`failed_page`]; the default matches everything.
#[derive(Debug, Clone, Default)]
pub struct FailedFilter {
    /// Only tasks with this name.
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
    limit: NonZeroU32,
) -> Result<Page<TaskRow, FinishedCursor>, SchedulerError> {
    let (c, i) = after.map_or((None, i64::MAX), |c| (Some(c.completed_at), c.id));
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
    .bind::<sql_types::Nullable<sql_types::Timestamptz>, _>(c)
    .bind::<sql_types::BigInt, _>(i)
    .bind::<sql_types::BigInt, _>(lookahead(limit));
    let items = match &filter.task_name {
        Some(n) => rows(
            q.bind::<sql_types::Text, _>(n)
                .get_results::<Row>(conn)
                .await?,
        )?,
        None => rows(q.get_results::<Row>(conn).await?)?,
    };
    page(items, limit, finished)
}

/// A task with its attempt history and the admin actions that touched it.
#[derive(Debug, Clone)]
pub struct TaskDetail {
    /// The task.
    pub task: TaskView,
    /// Its attempts, in order.
    pub attempts: Vec<Attempt>,
    /// Admin actions that touched it.
    pub actions: Vec<AdminAction>,
}

/// The task with this id and its history, if it exists.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn queued_row() -> TaskRow {
        TaskRow {
            id: TaskId(1),
            task_name: TaskName::try_from("job").unwrap(),
            queue: QueueName::default(),
            state: TaskState::Queued,
            priority: 0,
            due_at: Utc::now(),
            attempts: 0,
            max_attempts: MaxAttempts::default_value(),
            last_error: None,
            created_at: Utc::now(),
            attempt: None,
        }
    }

    #[test]
    fn cursor_builders_reject_a_row_in_the_wrong_state() {
        let r = queued_row();
        assert!(matches!(finished(&r, 0), Err(SchedulerError::Invariant(_))));
        assert!(matches!(running(&r, 0), Err(SchedulerError::Invariant(_))));
    }
}
