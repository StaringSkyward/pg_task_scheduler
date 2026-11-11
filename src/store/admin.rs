//! Admin operations and their audit: single-task cancel, `queue_admin_actions` rows and queries.

use std::time::Duration;

use chrono::{DateTime, Utc};
use diesel::sql_types;
use diesel_async::{AsyncPgConnection, RunQueryDsl};

use crate::error::SchedulerError;
use crate::ids::{QueueName, TaskId, TaskName};
use crate::models::{ParseEnumError, TaskView, text_enum};

/// What an operator did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminActionKind {
    Replay,
    Cancel,
    Pause,
    Resume,
}

text_enum!(AdminActionKind, "admin action", {
    Replay => "replay", Cancel => "cancel", Pause => "pause", Resume => "resume",
});

/// One `queue_admin_actions` row.
#[derive(Debug, Clone)]
pub struct AdminAction {
    pub id: i64,
    pub action: AdminActionKind,
    pub task_id: Option<TaskId>,
    pub task_name: Option<TaskName>,
    pub queue: Option<QueueName>,
    pub last_error_filter: Option<String>,
    /// Bulk id bound: the action covered only tasks with `id <=` this.
    pub max_task_id: Option<i64>,
    pub affected: i64,
    pub operator: String,
    pub reason: String,
    pub at: DateTime<Utc>,
}

#[derive(diesel::QueryableByName)]
struct ActionRow {
    #[diesel(sql_type = sql_types::BigInt)]
    id: i64,
    #[diesel(sql_type = sql_types::Text)]
    action: String,
    #[diesel(sql_type = sql_types::Nullable<sql_types::BigInt>)]
    task_id: Option<TaskId>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Text>)]
    task_name: Option<TaskName>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Text>)]
    queue: Option<QueueName>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Text>)]
    last_error_filter: Option<String>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::BigInt>)]
    max_task_id: Option<i64>,
    #[diesel(sql_type = sql_types::BigInt)]
    affected: i64,
    #[diesel(sql_type = sql_types::Text)]
    operator: String,
    #[diesel(sql_type = sql_types::Text)]
    reason: String,
    #[diesel(sql_type = sql_types::Timestamptz)]
    at: DateTime<Utc>,
}

impl TryFrom<ActionRow> for AdminAction {
    type Error = SchedulerError;
    fn try_from(r: ActionRow) -> Result<Self, SchedulerError> {
        Ok(Self {
            id: r.id,
            action: r
                .action
                .parse()
                .map_err(|_: ParseEnumError| SchedulerError::Invariant("unknown admin action"))?,
            task_id: r.task_id,
            task_name: r.task_name,
            queue: r.queue,
            last_error_filter: r.last_error_filter,
            max_task_id: r.max_task_id,
            affected: r.affected,
            operator: r.operator,
            reason: r.reason,
            at: r.at,
        })
    }
}

const COLUMNS: &str = "id, action, task_id, task_name, queue, last_error_filter, max_task_id, \
                       affected, operator, reason, at";

/// Newest-first page of actions; pass the last id seen as `before` for the next page.
pub async fn admin_actions(
    conn: &mut AsyncPgConnection,
    before: Option<i64>,
    limit: i64,
) -> Result<Vec<AdminAction>, SchedulerError> {
    diesel::sql_query(format!(
        "SELECT {COLUMNS} FROM queue_admin_actions \
         WHERE ($1::bigint IS NULL OR id < $1) ORDER BY id DESC LIMIT $2"
    ))
    .bind::<sql_types::Nullable<sql_types::BigInt>, _>(before)
    .bind::<sql_types::BigInt, _>(limit)
    .get_results::<ActionRow>(conn)
    .await?
    .into_iter()
    .map(AdminAction::try_from)
    .collect()
}

/// Actions on one task: rows naming it, plus the (possibly bulk) action it links to.
pub async fn task_actions(
    conn: &mut AsyncPgConnection,
    task: &TaskView,
) -> Result<Vec<AdminAction>, SchedulerError> {
    diesel::sql_query(format!(
        "SELECT {COLUMNS} FROM queue_admin_actions WHERE task_id = $1 OR id = $2 \
         ORDER BY id DESC"
    ))
    .bind::<sql_types::BigInt, _>(task.id)
    .bind::<sql_types::Nullable<sql_types::BigInt>, _>(task.admin_action_id)
    .get_results::<ActionRow>(conn)
    .await?
    .into_iter()
    .map(AdminAction::try_from)
    .collect()
}

/// How long a single cancel retries a row another transaction holds before reporting `Busy`.
const CANCEL_WAIT: Duration = Duration::from_secs(2);
const CANCEL_RETRY: Duration = Duration::from_millis(100);

/// What `cancel` did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    Cancelled,
    /// Already terminal, or no such task.
    NotCancellable,
    /// Another transaction held the row (for example a handler inside `lock_owned`) for the
    /// whole wait; try again.
    Busy,
}

// One statement, never waiting on a row lock (SKIP LOCKED). `busy` reads the statement's
// snapshot without a lock: queued or running there means the row was skipped (or changed
// after the snapshot), so the caller retries.
// The attempt is closed by upsert, not UPDATE: a claim that committed after this statement's
// snapshot, but before the row lock, inserted an attempt the snapshot cannot see; ON CONFLICT
// still finds it. A running task always has its attempt (the claim inserts both at once).
const CANCEL: &str = r#"
    WITH target AS (
        SELECT id, task_name, queue, status, attempts, lock_token, locked_by FROM queue_tasks
        WHERE id = $1 AND status IN ('queued', 'running')
        FOR UPDATE SKIP LOCKED
    ), act AS (
        INSERT INTO queue_admin_actions
            (action, task_id, task_name, queue, affected, operator, reason)
        SELECT 'cancel', id, task_name, queue, 1, $2, $3 FROM target
        RETURNING id
    ), closed AS (
        INSERT INTO queue_task_attempts
            (task_id, attempt_no, lock_token, worker_id, finished_at, outcome, error)
        SELECT id, attempts, lock_token, locked_by, now(), 'cancelled', 'cancelled by operator'
        FROM target WHERE status = 'running'
        ON CONFLICT (task_id, lock_token) DO UPDATE
        SET finished_at = excluded.finished_at, outcome = excluded.outcome,
            error = excluded.error
        WHERE queue_task_attempts.finished_at IS NULL
    ), cancelled AS (
        UPDATE queue_tasks t
        SET status = 'cancelled', locked_by = NULL, locked_until = NULL, completed_at = now(),
            updated_at = now(), admin_action_id = act.id
        FROM target, act
        WHERE t.id = target.id
        RETURNING t.id
    )
    SELECT EXISTS (SELECT 1 FROM cancelled) AS cancelled,
           EXISTS (SELECT 1 FROM queue_tasks WHERE id = $1 AND status IN ('queued', 'running'))
               AS busy
"#;

#[derive(diesel::QueryableByName)]
struct CancelRow {
    #[diesel(sql_type = sql_types::Bool)]
    cancelled: bool,
    #[diesel(sql_type = sql_types::Bool)]
    busy: bool,
}

/// Cancel a queued or running task and audit it. A running task's attempt closes with outcome
/// `cancelled`, and its worker loses the lease at its next heartbeat. Never blocks on a row
/// lock: a row held elsewhere is retried every 100 ms for up to 2 s, then reported `Busy`.
pub async fn cancel(
    conn: &mut AsyncPgConnection,
    id: TaskId,
    operator: &str,
    reason: &str,
) -> Result<CancelOutcome, SchedulerError> {
    let deadline = tokio::time::Instant::now() + CANCEL_WAIT;
    loop {
        let row = diesel::sql_query(CANCEL)
            .bind::<sql_types::BigInt, _>(id)
            .bind::<sql_types::Text, _>(operator)
            .bind::<sql_types::Text, _>(reason)
            .get_result::<CancelRow>(conn)
            .await?;
        if row.cancelled {
            return Ok(CancelOutcome::Cancelled);
        }
        if !row.busy {
            return Ok(CancelOutcome::NotCancellable);
        }
        if tokio::time::Instant::now() + CANCEL_RETRY > deadline {
            return Ok(CancelOutcome::Busy);
        }
        tokio::time::sleep(CANCEL_RETRY).await;
    }
}
