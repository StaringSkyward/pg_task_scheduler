//! Admin operations and their audit: single-task cancel, bulk actions, pauses, and the
//! `queue_admin_actions` rows (with the tasks each changed) and queries.

use std::num::NonZeroU32;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use diesel::sql_types;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;

use crate::error::SchedulerError;
use crate::ids::QueueName;
use crate::ids::TaskId;
use crate::ids::TaskName;
use crate::models::ParseEnumError;
use crate::models::text_enum;
use crate::store::Count;

/// What an operator did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminActionKind {
    /// Moved `dead` or `cancelled` tasks back to `queued`.
    Replay,
    /// Cancelled `queued` or `running` tasks.
    Cancel,
    /// Paused a queue.
    Pause,
    /// Resumed a queue.
    Resume,
}

text_enum!(AdminActionKind, "admin action", {
    Replay => "replay", Cancel => "cancel", Pause => "pause", Resume => "resume",
});

/// A single-task action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskAction {
    /// Moved the task back to `queued`.
    Replay,
    /// Cancelled the task.
    Cancel,
}

/// Which `last_error` values a bulk replay, or the failed list, matches.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ErrorFilter {
    /// Any `last_error`, or none.
    #[default]
    Any,
    /// Only tasks with no `last_error` (NULL).
    Missing,
    /// Only tasks whose `last_error` is exactly this text; `Exact("")` matches only `''`.
    Exact(String),
}

impl ErrorFilter {
    /// The stored mode and the text the statements bind: `($m = 'any' OR ($m = 'missing' AND
    /// last_error IS NULL) OR ($m = 'exact' AND last_error = $v))`.
    pub(crate) fn binds(&self) -> (&'static str, Option<&str>) {
        match self {
            Self::Any => ("any", None),
            Self::Missing => ("missing", None),
            Self::Exact(e) => ("exact", Some(e)),
        }
    }
}

/// A bulk action over a task name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BulkAction {
    /// Requeued dead and cancelled tasks whose `last_error` matches `error`.
    Replay {
        /// The `last_error` values the replay was limited to.
        error: ErrorFilter,
    },
    /// Cancelled queued and running tasks.
    Cancel,
}

/// A queue-wide action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueAction {
    /// Paused the queue.
    Pause,
    /// Resumed the queue.
    Resume,
}

/// What an admin action was aimed at; the shape the table's CHECK allows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminActionTarget {
    /// One task.
    Task {
        /// What was done.
        action: TaskAction,
        /// The task.
        task_id: TaskId,
        /// Its name.
        task_name: TaskName,
        /// Its queue.
        queue: QueueName,
    },
    /// Every matching task of a name, up to a bound.
    Bulk {
        /// What was done.
        action: BulkAction,
        /// The task name acted on.
        task_name: TaskName,
        /// Limited to this queue, or any queue when `None`.
        queue: Option<QueueName>,
        /// The action covered only tasks with `id <=` this.
        max_task_id: i64,
    },
    /// A whole queue.
    Queue {
        /// What was done.
        action: QueueAction,
        /// The queue.
        queue: QueueName,
    },
}

/// One `queue_admin_actions` row.
#[derive(Debug, Clone)]
pub struct AdminAction {
    /// Row id.
    pub id: i64,
    /// What was done, and to what.
    pub target: AdminActionTarget,
    /// Tasks it changed (its `queue_admin_action_tasks` rows), counted up to the reader's
    /// count cap.
    pub affected: Count,
    /// Who did it, as the caller passed it.
    pub operator: String,
    /// Why, as the caller passed it.
    pub reason: String,
    /// When.
    pub at: DateTime<Utc>,
}

impl AdminAction {
    /// The stored action tag.
    pub fn kind(&self) -> AdminActionKind {
        match &self.target {
            AdminActionTarget::Task { action, .. } => match action {
                TaskAction::Replay => AdminActionKind::Replay,
                TaskAction::Cancel => AdminActionKind::Cancel,
            },
            AdminActionTarget::Bulk { action, .. } => match action {
                BulkAction::Replay { .. } => AdminActionKind::Replay,
                BulkAction::Cancel => AdminActionKind::Cancel,
            },
            AdminActionTarget::Queue { action, .. } => match action {
                QueueAction::Pause => AdminActionKind::Pause,
                QueueAction::Resume => AdminActionKind::Resume,
            },
        }
    }
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
    error_filter: Option<String>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Text>)]
    last_error_filter: Option<String>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::BigInt>)]
    max_task_id: Option<i64>,
    #[diesel(sql_type = sql_types::BigInt)]
    affected: i64,
    #[diesel(sql_type = sql_types::Bool)]
    capped: bool,
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
        let kind: AdminActionKind = r
            .action
            .parse()
            .map_err(|_: ParseEnumError| SchedulerError::Invariant("unknown admin action"))?;
        let bad = || SchedulerError::Invariant("admin action row violates its shape");
        let error = match (r.error_filter.as_deref(), r.last_error_filter) {
            (None, None) => None,
            (Some("any"), None) => Some(ErrorFilter::Any),
            (Some("missing"), None) => Some(ErrorFilter::Missing),
            (Some("exact"), Some(e)) => Some(ErrorFilter::Exact(e)),
            _ => return Err(bad()),
        };
        let target = match (kind, r.task_id, r.task_name, r.queue, error, r.max_task_id) {
            (
                AdminActionKind::Pause | AdminActionKind::Resume,
                None,
                None,
                Some(queue),
                None,
                None,
            ) => {
                let action = if kind == AdminActionKind::Pause {
                    QueueAction::Pause
                } else {
                    QueueAction::Resume
                };
                AdminActionTarget::Queue { action, queue }
            }
            (
                AdminActionKind::Replay | AdminActionKind::Cancel,
                Some(task_id),
                Some(task_name),
                Some(queue),
                None,
                None,
            ) => AdminActionTarget::Task {
                action: if kind == AdminActionKind::Replay {
                    TaskAction::Replay
                } else {
                    TaskAction::Cancel
                },
                task_id,
                task_name,
                queue,
            },
            (
                AdminActionKind::Replay,
                None,
                Some(task_name),
                queue,
                Some(error),
                Some(max_task_id),
            ) => AdminActionTarget::Bulk {
                action: BulkAction::Replay { error },
                task_name,
                queue,
                max_task_id,
            },
            (AdminActionKind::Cancel, None, Some(task_name), queue, None, Some(max_task_id)) => {
                AdminActionTarget::Bulk {
                    action: BulkAction::Cancel,
                    task_name,
                    queue,
                    max_task_id,
                }
            }
            _ => return Err(bad()),
        };
        Ok(Self {
            id: r.id,
            target,
            affected: Count {
                value: r.affected,
                capped: r.capped,
            },
            operator: r.operator,
            reason: r.reason,
            at: r.at,
        })
    }
}

// Binds: $1 count cap. `affected` counts the action's membership rows, reading at most the cap
// (capped when it reaches it, the admin UI's convention). A single action's task is its one
// membership row; were there two, the scalar subquery fails rather than pick one.
const SELECT_ACTIONS: &str = "\
    SELECT a.id, a.action, \
           (SELECT m.task_id FROM queue_admin_action_tasks m \
            WHERE m.action_id = a.id AND a.max_task_id IS NULL) AS task_id, \
           a.task_name, a.queue, a.error_filter, a.last_error_filter, a.max_task_id, \
           c.n AS affected, c.n >= $1 AS capped, a.operator, a.reason, a.at \
    FROM queue_admin_actions a CROSS JOIN LATERAL (SELECT count(*) AS n FROM \
        (SELECT 1 FROM queue_admin_action_tasks m WHERE m.action_id = a.id LIMIT $1) s) c";

fn rows(rows: Vec<ActionRow>) -> Result<Vec<AdminAction>, SchedulerError> {
    rows.into_iter().map(AdminAction::try_from).collect()
}

/// Newest-first page of actions; pass the last id seen as `before` for the next page. Each
/// `affected` counts at most `count_cap` tasks.
pub async fn admin_actions(
    conn: &mut AsyncPgConnection,
    before: Option<i64>,
    limit: NonZeroU32,
    count_cap: NonZeroU32,
) -> Result<Vec<AdminAction>, SchedulerError> {
    rows(
        diesel::sql_query(format!(
            "{SELECT_ACTIONS} WHERE ($2::bigint IS NULL OR a.id < $2) ORDER BY a.id DESC LIMIT $3"
        ))
        .bind::<sql_types::BigInt, _>(i64::from(count_cap.get()))
        .bind::<sql_types::Nullable<sql_types::BigInt>, _>(before)
        .bind::<sql_types::BigInt, _>(i64::from(limit.get()))
        .get_results::<ActionRow>(conn)
        .await?,
    )
}

/// The action with this id, if there is one. Its `affected` counts at most `count_cap` tasks.
pub async fn admin_action(
    conn: &mut AsyncPgConnection,
    id: i64,
    count_cap: NonZeroU32,
) -> Result<Option<AdminAction>, SchedulerError> {
    let found = diesel::sql_query(format!("{SELECT_ACTIONS} WHERE a.id = $2"))
        .bind::<sql_types::BigInt, _>(i64::from(count_cap.get()))
        .bind::<sql_types::BigInt, _>(id)
        .get_results::<ActionRow>(conn)
        .await?;
    Ok(rows(found)?.pop())
}

/// Every action that changed the task, single or bulk, newest first. Each `affected` counts at
/// most `count_cap` tasks.
pub async fn task_actions(
    conn: &mut AsyncPgConnection,
    task_id: TaskId,
    count_cap: NonZeroU32,
) -> Result<Vec<AdminAction>, SchedulerError> {
    rows(
        diesel::sql_query(format!(
            "{SELECT_ACTIONS} WHERE a.id IN \
             (SELECT action_id FROM queue_admin_action_tasks WHERE task_id = $2) \
             ORDER BY a.id DESC"
        ))
        .bind::<sql_types::BigInt, _>(i64::from(count_cap.get()))
        .bind::<sql_types::BigInt, _>(task_id)
        .get_results::<ActionRow>(conn)
        .await?,
    )
}

/// How long a single cancel retries a row another transaction holds before reporting `Busy`.
const CANCEL_WAIT: Duration = Duration::from_secs(2);
const CANCEL_RETRY: Duration = Duration::from_millis(100);

/// What `cancel` did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    /// The task is now `cancelled`.
    Cancelled,
    /// Already terminal, or no such task.
    NotCancellable,
    /// Another transaction held the row (for example a handler inside `lock_owned`) for the
    /// whole wait; try again.
    Busy,
}

// One statement, never waiting on a row lock (SKIP LOCKED): the audit row, its membership row
// and the transition commit together. `busy` reads the statement's snapshot without a lock:
// queued or running there means the row was skipped (or changed after the snapshot), so the
// caller retries.
// The attempt is closed by upsert, not UPDATE: a claim that committed after this statement's
// snapshot, but before the row lock, inserted an attempt the snapshot cannot see; ON CONFLICT
// still finds it. A running task always has its attempt (the claim inserts both at once).
const CANCEL: &str = r#"
    WITH target AS (
        SELECT id, task_name, queue, status, attempts, lock_token, locked_by FROM queue_tasks
        WHERE id = $1 AND status IN ('queued', 'running')
        FOR UPDATE SKIP LOCKED
    ), act AS (
        INSERT INTO queue_admin_actions (action, task_name, queue, operator, reason)
        SELECT 'cancel', task_name, queue, $2, $3 FROM target
        RETURNING id
    ), closed AS (
        INSERT INTO queue_task_attempts
            (task_id, attempt_no, lock_token, worker_id, finished_at, outcome, error)
        SELECT id, attempts, lock_token, locked_by, statement_timestamp(), 'cancelled', 'cancelled by operator'
        FROM target WHERE status = 'running'
        ON CONFLICT (task_id, lock_token) DO UPDATE
        SET finished_at = excluded.finished_at, outcome = excluded.outcome,
            error = excluded.error
        WHERE queue_task_attempts.finished_at IS NULL
    ), cancelled AS (
        UPDATE queue_tasks t
        SET status = 'cancelled', locked_by = NULL, locked_until = NULL, completed_at = statement_timestamp(),
            updated_at = statement_timestamp()
        FROM target
        WHERE t.id = target.id
        RETURNING t.id
    ), member AS (
        INSERT INTO queue_admin_action_tasks (action_id, action, task_id)
        SELECT act.id, 'cancel', cancelled.id FROM act, cancelled
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

/// Rows one bulk batch locks and changes.
pub const BULK_BATCH: i64 = 500;
/// How long a bulk call keeps starting batches.
pub const BULK_BUDGET: Duration = Duration::from_secs(10);

/// Which tasks a bulk action targets.
#[derive(Debug, Clone)]
pub struct BulkFilter {
    /// Only tasks with this name.
    pub task_name: TaskName,
    /// Any queue when `None`.
    pub queue: Option<QueueName>,
}

/// Why a bulk call stopped. Neither proves nothing matching is left.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BulkStop {
    /// The budget ran out after a full batch; [`resume_bulk`] continues the same action.
    BudgetSpent,
    /// A batch came back short: the matches ran out, or rows locked by other work were
    /// skipped. Calling again (or resuming) catches the skipped ones.
    ShortBatch,
}

/// What one bulk call did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BulkOutcome {
    /// The `queue_admin_actions` row recording the action.
    pub action_id: i64,
    /// Tasks this call changed, exactly.
    pub affected: i64,
    /// Why the call stopped.
    pub stopped: BulkStop,
}

#[derive(diesel::QueryableByName)]
struct NewAction {
    #[diesel(sql_type = sql_types::BigInt)]
    id: i64,
    #[diesel(sql_type = sql_types::BigInt)]
    max_task_id: i64,
}

#[derive(diesel::QueryableByName)]
struct N {
    #[diesel(sql_type = sql_types::BigInt)]
    n: i64,
}

// Binds: $1 task_name, $2 queue (NULL = any), $3 max_task_id, $4 action id, $5 batch size.
// One statement per batch: lock, transition, and record each changed task as the action's
// membership row together, so the audit never drifts from what was applied. A task the action
// already changed is skipped (the anti-join), so a replay by another operator mid-sweep is not
// cancelled again. Same transition as `CANCEL`, including the attempt upsert (only for rows
// that are running after the lock; a claim may have committed between the snapshot and the
// lock).
// The status test is `status = 'queued' OR status = 'running'`, never `IN (..)`: only the OR
// form matches the partial indexes on queued and running rows (a BitmapOr over
// queue_tasks_due_idx and queue_tasks_expired_lease_idx). `IN` plans as a seq scan over every
// retained row (performance::bulk_cancel_batch_reads_only_live_rows).
// ponytail: no index finds live rows by task_name, because enqueue must not maintain a new index
// (docs/AdminUI.md §3.10); this reads every queued and running row and filters by name. Add one
// only if enqueue can afford it.
#[doc(hidden)]
pub const CANCEL_BATCH: &str = r#"
    WITH target AS (
        SELECT id, status, attempts, lock_token, locked_by FROM queue_tasks
        WHERE id <= $3 AND task_name = $1 AND ($2::text IS NULL OR queue = $2)
          AND (status = 'queued' OR status = 'running')
          AND NOT EXISTS (SELECT 1 FROM queue_admin_action_tasks m
                          WHERE m.action_id = $4 AND m.task_id = queue_tasks.id)
        LIMIT $5
        FOR UPDATE SKIP LOCKED
    ), closed AS (
        INSERT INTO queue_task_attempts
            (task_id, attempt_no, lock_token, worker_id, finished_at, outcome, error)
        SELECT id, attempts, lock_token, locked_by, statement_timestamp(), 'cancelled', 'cancelled by operator'
        FROM target WHERE status = 'running'
        ON CONFLICT (task_id, lock_token) DO UPDATE
        SET finished_at = excluded.finished_at, outcome = excluded.outcome,
            error = excluded.error
        WHERE queue_task_attempts.finished_at IS NULL
    ), cancelled AS (
        UPDATE queue_tasks t
        SET status = 'cancelled', locked_by = NULL, locked_until = NULL, completed_at = statement_timestamp(),
            updated_at = statement_timestamp()
        FROM target
        WHERE t.id = target.id
        RETURNING t.id
    ), member AS (
        INSERT INTO queue_admin_action_tasks (action_id, action, task_id)
        SELECT $4, 'cancel', id FROM cancelled
    )
    SELECT count(*) AS n FROM cancelled
"#;

// Binds: $1 task_name, $2 queue, $3 error mode and $4 its text (`ErrorFilter::binds`),
// $5 max_task_id, $6 action id, $7 batch size. Mirrors `queue::REPLAY`'s column updates. As in `CANCEL_BATCH`, the changed
// tasks become the action's membership rows and the anti-join skips the ones it already changed,
// so a replayed task that fails or is cancelled again is not replayed twice by one action.
#[doc(hidden)]
pub const REPLAY_BATCH: &str = r#"
    WITH target AS (
        SELECT id FROM queue_tasks
        WHERE id <= $5 AND task_name = $1 AND ($2::text IS NULL OR queue = $2)
          AND status IN ('dead', 'cancelled')
          AND ($3::text = 'any' OR ($3 = 'missing' AND last_error IS NULL)
               OR ($3 = 'exact' AND last_error = $4))
          AND NOT EXISTS (SELECT 1 FROM queue_admin_action_tasks m
                          WHERE m.action_id = $6 AND m.task_id = queue_tasks.id)
        LIMIT $7
        FOR UPDATE SKIP LOCKED
    ), replayed AS (
        UPDATE queue_tasks t
        SET status = 'queued', attempts = 0, due_at = statement_timestamp(), locked_by = NULL,
            lock_token = lock_token + 1, locked_until = NULL, last_error = NULL,
            completed_at = NULL, updated_at = statement_timestamp()
        FROM target
        WHERE t.id = target.id
        RETURNING t.id
    ), member AS (
        INSERT INTO queue_admin_action_tasks (action_id, action, task_id)
        SELECT $6, 'replay', id FROM replayed
    )
    SELECT count(*) AS n FROM (SELECT pg_notify('queue_available', '') FROM replayed) notified
"#;

async fn start_action(
    conn: &mut AsyncPgConnection,
    action: &BulkAction,
    filter: &BulkFilter,
    operator: &str,
    reason: &str,
) -> Result<NewAction, SchedulerError> {
    let (kind, (mode, text)) = match action {
        BulkAction::Replay { error } => {
            let (mode, text) = error.binds();
            (AdminActionKind::Replay, (Some(mode), text))
        }
        BulkAction::Cancel => (AdminActionKind::Cancel, (None, None)),
    };
    Ok(diesel::sql_query(
        "INSERT INTO queue_admin_actions (action, task_name, queue, error_filter, \
             last_error_filter, max_task_id, operator, reason) \
         SELECT $1, $2, $3, $4, $5, coalesce(max(id), 0), $6, $7 FROM queue_tasks \
         RETURNING id, max_task_id",
    )
    .bind::<sql_types::Text, _>(kind.as_str())
    .bind::<sql_types::Text, _>(&filter.task_name)
    .bind::<sql_types::Nullable<sql_types::Text>, _>(&filter.queue)
    .bind::<sql_types::Nullable<sql_types::Text>, _>(mode)
    .bind::<sql_types::Nullable<sql_types::Text>, _>(text)
    .bind::<sql_types::Text, _>(operator)
    .bind::<sql_types::Text, _>(reason)
    .get_result::<NewAction>(conn)
    .await?)
}

/// Batches of the action `act` until one comes back short, or `budget` is spent after a full
/// batch.
async fn run_batches(
    conn: &mut AsyncPgConnection,
    act: &NewAction,
    filter: &BulkFilter,
    action: &BulkAction,
    budget: Duration,
) -> Result<BulkOutcome, SchedulerError> {
    let started = tokio::time::Instant::now();
    let mut affected = 0;
    loop {
        let batch = match action {
            BulkAction::Replay { error } => {
                let (mode, text) = error.binds();
                diesel::sql_query(REPLAY_BATCH)
                    .bind::<sql_types::Text, _>(&filter.task_name)
                    .bind::<sql_types::Nullable<sql_types::Text>, _>(&filter.queue)
                    .bind::<sql_types::Text, _>(mode)
                    .bind::<sql_types::Nullable<sql_types::Text>, _>(text)
                    .bind::<sql_types::BigInt, _>(act.max_task_id)
                    .bind::<sql_types::BigInt, _>(act.id)
                    .bind::<sql_types::BigInt, _>(BULK_BATCH)
                    .get_result::<N>(conn)
                    .await?
            }
            BulkAction::Cancel => {
                diesel::sql_query(CANCEL_BATCH)
                    .bind::<sql_types::Text, _>(&filter.task_name)
                    .bind::<sql_types::Nullable<sql_types::Text>, _>(&filter.queue)
                    .bind::<sql_types::BigInt, _>(act.max_task_id)
                    .bind::<sql_types::BigInt, _>(act.id)
                    .bind::<sql_types::BigInt, _>(BULK_BATCH)
                    .get_result::<N>(conn)
                    .await?
            }
        };
        affected += batch.n;
        let stopped = if batch.n < BULK_BATCH {
            BulkStop::ShortBatch
        } else if started.elapsed() >= budget {
            BulkStop::BudgetSpent
        } else {
            continue;
        };
        return Ok(BulkOutcome {
            action_id: act.id,
            affected,
            stopped,
        });
    }
}

/// Cancel queued and running tasks of `filter`, in batches of [`BULK_BATCH`], for at most
/// `budget` (checked after each full batch). Only tasks that existed when it started are
/// touched, each at most once per action. Rows locked elsewhere are skipped, so concurrent
/// calls split the work.
pub async fn cancel_matching(
    conn: &mut AsyncPgConnection,
    filter: &BulkFilter,
    operator: &str,
    reason: &str,
    budget: Duration,
) -> Result<BulkOutcome, SchedulerError> {
    let action = BulkAction::Cancel;
    let act = start_action(conn, &action, filter, operator, reason).await?;
    run_batches(conn, &act, filter, &action, budget).await
}

/// Requeue dead and cancelled tasks of `filter` whose `last_error` matches `error`, same budget
/// rules as [`cancel_matching`]. Each non-empty batch notifies workers
/// in its own statement. After an error, earlier batches stay committed (each batch is its own
/// statement), and the action's count of changed tasks is the committed progress.
/// One action replays each task at most once: a batch skips the tasks its action already
/// changed, so a replayed task that fails or is cancelled again is left alone, by this call
/// and by a [`resume_bulk`] of it.
pub async fn replay_matching(
    conn: &mut AsyncPgConnection,
    filter: &BulkFilter,
    error: &ErrorFilter,
    operator: &str,
    reason: &str,
    budget: Duration,
) -> Result<BulkOutcome, SchedulerError> {
    let action = BulkAction::Replay {
        error: error.clone(),
    };
    let act = start_action(conn, &action, filter, operator, reason).await?;
    run_batches(conn, &act, filter, &action, budget).await
}

/// Continue the bulk replay or cancel `action_id` with more batches, under the same budget
/// rules: its own filter and id bound, and its own operator and reason (no new audit row).
/// `None` if no bulk replay or cancel has that id. The outcome's `affected` counts this call
/// only.
pub async fn resume_bulk(
    conn: &mut AsyncPgConnection,
    action_id: i64,
    budget: Duration,
) -> Result<Option<BulkOutcome>, SchedulerError> {
    // Only the target is read; a cap of 1 keeps its count to one index probe.
    let Some(AdminAction {
        target:
            AdminActionTarget::Bulk {
                action,
                task_name,
                queue,
                max_task_id,
            },
        ..
    }) = admin_action(conn, action_id, NonZeroU32::MIN).await?
    else {
        return Ok(None);
    };
    let act = NewAction {
        id: action_id,
        max_task_id,
    };
    let filter = BulkFilter { task_name, queue };
    run_batches(conn, &act, &filter, &action, budget)
        .await
        .map(Some)
}

// Binds: $1 task_name, $2 queue (NULL = any), $3 cap. Same selection, and the same OR form
// for the same reason, as `CANCEL_BATCH`.
#[doc(hidden)]
pub const COUNT_CANCELLABLE: &str = "\
    SELECT count(*) AS n FROM (SELECT 1 FROM queue_tasks \
    WHERE task_name = $1 AND ($2::text IS NULL OR queue = $2) \
      AND (status = 'queued' OR status = 'running') LIMIT $3) s";

// Binds: $1 task_name, $2 queue (NULL = any), $3 error mode and $4 its text, $5 cap. Same
// selection as `REPLAY_BATCH`.
#[doc(hidden)]
pub const COUNT_REPLAYABLE: &str = "\
    SELECT count(*) AS n FROM (SELECT 1 FROM queue_tasks \
    WHERE task_name = $1 AND ($2::text IS NULL OR queue = $2) \
      AND status IN ('dead', 'cancelled') \
      AND ($3::text = 'any' OR ($3 = 'missing' AND last_error IS NULL) \
           OR ($3 = 'exact' AND last_error = $4)) \
    LIMIT $5) s";

/// How many tasks `cancel_matching` would match, counting at most `cap`.
pub async fn count_cancellable(
    conn: &mut AsyncPgConnection,
    filter: &BulkFilter,
    cap: NonZeroU32,
) -> Result<i64, SchedulerError> {
    Ok(diesel::sql_query(COUNT_CANCELLABLE)
        .bind::<sql_types::Text, _>(&filter.task_name)
        .bind::<sql_types::Nullable<sql_types::Text>, _>(&filter.queue)
        .bind::<sql_types::BigInt, _>(i64::from(cap.get()))
        .get_result::<N>(conn)
        .await?
        .n)
}

/// How many tasks `replay_matching` would match, counting at most `cap`.
pub async fn count_replayable(
    conn: &mut AsyncPgConnection,
    filter: &BulkFilter,
    error: &ErrorFilter,
    cap: NonZeroU32,
) -> Result<i64, SchedulerError> {
    let (mode, text) = error.binds();
    Ok(diesel::sql_query(COUNT_REPLAYABLE)
        .bind::<sql_types::Text, _>(&filter.task_name)
        .bind::<sql_types::Nullable<sql_types::Text>, _>(&filter.queue)
        .bind::<sql_types::Text, _>(mode)
        .bind::<sql_types::Nullable<sql_types::Text>, _>(text)
        .bind::<sql_types::BigInt, _>(i64::from(cap.get()))
        .get_result::<N>(conn)
        .await?
        .n)
}

/// A paused queue.
#[derive(Debug, Clone, diesel::QueryableByName)]
pub struct QueuePause {
    /// The paused queue.
    #[diesel(sql_type = sql_types::Text)]
    pub queue: QueueName,
    /// Who paused it.
    #[diesel(sql_type = sql_types::Text)]
    pub operator: String,
    /// Why.
    #[diesel(sql_type = sql_types::Text)]
    pub reason: String,
    /// When.
    #[diesel(sql_type = sql_types::Timestamptz)]
    pub paused_at: DateTime<Utc>,
}

// The pause row references its audit action, so the action goes first. When the queue is already
// paused the row is not inserted and `pause_queue` rolls the action back.
const PAUSE_ACTION: &str = r#"
    INSERT INTO queue_admin_actions (action, queue, operator, reason)
    VALUES ('pause', $1, $2, $3) RETURNING id
"#;

const PAUSE: &str = r#"
    INSERT INTO queue_pauses (queue, action_id) VALUES ($1, $2)
    ON CONFLICT (queue) DO NOTHING RETURNING queue
"#;

const RESUME: &str = r#"
    WITH resumed AS (
        DELETE FROM queue_pauses WHERE queue = $1 RETURNING queue
    ), act AS (
        INSERT INTO queue_admin_actions (action, queue, operator, reason)
        SELECT 'resume', queue, $2, $3 FROM resumed
    )
    SELECT count(*) > 0 AS changed FROM (SELECT pg_notify('queue_available', '') FROM resumed) notified
"#;

#[derive(diesel::QueryableByName)]
struct IdRow {
    #[diesel(sql_type = sql_types::BigInt)]
    id: i64,
}

/// Why the pause transaction ended early: the sentinel rolls the audit row back.
enum PauseAbort {
    AlreadyPaused,
    Failed(SchedulerError),
}

impl From<diesel::result::Error> for PauseAbort {
    fn from(e: diesel::result::Error) -> Self {
        Self::Failed(e.into())
    }
}

#[derive(diesel::QueryableByName)]
struct ChangedRow {
    #[diesel(sql_type = sql_types::Bool)]
    changed: bool,
}

/// Stop every worker claiming from `queue`; running tasks finish. `true` if newly paused.
pub async fn pause_queue(
    conn: &mut AsyncPgConnection,
    queue: &QueueName,
    operator: &str,
    reason: &str,
) -> Result<bool, SchedulerError> {
    let paused = conn
        .transaction::<(), PauseAbort, _>(async |c| {
            let action = diesel::sql_query(PAUSE_ACTION)
                .bind::<sql_types::Text, _>(queue)
                .bind::<sql_types::Text, _>(operator)
                .bind::<sql_types::Text, _>(reason)
                .get_result::<IdRow>(c)
                .await?;
            let inserted = diesel::sql_query(PAUSE)
                .bind::<sql_types::Text, _>(queue)
                .bind::<sql_types::BigInt, _>(action.id)
                .execute(c)
                .await?;
            if inserted == 0 {
                return Err(PauseAbort::AlreadyPaused);
            }
            Ok(())
        })
        .await;
    match paused {
        Ok(()) => Ok(true),
        Err(PauseAbort::AlreadyPaused) => Ok(false),
        Err(PauseAbort::Failed(e)) => Err(e),
    }
}

/// Let workers claim from `queue` again and wake them. `true` if it was paused.
pub async fn resume_queue(
    conn: &mut AsyncPgConnection,
    queue: &QueueName,
    operator: &str,
    reason: &str,
) -> Result<bool, SchedulerError> {
    Ok(diesel::sql_query(RESUME)
        .bind::<sql_types::Text, _>(queue)
        .bind::<sql_types::Text, _>(operator)
        .bind::<sql_types::Text, _>(reason)
        .get_result::<ChangedRow>(conn)
        .await?
        .changed)
}

/// Every paused queue, ordered by queue.
pub async fn paused_queues(
    conn: &mut AsyncPgConnection,
) -> Result<Vec<QueuePause>, SchedulerError> {
    Ok(diesel::sql_query(
        "SELECT p.queue, a.operator, a.reason, a.at AS paused_at \
         FROM queue_pauses p JOIN queue_admin_actions a ON a.id = p.action_id \
         ORDER BY p.queue",
    )
    .get_results::<QueuePause>(conn)
    .await?)
}
