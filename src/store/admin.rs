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

/// Rows one bulk batch locks and changes.
pub const BULK_BATCH: i64 = 500;
/// How long a bulk call keeps starting batches.
pub const BULK_BUDGET: Duration = Duration::from_secs(10);

/// Which tasks a bulk action targets.
#[derive(Debug, Clone)]
pub struct BulkFilter {
    pub task_name: TaskName,
    /// Any queue when `None`.
    pub queue: Option<QueueName>,
}

/// What one bulk call did. `more_remain`: the budget ran out after a full batch; call again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BulkOutcome {
    pub action_id: i64,
    pub affected: i64,
    pub more_remain: bool,
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
// One statement per batch: lock, transition, and bump the action's count together, so the
// count never drifts from what was applied. Same transition as `CANCEL`, including the
// attempt upsert (only for rows that are running after the lock; a claim may have committed
// between the snapshot and the lock).
// ponytail: the queued-row scan by task_name has no index, because §3 forbids an index
// enqueue must maintain; it scans the queue's queued rows. Add one only if enqueue can afford it.
const CANCEL_BATCH: &str = r#"
    WITH target AS (
        SELECT id, status, attempts, lock_token, locked_by FROM queue_tasks
        WHERE id <= $3 AND task_name = $1 AND ($2::text IS NULL OR queue = $2)
          AND status IN ('queued', 'running')
        LIMIT $5
        FOR UPDATE SKIP LOCKED
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
            updated_at = now(), admin_action_id = $4
        FROM target
        WHERE t.id = target.id
        RETURNING t.id
    ), bump AS (
        UPDATE queue_admin_actions SET affected = affected + (SELECT count(*) FROM cancelled)
        WHERE id = $4
    )
    SELECT count(*) AS n FROM cancelled
"#;

// Binds: $1 task_name, $2 queue, $3 last_error (NULL = any), $4 max_task_id, $5 action id,
// $6 batch size. Mirrors `queue::REPLAY`'s column updates.
#[doc(hidden)]
pub const REPLAY_BATCH: &str = r#"
    WITH target AS (
        SELECT id FROM queue_tasks
        WHERE id <= $4 AND task_name = $1 AND ($2::text IS NULL OR queue = $2)
          AND status IN ('dead', 'cancelled') AND ($3::text IS NULL OR last_error = $3)
        LIMIT $6
        FOR UPDATE SKIP LOCKED
    ), replayed AS (
        UPDATE queue_tasks t
        SET status = 'queued', attempts = 0, due_at = now(), locked_by = NULL,
            lock_token = lock_token + 1, locked_until = NULL, last_error = NULL,
            completed_at = NULL, updated_at = now(), admin_action_id = $5
        FROM target
        WHERE t.id = target.id
        RETURNING t.id
    ), bump AS (
        UPDATE queue_admin_actions SET affected = affected + (SELECT count(*) FROM replayed)
        WHERE id = $5
    )
    SELECT count(*) AS n FROM replayed
"#;

async fn start_action(
    conn: &mut AsyncPgConnection,
    action: AdminActionKind,
    filter: &BulkFilter,
    last_error: Option<&str>,
    operator: &str,
    reason: &str,
) -> Result<NewAction, SchedulerError> {
    Ok(diesel::sql_query(
        "INSERT INTO queue_admin_actions \
             (action, task_name, queue, last_error_filter, max_task_id, operator, reason) \
         SELECT $1, $2, $3, $4, coalesce(max(id), 0), $5, $6 FROM queue_tasks \
         RETURNING id, max_task_id",
    )
    .bind::<sql_types::Text, _>(action.as_str())
    .bind::<sql_types::Text, _>(&filter.task_name)
    .bind::<sql_types::Nullable<sql_types::Text>, _>(&filter.queue)
    .bind::<sql_types::Nullable<sql_types::Text>, _>(last_error)
    .bind::<sql_types::Text, _>(operator)
    .bind::<sql_types::Text, _>(reason)
    .get_result::<NewAction>(conn)
    .await?)
}

/// Cancel queued and running tasks of `filter`, in batches of [`BULK_BATCH`], for at most
/// `budget` (checked after each full batch). Only tasks that existed when it started are
/// touched. Rows locked elsewhere are skipped, so concurrent calls split the work.
pub async fn cancel_matching(
    conn: &mut AsyncPgConnection,
    filter: &BulkFilter,
    operator: &str,
    reason: &str,
    budget: Duration,
) -> Result<BulkOutcome, SchedulerError> {
    let act = start_action(
        conn,
        AdminActionKind::Cancel,
        filter,
        None,
        operator,
        reason,
    )
    .await?;
    let started = tokio::time::Instant::now();
    let mut affected = 0;
    loop {
        let n = diesel::sql_query(CANCEL_BATCH)
            .bind::<sql_types::Text, _>(&filter.task_name)
            .bind::<sql_types::Nullable<sql_types::Text>, _>(&filter.queue)
            .bind::<sql_types::BigInt, _>(act.max_task_id)
            .bind::<sql_types::BigInt, _>(act.id)
            .bind::<sql_types::BigInt, _>(BULK_BATCH)
            .get_result::<N>(conn)
            .await?
            .n;
        affected += n;
        if n < BULK_BATCH {
            return Ok(BulkOutcome {
                action_id: act.id,
                affected,
                more_remain: false,
            });
        }
        if started.elapsed() >= budget {
            return Ok(BulkOutcome {
                action_id: act.id,
                affected,
                more_remain: true,
            });
        }
    }
}

/// Requeue dead and cancelled tasks of `filter` (with `last_error` equal to the given text,
/// when given), same budget rules as [`cancel_matching`]. Each non-empty batch notifies workers.
pub async fn replay_matching(
    conn: &mut AsyncPgConnection,
    filter: &BulkFilter,
    last_error: Option<&str>,
    operator: &str,
    reason: &str,
    budget: Duration,
) -> Result<BulkOutcome, SchedulerError> {
    let act = start_action(
        conn,
        AdminActionKind::Replay,
        filter,
        last_error,
        operator,
        reason,
    )
    .await?;
    let started = tokio::time::Instant::now();
    let mut affected = 0;
    loop {
        let n = diesel::sql_query(REPLAY_BATCH)
            .bind::<sql_types::Text, _>(&filter.task_name)
            .bind::<sql_types::Nullable<sql_types::Text>, _>(&filter.queue)
            .bind::<sql_types::Nullable<sql_types::Text>, _>(last_error)
            .bind::<sql_types::BigInt, _>(act.max_task_id)
            .bind::<sql_types::BigInt, _>(act.id)
            .bind::<sql_types::BigInt, _>(BULK_BATCH)
            .get_result::<N>(conn)
            .await?
            .n;
        if n > 0 {
            diesel::sql_query("SELECT pg_notify('queue_available', '')")
                .execute(conn)
                .await?;
        }
        affected += n;
        if n < BULK_BATCH {
            return Ok(BulkOutcome {
                action_id: act.id,
                affected,
                more_remain: false,
            });
        }
        if started.elapsed() >= budget {
            return Ok(BulkOutcome {
                action_id: act.id,
                affected,
                more_remain: true,
            });
        }
    }
}

/// How many tasks `cancel_matching` would match, counting at most `cap`.
pub async fn count_cancellable(
    conn: &mut AsyncPgConnection,
    filter: &BulkFilter,
    cap: i64,
) -> Result<i64, SchedulerError> {
    Ok(diesel::sql_query(
        "SELECT count(*) AS n FROM (SELECT 1 FROM queue_tasks \
         WHERE task_name = $1 AND ($2::text IS NULL OR queue = $2) \
           AND status IN ('queued', 'running') LIMIT $3) s",
    )
    .bind::<sql_types::Text, _>(&filter.task_name)
    .bind::<sql_types::Nullable<sql_types::Text>, _>(&filter.queue)
    .bind::<sql_types::BigInt, _>(cap)
    .get_result::<N>(conn)
    .await?
    .n)
}

/// How many tasks `replay_matching` would match, counting at most `cap`.
pub async fn count_replayable(
    conn: &mut AsyncPgConnection,
    filter: &BulkFilter,
    last_error: Option<&str>,
    cap: i64,
) -> Result<i64, SchedulerError> {
    Ok(diesel::sql_query(
        "SELECT count(*) AS n FROM (SELECT 1 FROM queue_tasks \
         WHERE task_name = $1 AND ($2::text IS NULL OR queue = $2) \
           AND status IN ('dead', 'cancelled') AND ($3::text IS NULL OR last_error = $3) \
         LIMIT $4) s",
    )
    .bind::<sql_types::Text, _>(&filter.task_name)
    .bind::<sql_types::Nullable<sql_types::Text>, _>(&filter.queue)
    .bind::<sql_types::Nullable<sql_types::Text>, _>(last_error)
    .bind::<sql_types::BigInt, _>(cap)
    .get_result::<N>(conn)
    .await?
    .n)
}
