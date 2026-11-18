use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use diesel::OptionalExtension;
use diesel::pg::data_types::PgInterval;
use diesel::sql_types;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use tokio_util::sync::CancellationToken;

use crate::error::SchedulerError;
use crate::ids::LockToken;
use crate::ids::TaskId;
use crate::ids::TaskKey;
use crate::ids::WorkerId;
use crate::models::LeaseDuration;
use crate::models::OutboxCommand;
use crate::models::OutboxWrite;
use crate::models::RetryOutcome;
use crate::models::TaskLease;
use crate::models::TaskStatus;
use crate::models::pg_interval;

// Ownership guard shared by every worker-owned mutation. Binds: $1 task id, $2 worker, $3 token.
//
// Every transition first locks the row in its own statement (`lock_task_row`), then runs the
// guarded statement. PostgreSQL re-evaluates a statement's WHERE after a lock wait only when the
// blocker updated the row; a blocker that merely locked it and rolled back leaves the scan's
// earlier `clock_timestamp()` check stale. A new statement evaluates the expiry after the wait.
// The lock statement matches on ownership identity (id, worker, token) so a stale or fenced
// worker gets zero rows at once rather than waiting on, or deadlocking with, the current
// owner; expiry stays in the second statement because it must be read after the wait.
macro_rules! owned {
    () => {
        "WHERE id = $1 AND status = 'running' AND locked_by = $2 AND lock_token = $3 \
         AND locked_until > clock_timestamp()"
    };
}

const LOCK_ROW: &str = "SELECT id FROM queue_tasks \
    WHERE id = $1 AND locked_by = $2 AND lock_token = $3 FOR UPDATE";

const HEARTBEAT: &str = concat!(
    "UPDATE queue_tasks SET locked_until = clock_timestamp() + $4::interval, updated_at = now() ",
    owned!(),
    " RETURNING locked_until"
);

// Transaction-local (`is_local`), so it ends with the heartbeat's transaction.
const SET_LOCK_TIMEOUT: &str = "SELECT set_config('lock_timeout', $1, true)";

const LOCK_OWNED: &str = concat!("SELECT id FROM queue_tasks ", owned!(), " FOR UPDATE");

const RELEASE: &str = concat!(
    "UPDATE queue_tasks SET status = 'queued', attempts = attempts - 1, \
     locked_by = NULL, locked_until = NULL, updated_at = now() ",
    owned!(),
    " RETURNING id"
);

const COMPLETE: &str = concat!(
    "UPDATE queue_tasks SET status = 'done', locked_by = NULL, locked_until = NULL, \
     completed_at = now(), updated_at = now() ",
    owned!(),
    " RETURNING id"
);

const COMPLETE_WITH_OUTBOX: &str = concat!(
    "WITH completed AS (
        UPDATE queue_tasks SET status = 'done', locked_by = NULL, locked_until = NULL,
            completed_at = now(), updated_at = now() ",
    owned!(),
    " RETURNING id, task_key
    ), outbox_insert AS (
        INSERT INTO queue_outbox (task_key, effect_name, command_key, topic, payload)
        SELECT completed.task_key, $4, $5, $6, $7 FROM completed
        ON CONFLICT (task_key, effect_name) DO NOTHING
        RETURNING id
    )
    SELECT completed.id AS completed_task_id,
           completed.task_key AS completed_task_key,
           EXISTS (SELECT 1 FROM outbox_insert) AS inserted_outbox_row,
           EXISTS (SELECT 1 FROM queue_outbox o
                   WHERE o.task_key = completed.task_key AND o.effect_name = $4
                     AND o.command_key = $5 AND o.topic = $6 AND o.payload = $7) AS same_content
    FROM completed"
);

const RETRY: &str = concat!(
    "UPDATE queue_tasks SET
        status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
        due_at = CASE WHEN attempts >= max_attempts THEN due_at ELSE now() + $4::interval END,
        locked_by = NULL, locked_until = NULL, last_error = $5,
        completed_at = CASE WHEN attempts >= max_attempts THEN now() ELSE NULL END,
        updated_at = now() ",
    owned!(),
    " RETURNING status, due_at"
);

const FAIL: &str = concat!(
    "UPDATE queue_tasks SET status = 'dead', locked_by = NULL, locked_until = NULL,
        last_error = $4, completed_at = now(), updated_at = now() ",
    owned!(),
    " RETURNING id"
);

// Keyed by (task_id, lock_token): lock_token never repeats for a task, attempt_no can.
const CLOSE_ATTEMPT: &str = "UPDATE queue_task_attempts \
    SET finished_at = now(), outcome = $3, error = $4 \
    WHERE task_id = $1 AND lock_token = $2";

const COMPLETED_BY: &str = "SELECT EXISTS (SELECT 1 FROM queue_tasks \
    WHERE id = $1 AND status = 'done' AND lock_token = $2) AS done";

const LEASE_STATUS: &str = "SELECT status FROM queue_tasks WHERE id = $1 AND lock_token = $2";

#[derive(diesel::QueryableByName)]
struct StatusRow {
    #[diesel(sql_type = sql_types::Text)]
    status: String,
}

#[derive(diesel::QueryableByName)]
struct IdRow {
    #[diesel(sql_type = sql_types::BigInt)]
    #[allow(dead_code)]
    id: i64,
}

#[derive(diesel::QueryableByName)]
struct HeartbeatRow {
    #[diesel(sql_type = sql_types::Timestamptz)]
    locked_until: DateTime<Utc>,
}

#[derive(diesel::QueryableByName)]
struct OutboxRow {
    #[diesel(sql_type = sql_types::BigInt)]
    #[allow(dead_code)]
    completed_task_id: i64,
    #[diesel(sql_type = sql_types::Text)]
    completed_task_key: TaskKey,
    #[diesel(sql_type = sql_types::Bool)]
    inserted_outbox_row: bool,
    #[diesel(sql_type = sql_types::Bool)]
    same_content: bool,
}

#[derive(diesel::QueryableByName)]
struct RetryRow {
    #[diesel(sql_type = sql_types::Text)]
    status: String,
    #[diesel(sql_type = sql_types::Timestamptz)]
    due_at: DateTime<Utc>,
}

#[derive(diesel::QueryableByName)]
struct DoneRow {
    #[diesel(sql_type = sql_types::Bool)]
    done: bool,
}

// `sql_query(sql)` with the three ownership binds ($1..$3) applied.
macro_rules! owned_query {
    ($sql:expr, $lease:expr) => {
        diesel::sql_query($sql)
            .bind::<sql_types::BigInt, _>(&$lease.task_id)
            .bind::<sql_types::Text, _>(&$lease.worker_id)
            .bind::<sql_types::BigInt, _>(&$lease.lock_token)
    };
}

/// Lock the row by ownership identity (id, worker, token), waiting out any other holder. A stale
/// or fenced worker matches no row and is lost at once instead of queueing behind the current
/// owner; expiry is deliberately not part of the lock, it is checked by the guarded statement.
async fn lock_task_row(
    conn: &mut AsyncPgConnection,
    lease: &TaskLease,
) -> Result<(), SchedulerError> {
    lock_task_row_by(conn, lease.task_id, &lease.worker_id, lease.lock_token).await
}

async fn lock_task_row_by(
    conn: &mut AsyncPgConnection,
    task_id: TaskId,
    worker: &WorkerId,
    lock_token: LockToken,
) -> Result<(), SchedulerError> {
    diesel::sql_query(LOCK_ROW)
        .bind::<sql_types::BigInt, _>(&task_id)
        .bind::<sql_types::Text, _>(worker)
        .bind::<sql_types::BigInt, _>(&lock_token)
        .get_result::<IdRow>(conn)
        .await
        .map(|_| ())
        .map_err(|e| match e {
            diesel::result::Error::NotFound => SchedulerError::LostOwnership(task_id),
            e => e.into(),
        })
}

fn lost(lease: &TaskLease) -> SchedulerError {
    SchedulerError::LostOwnership(lease.task_id)
}

/// Task error text made safe to bind: PostgreSQL `TEXT` cannot hold NUL, and a bind failure would
/// roll back the whole transition, so each NUL becomes the two characters `\0`.
pub(crate) struct Diagnostic(String);

impl Diagnostic {
    pub(crate) fn new(s: &str) -> Self {
        Self(s.replace('\0', "\\0"))
    }
}

/// Close the attempt row for `lease`; exactly one must match or the caller's transaction rolls back.
async fn close_attempt(
    conn: &mut AsyncPgConnection,
    lease: &TaskLease,
    outcome: &str,
    error: Option<&Diagnostic>,
) -> Result<(), SchedulerError> {
    close_attempt_by(conn, lease.task_id, lease.lock_token, outcome, error).await
}

async fn close_attempt_by(
    conn: &mut AsyncPgConnection,
    task_id: TaskId,
    lock_token: LockToken,
    outcome: &str,
    error: Option<&Diagnostic>,
) -> Result<(), SchedulerError> {
    let n = diesel::sql_query(CLOSE_ATTEMPT)
        .bind::<sql_types::BigInt, _>(&task_id)
        .bind::<sql_types::BigInt, _>(&lock_token)
        .bind::<sql_types::Text, _>(outcome)
        .bind::<sql_types::Nullable<sql_types::Text>, _>(error.map(|d| d.0.as_str()))
        .execute(conn)
        .await?;
    if n == 1 {
        Ok(())
    } else {
        Err(SchedulerError::Invariant(
            "transition did not close exactly one attempt",
        ))
    }
}

/// Extend the lease to `duration` from now; returns the new `locked_until`.
///
/// It runs in a transaction, so don't cancel it mid-flight (`select!`, `timeout`): a cancelled
/// heartbeat can leave its connection idle in a transaction holding the row lock. Spawn it, as
/// the worker does.
pub async fn heartbeat(
    conn: &mut AsyncPgConnection,
    lease: &TaskLease,
    duration: LeaseDuration,
) -> Result<DateTime<Utc>, SchedulerError> {
    let duration = pg_interval(duration.get())?;
    conn.transaction::<_, SchedulerError, _>(async |c| {
        lock_task_row(c, lease).await?;
        extend_lease(c, lease, duration).await
    })
    .await
}

/// [`heartbeat`] for the worker's lease keeper, which spawns it. It waits at most `lock_wait`
/// for the row lock, so it cannot wait past the lease, and once it has the lock it renews
/// nothing if `stop` is cancelled: its dispatch has ended. Either way it rolls back; a lock
/// timeout is the database error, a cancelled `stop` is `LostOwnership`.
#[doc(hidden)]
pub async fn heartbeat_bounded(
    conn: &mut AsyncPgConnection,
    lease: &TaskLease,
    duration: LeaseDuration,
    lock_wait: Duration,
    stop: &CancellationToken,
) -> Result<DateTime<Utc>, SchedulerError> {
    let duration = pg_interval(duration.get())?;
    conn.transaction::<_, SchedulerError, _>(async |c| {
        diesel::sql_query(SET_LOCK_TIMEOUT)
            .bind::<sql_types::Text, _>(lock_timeout(lock_wait))
            .execute(c)
            .await?;
        lock_task_row(c, lease).await?;
        if stop.is_cancelled() {
            return Err(lost(lease));
        }
        extend_lease(c, lease, duration).await
    })
    .await
}

/// `lock_wait` as a `lock_timeout` value: whole milliseconds, at least 1 because 0 means no
/// timeout, and at most `i32::MAX` (about 24.8 days), the largest PostgreSQL accepts. A lease
/// longer than that still ends its keeper at the local deadline; only the spawned heartbeat's
/// lock wait is capped there.
fn lock_timeout(lock_wait: Duration) -> String {
    let ms = i32::try_from(lock_wait.as_millis())
        .unwrap_or(i32::MAX)
        .max(1);
    format!("{ms}ms")
}

/// The guarded `HEARTBEAT` update, after `lock_task_row` in the same transaction.
async fn extend_lease(
    c: &mut AsyncPgConnection,
    lease: &TaskLease,
    duration: PgInterval,
) -> Result<DateTime<Utc>, SchedulerError> {
    owned_query!(HEARTBEAT, lease)
        .bind::<sql_types::Interval, _>(duration)
        .get_result::<HeartbeatRow>(c)
        .await
        .map(|r| r.locked_until)
        .map_err(|e| match e {
            diesel::result::Error::NotFound => lost(lease),
            e => e.into(),
        })
}

/// `SELECT ... FOR UPDATE` on the owned row, for DB-contained handlers. Call inside the
/// caller's transaction; the reaper skips the row until it ends.
pub async fn lock_owned(
    conn: &mut AsyncPgConnection,
    lease: &TaskLease,
) -> Result<(), SchedulerError> {
    lock_task_row(conn, lease).await?;
    let rows = owned_query!(LOCK_OWNED, lease)
        .get_results::<IdRow>(conn)
        .await?;
    if rows.is_empty() {
        return Err(lost(lease));
    }
    Ok(())
}

/// Mark the task done and close its attempt.
pub async fn complete(
    conn: &mut AsyncPgConnection,
    lease: &TaskLease,
) -> Result<(), SchedulerError> {
    conn.transaction::<_, SchedulerError, _>(async |c| {
        lock_task_row(c, lease).await?;
        let rows = owned_query!(COMPLETE, lease)
            .get_results::<IdRow>(c)
            .await?;
        if rows.is_empty() {
            return Err(lost(lease));
        }
        close_attempt(c, lease, "done", None).await
    })
    .await
}

/// Return a running task to `queued` without counting this attempt (worker shutdown). `due_at` is
/// unchanged, so it is due at once and keeps its place.
pub async fn release(
    conn: &mut AsyncPgConnection,
    lease: &TaskLease,
) -> Result<(), SchedulerError> {
    conn.transaction::<_, SchedulerError, _>(async |c| {
        lock_task_row(c, lease).await?;
        let rows = owned_query!(RELEASE, lease).get_results::<IdRow>(c).await?;
        if rows.is_empty() {
            return Err(lost(lease));
        }
        close_attempt(c, lease, "released", None).await
    })
    .await
}

/// Complete and insert `command` into the outbox in one statement. Returns
/// [`OutboxWrite::Inserted`] for a new row and [`OutboxWrite::AlreadyPresent`] when
/// `(task_key, effect_name)` already exists with identical content. Existing content that
/// differs fails with [`SchedulerError::OutboxConflict`] and rolls back the completion.
///
/// Propagating `OutboxConflict` with `?` into a `TaskError` fails the task permanently.
pub async fn complete_with_outbox(
    conn: &mut AsyncPgConnection,
    lease: &TaskLease,
    command: &OutboxCommand,
) -> Result<OutboxWrite, SchedulerError> {
    conn.transaction::<_, SchedulerError, _>(async |c| {
        lock_task_row(c, lease).await?;
        let rows = owned_query!(COMPLETE_WITH_OUTBOX, lease)
            .bind::<sql_types::Text, _>(&command.effect_name)
            .bind::<sql_types::Text, _>(&command.command_key)
            .bind::<sql_types::Text, _>(&command.topic)
            .bind::<sql_types::Jsonb, _>(&command.payload)
            .get_results::<OutboxRow>(c)
            .await?;
        let Some(row) = rows.into_iter().next() else {
            return Err(lost(lease));
        };
        if row.inserted_outbox_row {
            close_attempt(c, lease, "done", None).await?;
            Ok(OutboxWrite::Inserted)
        } else if row.same_content {
            close_attempt(c, lease, "done", None).await?;
            Ok(OutboxWrite::AlreadyPresent)
        } else {
            Err(SchedulerError::OutboxConflict {
                task_key: row.completed_task_key,
                effect_name: command.effect_name.clone(),
            })
        }
    })
    .await
}

/// Requeue after `delay` of PostgreSQL time, or mark dead when attempts are exhausted.
pub async fn retry(
    conn: &mut AsyncPgConnection,
    lease: &TaskLease,
    error: &str,
    delay: Duration,
) -> Result<RetryOutcome, SchedulerError> {
    let delay = pg_interval(delay)?;
    let error = Diagnostic::new(error);
    conn.transaction::<_, SchedulerError, _>(async |c| {
        lock_task_row(c, lease).await?;
        let rows = owned_query!(RETRY, lease)
            .bind::<sql_types::Interval, _>(delay)
            .bind::<sql_types::Text, _>(&error.0)
            .get_results::<RetryRow>(c)
            .await?;
        let Some(row) = rows.into_iter().next() else {
            return Err(lost(lease));
        };
        let dead = row.status == "dead";
        close_attempt(c, lease, if dead { "dead" } else { "retry" }, Some(&error)).await?;
        Ok(if dead {
            RetryOutcome::Dead
        } else {
            RetryOutcome::Queued { due_at: row.due_at }
        })
    })
    .await
}

/// Mark the task dead regardless of remaining attempts.
pub async fn fail_permanently(
    conn: &mut AsyncPgConnection,
    lease: &TaskLease,
    error: &str,
) -> Result<(), SchedulerError> {
    fail_owned(
        conn,
        lease.task_id,
        &lease.worker_id,
        lease.lock_token,
        error,
    )
    .await
}

/// [`fail_permanently`] keyed by the ownership identity alone (the queries read no attempt number).
pub(crate) async fn fail_owned(
    conn: &mut AsyncPgConnection,
    task_id: TaskId,
    worker: &WorkerId,
    lock_token: LockToken,
    error: &str,
) -> Result<(), SchedulerError> {
    let error = Diagnostic::new(error);
    conn.transaction::<_, SchedulerError, _>(async |c| {
        lock_task_row_by(c, task_id, worker, lock_token).await?;
        let rows = diesel::sql_query(FAIL)
            .bind::<sql_types::BigInt, _>(&task_id)
            .bind::<sql_types::Text, _>(worker)
            .bind::<sql_types::BigInt, _>(&lock_token)
            .bind::<sql_types::Text, _>(&error.0)
            .get_results::<IdRow>(c)
            .await?;
        if rows.is_empty() {
            return Err(SchedulerError::LostOwnership(task_id));
        }
        close_attempt_by(c, task_id, lock_token, "dead", Some(&error)).await
    })
    .await
}

/// Whether this exact lease's token completed the task (for recovering from an ambiguous commit).
pub async fn completed_by(
    conn: &mut AsyncPgConnection,
    lease: &TaskLease,
) -> Result<bool, SchedulerError> {
    Ok(diesel::sql_query(COMPLETED_BY)
        .bind::<sql_types::BigInt, _>(&lease.task_id)
        .bind::<sql_types::BigInt, _>(&lease.lock_token)
        .get_result::<DoneRow>(conn)
        .await?
        .done)
}

/// The task's status while `lease`'s token still holds it, else `None` (claimed again, replayed,
/// or deleted). After a transition reports `LostOwnership`, `Done` means the handler completed
/// the task itself and `Cancelled` that an operator cancelled it.
pub async fn lease_status(
    conn: &mut AsyncPgConnection,
    lease: &TaskLease,
) -> Result<Option<TaskStatus>, SchedulerError> {
    diesel::sql_query(LEASE_STATUS)
        .bind::<sql_types::BigInt, _>(&lease.task_id)
        .bind::<sql_types::BigInt, _>(&lease.lock_token)
        .get_result::<StatusRow>(conn)
        .await
        .optional()?
        .map(|r| {
            r.status
                .parse()
                .map_err(|_| SchedulerError::Invariant("unknown task status"))
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::Diagnostic;
    use super::lock_timeout;

    #[test]
    fn diagnostic_escapes_every_nul() {
        assert_eq!(Diagnostic::new("a\0b\0").0, "a\\0b\\0");
    }

    #[test]
    fn lock_timeout_is_whole_ms_at_least_one_at_most_i32_max() {
        assert_eq!(lock_timeout(Duration::ZERO), "1ms");
        assert_eq!(lock_timeout(Duration::from_micros(1_999)), "1ms");
        assert_eq!(lock_timeout(Duration::from_millis(100)), "100ms");
        assert_eq!(lock_timeout(Duration::MAX), "2147483647ms");
    }
}
