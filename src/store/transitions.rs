use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use diesel::OptionalExtension;
use diesel::sql_types;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;

use crate::error::SchedulerError;
use crate::models::OutboxCommand;
use crate::models::RetryOutcome;
use crate::models::TaskLease;
use crate::models::TaskStatus;
use crate::models::pg_interval;

// Ownership guard shared by every worker-owned mutation. Binds: $1 task id, $2 worker, $3 token.
macro_rules! owned {
    () => {
        "WHERE id = $1 AND status = 'running' AND locked_by = $2 AND lock_token = $3 \
         AND locked_until > clock_timestamp()"
    };
}

const HEARTBEAT: &str = concat!(
    "UPDATE queue_tasks SET locked_until = clock_timestamp() + $4::interval, updated_at = now() ",
    owned!(),
    " RETURNING locked_until"
);

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
           EXISTS (SELECT 1 FROM outbox_insert) AS inserted_outbox_row
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
    #[diesel(sql_type = sql_types::Bool)]
    inserted_outbox_row: bool,
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

fn lost(lease: &TaskLease) -> SchedulerError {
    SchedulerError::LostOwnership(lease.task_id)
}

/// Close the attempt row for `lease`; exactly one must match or the caller's transaction rolls back.
async fn close_attempt(
    conn: &mut AsyncPgConnection,
    lease: &TaskLease,
    outcome: &str,
    error: Option<&str>,
) -> Result<(), SchedulerError> {
    let n = diesel::sql_query(CLOSE_ATTEMPT)
        .bind::<sql_types::BigInt, _>(&lease.task_id)
        .bind::<sql_types::BigInt, _>(&lease.lock_token)
        .bind::<sql_types::Text, _>(outcome)
        .bind::<sql_types::Nullable<sql_types::Text>, _>(error)
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
pub async fn heartbeat(
    conn: &mut AsyncPgConnection,
    lease: &TaskLease,
    duration: Duration,
) -> Result<DateTime<Utc>, SchedulerError> {
    owned_query!(HEARTBEAT, lease)
        .bind::<sql_types::Interval, _>(pg_interval(duration))
        .get_result::<HeartbeatRow>(conn)
        .await
        .map(|r| r.locked_until)
        .map_err(|e| match e {
            diesel::result::Error::NotFound => lost(lease),
            e => e.into(),
        })
}

/// `SELECT ... FOR UPDATE` on the owned row, for DB-contained handlers (§10.2). Call inside the
/// caller's transaction; the reaper skips the row until it ends.
pub async fn lock_owned(
    conn: &mut AsyncPgConnection,
    lease: &TaskLease,
) -> Result<(), SchedulerError> {
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
        let rows = owned_query!(RELEASE, lease).get_results::<IdRow>(c).await?;
        if rows.is_empty() {
            return Err(lost(lease));
        }
        close_attempt(c, lease, "released", None).await
    })
    .await
}

/// Complete and insert `command` into the outbox in one statement. `true` if a new outbox row was
/// written, `false` if `(task_key, effect_name)` already existed.
pub async fn complete_with_outbox(
    conn: &mut AsyncPgConnection,
    lease: &TaskLease,
    command: &OutboxCommand,
) -> Result<bool, SchedulerError> {
    conn.transaction::<_, SchedulerError, _>(async |c| {
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
        close_attempt(c, lease, "done", None).await?;
        Ok(row.inserted_outbox_row)
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
    conn.transaction::<_, SchedulerError, _>(async |c| {
        let rows = owned_query!(RETRY, lease)
            .bind::<sql_types::Interval, _>(pg_interval(delay))
            .bind::<sql_types::Text, _>(error)
            .get_results::<RetryRow>(c)
            .await?;
        let Some(row) = rows.into_iter().next() else {
            return Err(lost(lease));
        };
        let dead = row.status == "dead";
        close_attempt(c, lease, if dead { "dead" } else { "retry" }, Some(error)).await?;
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
    conn.transaction::<_, SchedulerError, _>(async |c| {
        let rows = owned_query!(FAIL, lease)
            .bind::<sql_types::Text, _>(error)
            .get_results::<IdRow>(c)
            .await?;
        if rows.is_empty() {
            return Err(lost(lease));
        }
        close_attempt(c, lease, "dead", Some(error)).await
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
