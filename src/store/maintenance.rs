use std::num::NonZeroU32;
use std::time::Duration;

use diesel::sql_types;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;

use crate::error::SchedulerError;
use crate::models::CleanupSummary;
use crate::models::ReapSummary;
use crate::models::Retention;
use crate::models::TaskStatus;
use crate::models::pg_interval;

// Reaper: expired leases go back to queued after the crash-retry delay, or to dead when no attempts
// remain, and each reaped task's open attempt closes. One statement, so both commit together.
// `expired` returns the locked, newest version of each row, so its lock_token is the lease being
// reaped. The attempt is closed by upsert, not UPDATE: a reclaim that committed after this
// statement's snapshot, but before the row lock, inserted an attempt the snapshot cannot see;
// ON CONFLICT still finds it. A running task always has its attempt (the claim inserts both at
// once), and a CHECK keeps `attempts >= 1` on every running row, so the `attempt_no =
// attempts` insert is always valid. It only repairs an imported row missing its attempt row,
// as `CANCEL` does.
// `statement_timestamp()` is stable within the statement, so the cutoff is an index bound on
// queue_tasks_expired_lease_idx, and it is never later than the `clock_timestamp()` an owner's
// own expiry check reads. A lease that expires during the statement waits for the next pass.
#[doc(hidden)]
pub const REAP: &str = r#"
    WITH expired AS (
        SELECT id, attempts, lock_token, locked_by FROM queue_tasks
        WHERE status = 'running' AND locked_until <= statement_timestamp()
        ORDER BY locked_until ASC, id ASC
        LIMIT $1
        FOR UPDATE SKIP LOCKED
    ), reaped AS (
        UPDATE queue_tasks AS q
        SET status = CASE WHEN q.attempts >= q.max_attempts THEN 'dead' ELSE 'queued' END,
            due_at = CASE WHEN q.attempts >= q.max_attempts THEN q.due_at
                          ELSE statement_timestamp() + $2::interval END,
            locked_by = NULL, locked_until = NULL,
            last_error = 'worker lease expired',
            completed_at = CASE WHEN q.attempts >= q.max_attempts THEN statement_timestamp() ELSE NULL END,
            updated_at = statement_timestamp()
        FROM expired AS e WHERE q.id = e.id
        RETURNING q.id, q.status
    ), closed AS (
        INSERT INTO queue_task_attempts
            (task_id, attempt_no, lock_token, worker_id, finished_at, outcome, error)
        SELECT e.id, e.attempts, e.lock_token, e.locked_by, statement_timestamp(), 'lease_expired',
               'worker lease expired'
        FROM expired AS e JOIN reaped AS r ON r.id = e.id
        ON CONFLICT (task_id, lock_token) DO UPDATE
        SET finished_at = excluded.finished_at, outcome = excluded.outcome,
            error = excluded.error
        WHERE queue_task_attempts.finished_at IS NULL
    )
    SELECT status FROM reaped
"#;

// Retention cleanup, one statement per terminal status: each is an ordered range scan of
// queue_tasks_terminal_cleanup_idx with `completed_at < cutoff` as its index condition,
// stopping at the LIMIT. One OR across both statuses cannot be one range: the planner
// walks the whole index filtering every retained row, or bitmap-scans and sorts every
// eligible row before the LIMIT.
const CLEANUP_DONE: &str = r#"
    WITH doomed AS (
        SELECT id FROM queue_tasks
        WHERE status = 'done' AND completed_at < now() - $1::interval
        ORDER BY completed_at ASC, id ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
    )
    DELETE FROM queue_tasks AS q USING doomed AS d WHERE q.id = d.id
"#;

const CLEANUP_DEAD: &str = r#"
    WITH doomed AS (
        SELECT id FROM queue_tasks
        WHERE status = 'dead' AND completed_at < now() - $1::interval
        ORDER BY completed_at ASC, id ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
    )
    DELETE FROM queue_tasks AS q USING doomed AS d WHERE q.id = d.id
"#;

#[doc(hidden)]
pub const CLEANUP_CANCELLED: &str = r#"
    WITH doomed AS (
        SELECT id FROM queue_tasks
        WHERE status = 'cancelled' AND completed_at < now() - $1::interval
        ORDER BY completed_at ASC, id ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
    )
    DELETE FROM queue_tasks AS q USING doomed AS d WHERE q.id = d.id
"#;

const CLEANUP_OUTBOX: &str = r#"
    WITH doomed AS (
        SELECT id FROM queue_outbox
        WHERE published_at < now() - $1::interval
        ORDER BY published_at ASC, id ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
    )
    DELETE FROM queue_outbox AS o USING doomed AS d WHERE o.id = d.id
"#;

#[derive(diesel::QueryableByName)]
struct StatusRow {
    #[diesel(sql_type = sql_types::Text)]
    status: String,
}

/// Recover up to `batch` tasks whose lease expired: requeue after `crash_retry_delay`, or mark
/// dead when attempts are exhausted. Safe to run from several processes (SKIP LOCKED).
pub async fn reap_expired(
    conn: &mut AsyncPgConnection,
    batch: NonZeroU32,
    crash_retry_delay: Duration,
) -> Result<ReapSummary, SchedulerError> {
    let rows = diesel::sql_query(REAP)
        .bind::<sql_types::BigInt, _>(i64::from(batch.get()))
        .bind::<sql_types::Interval, _>(pg_interval(crash_retry_delay)?)
        .get_results::<StatusRow>(conn)
        .await?;
    let (mut requeued, mut dead) = (0, 0);
    for row in &rows {
        match row.status.parse::<TaskStatus>() {
            Ok(TaskStatus::Queued) => requeued += 1,
            Ok(TaskStatus::Dead) => dead += 1,
            _ => {
                return Err(SchedulerError::Invariant(
                    "reap returned an unexpected status",
                ));
            }
        }
    }
    Ok(ReapSummary { requeued, dead })
}

/// Delete one bounded batch of expired terminal tasks per status (`done`, `dead` and `cancelled`:
/// at most `retention.batch_size` each) and one of published outbox rows. Call repeatedly until
/// every count is below `retention.batch_size`. Retention over 5 years is rejected with
/// `Config` before any delete. The batches commit separately, so a database error mid-call
/// leaves earlier batches committed.
pub async fn cleanup(
    conn: &mut AsyncPgConnection,
    retention: &Retention,
) -> Result<CleanupSummary, SchedulerError> {
    retention.check()?;
    let batch = retention.batch_size;
    // Convert every interval first so invalid input is rejected before any delete.
    let done_age = pg_interval(retention.done)?;
    let dead_age = pg_interval(retention.dead)?;
    let outbox_age = pg_interval(retention.outbox_published)?;
    let done = diesel::sql_query(CLEANUP_DONE)
        .bind::<sql_types::Interval, _>(done_age)
        .bind::<sql_types::BigInt, _>(i64::from(batch.get()))
        .execute(conn)
        .await?;
    let dead = diesel::sql_query(CLEANUP_DEAD)
        .bind::<sql_types::Interval, _>(dead_age)
        .bind::<sql_types::BigInt, _>(i64::from(batch.get()))
        .execute(conn)
        .await?;
    let cancelled = diesel::sql_query(CLEANUP_CANCELLED)
        .bind::<sql_types::Interval, _>(dead_age)
        .bind::<sql_types::BigInt, _>(i64::from(batch.get()))
        .execute(conn)
        .await?;
    let outbox = diesel::sql_query(CLEANUP_OUTBOX)
        .bind::<sql_types::Interval, _>(outbox_age)
        .bind::<sql_types::BigInt, _>(i64::from(batch.get()))
        .execute(conn)
        .await?;
    Ok(CleanupSummary {
        done,
        dead,
        cancelled,
        outbox,
    })
}
