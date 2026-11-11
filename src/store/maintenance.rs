use std::num::NonZeroUsize;
use std::time::Duration;

use diesel::sql_types;
use diesel_async::{AsyncPgConnection, RunQueryDsl};

use crate::error::SchedulerError;
use crate::models::{CleanupSummary, ReapSummary, Retention, pg_interval};

// §12 reaper, plus closing each reaped task's open attempt. One statement, so both commit together.
const REAP: &str = r#"
    WITH expired AS (
        SELECT id FROM queue_tasks
        WHERE status = 'running' AND locked_until <= clock_timestamp()
        ORDER BY locked_until ASC, id ASC
        LIMIT $1
        FOR UPDATE SKIP LOCKED
    ), reaped AS (
        UPDATE queue_tasks AS q
        SET status = CASE WHEN q.attempts >= q.max_attempts THEN 'dead' ELSE 'queued' END,
            due_at = CASE WHEN q.attempts >= q.max_attempts THEN q.due_at
                          ELSE now() + $2::interval END,
            locked_by = NULL, locked_until = NULL,
            last_error = 'worker lease expired',
            completed_at = CASE WHEN q.attempts >= q.max_attempts THEN now() ELSE NULL END,
            updated_at = now()
        FROM expired AS e WHERE q.id = e.id
        RETURNING q.id, q.lock_token, q.status
    ), closed AS (
        UPDATE queue_task_attempts a
        SET finished_at = now(), outcome = 'lease_expired', error = 'worker lease expired'
        FROM reaped r
        WHERE a.task_id = r.id AND a.lock_token = r.lock_token AND a.finished_at IS NULL
        RETURNING a.task_id
    )
    SELECT status FROM reaped
"#;

// §20, one statement per terminal status: each is an ordered range scan of
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

fn limit(n: NonZeroUsize) -> i64 {
    i64::try_from(n.get()).unwrap_or(i64::MAX)
}

/// Recover up to `batch` tasks whose lease expired: requeue after `crash_retry_delay`, or mark
/// dead when attempts are exhausted. Safe to run from several processes (SKIP LOCKED).
pub async fn reap_expired(
    conn: &mut AsyncPgConnection,
    batch: NonZeroUsize,
    crash_retry_delay: Duration,
) -> Result<ReapSummary, SchedulerError> {
    let rows = diesel::sql_query(REAP)
        .bind::<sql_types::BigInt, _>(limit(batch))
        .bind::<sql_types::Interval, _>(pg_interval(crash_retry_delay))
        .get_results::<StatusRow>(conn)
        .await?;
    let dead = rows.iter().filter(|r| r.status == "dead").count();
    Ok(ReapSummary {
        requeued: rows.len() - dead,
        dead,
    })
}

/// Delete one bounded batch of expired terminal tasks (`done` first, then `dead`, at most
/// `retention.batch_size` in all) and one of published outbox rows. Call repeatedly until
/// both counts are below `retention.batch_size`.
pub async fn cleanup(
    conn: &mut AsyncPgConnection,
    retention: &Retention,
) -> Result<CleanupSummary, SchedulerError> {
    let batch = retention.batch_size;
    let done = diesel::sql_query(CLEANUP_DONE)
        .bind::<sql_types::Interval, _>(pg_interval(retention.done))
        .bind::<sql_types::BigInt, _>(limit(batch))
        .execute(conn)
        .await?;
    // Both statuses share one budget: a call deletes at most `batch_size` task rows.
    let dead = match NonZeroUsize::new(batch.get().saturating_sub(done)) {
        Some(left) => {
            diesel::sql_query(CLEANUP_DEAD)
                .bind::<sql_types::Interval, _>(pg_interval(retention.dead))
                .bind::<sql_types::BigInt, _>(limit(left))
                .execute(conn)
                .await?
        }
        None => 0,
    };
    let outbox = diesel::sql_query(CLEANUP_OUTBOX)
        .bind::<sql_types::Interval, _>(pg_interval(retention.outbox_published))
        .bind::<sql_types::BigInt, _>(limit(batch))
        .execute(conn)
        .await?;
    Ok(CleanupSummary {
        tasks: done + dead,
        outbox,
    })
}
