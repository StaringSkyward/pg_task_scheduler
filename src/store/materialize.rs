use chrono::DateTime;
use chrono::SecondsFormat;
use chrono::Utc;
use diesel::sql_types;
use diesel_async::AnsiTransactionManager;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::TransactionManager;

use crate::cron::CronExpression;
use crate::error::SchedulerError;
use crate::ids::IdentifierError;
use crate::ids::JobId;
use crate::ids::QueueName;
use crate::ids::SCHEDULE_KEY_PREFIX;
use crate::ids::TaskKey;
use crate::ids::TaskName;
use crate::models::MaxAttempts;

#[derive(diesel::QueryableByName)]
struct DueJob {
    #[diesel(sql_type = sql_types::Uuid)]
    id: uuid::Uuid,
    #[diesel(sql_type = sql_types::Text)]
    cron_expression: String,
    #[diesel(sql_type = sql_types::Timestamptz)]
    db_now: DateTime<Utc>,
    #[diesel(sql_type = sql_types::Text)]
    task_name: String,
    #[diesel(sql_type = sql_types::Integer)]
    max_attempts: i32,
    #[diesel(sql_type = sql_types::Text)]
    queue: String,
}

#[derive(diesel::QueryableByName)]
struct Isolation {
    #[diesel(sql_type = sql_types::Text)]
    isolation: String,
}

/// A locked job's cursor and pause, as [`RECHECK`] reads them.
#[derive(diesel::QueryableByName)]
struct Current {
    #[diesel(sql_type = sql_types::Timestamptz)]
    next_run_at: DateTime<Utc>,
    #[diesel(sql_type = sql_types::Bool)]
    paused: bool,
}

/// Finds the earliest due, unpaused cursor through `scheduler_job_cursors_due_idx` and locks its
/// job's row, skipping one another pass holds. Every cursor write happens under its job's row lock
/// (`FOR UPDATE` on `scheduler_jobs`), so the lock order is job → cursor → pause, and the cursor
/// stays put while we hold the job. This statement's snapshot of it may already be stale, so
/// `RECHECK` re-reads it. The task columns stay raw so `parse_due` can pause a job with a bad
/// value instead of a decode error ending the pass. `job_args` is not read at all: `INSERT_TASK`
/// copies it in SQL.
#[doc(hidden)]
pub const SELECT_DUE: &str = "\
    SELECT j.id, j.cron_expression, statement_timestamp() AS db_now, j.task_name, \
           j.max_attempts, j.queue \
    FROM scheduler_job_cursors c JOIN scheduler_jobs j ON j.id = c.job_id \
    WHERE c.next_run_at <= statement_timestamp() \
      AND NOT EXISTS (SELECT 1 FROM scheduler_job_pauses p WHERE p.job_id = c.job_id) \
    ORDER BY c.next_run_at ASC FOR UPDATE OF j SKIP LOCKED LIMIT 1";
/// Re-reads the cursor and the pause once [`SELECT_DUE`] holds the job. `SELECT_DUE` reads both in
/// its statement snapshot, and neither a cursor write (`jobs::reschedule`, another pass's advance)
/// nor a pause (`jobs::pause` takes only `FOR KEY SHARE`) changes the job row, so locking it fires
/// no EvalPlanQual recheck: a reschedule committed between that snapshot and the lock would be
/// materialized from the stale slot and then overwritten, and a pause would still get one more
/// occurrence. Under READ COMMITTED, which [`materialize_due_jobs`] enforces, this new statement
/// sees both; a later one blocks on our `FOR UPDATE` until we commit. `next_run_at` decodes
/// safely: the `queue_timestamptz` domain keeps it in chrono's range.
const RECHECK: &str = "\
    SELECT c.next_run_at, \
           EXISTS (SELECT 1 FROM scheduler_job_pauses p WHERE p.job_id = c.job_id) AS paused \
    FROM scheduler_job_cursors c WHERE c.job_id = $1";
const INSERT_TASK: &str = "\
    INSERT INTO queue_tasks (task_key, task_name, payload, due_at, max_attempts, queue) \
    SELECT $1, $2, j.job_args, $3, $4, $5 FROM scheduler_jobs j WHERE j.id = $6 \
    ON CONFLICT (task_key) DO NOTHING";
const NOTIFY: &str = "SELECT pg_notify('queue_available', '')";
const ADVANCE: &str = "UPDATE scheduler_job_cursors SET next_run_at = $1 WHERE job_id = $2";
const PAUSE: &str = "INSERT INTO scheduler_job_pauses (job_id) VALUES ($1) ON CONFLICT DO NOTHING";
/// A transaction cut off mid-way (shutdown drops the future) stays open on its pooled
/// connection, holding the job's row lock; the server ends that session after 30 s idle.
const IDLE_GUARD: &str = "SELECT set_config('idle_in_transaction_session_timeout', '30s', true)";
/// Bounds each lock wait and statement at 30 s, so a stalled relation lock (a migration holding
/// `scheduler_jobs`, say) fails the pass instead of holding its connection. A stricter non-zero
/// host value is kept (`transitions`' `SET_TIMEOUTS` pattern).
const TIMEOUTS: &str = "SELECT set_config(name, \
         CASE WHEN current_setting(name)::interval > interval '0' \
                   AND current_setting(name)::interval < interval '30s' \
              THEN current_setting(name) ELSE '30s' END, true) \
     FROM unnest(ARRAY['lock_timeout', 'statement_timeout']) AS name";
const ISOLATION: &str = "SELECT current_setting('transaction_isolation') AS isolation";

/// The idempotency key of one occurrence of a schedule: re-materializing the same
/// `(job, scheduled_for)` hits `queue_tasks.task_key` and inserts nothing.
pub(crate) fn schedule_task_key(
    id: JobId,
    scheduled_for: DateTime<Utc>,
) -> Result<TaskKey, IdentifierError> {
    TaskKey::try_from(format!(
        "{}{}:{}",
        SCHEDULE_KEY_PREFIX,
        id.0,
        scheduled_for.to_rfc3339_opts(SecondsFormat::Micros, true)
    ))
}

/// Materialize due jobs in one bounded pass of at most 100, one transaction per job.
/// Returns the number of tasks inserted: an occurrence already present as a task, or a
/// job paused (by an operator, or below for a job-local defect), counts nothing. Each
/// due job the pass reaches uses one of the 100, inserted or not, and a larger backlog
/// is left for later passes.
///
/// Each job is handled in its own transaction so a single bad row cannot block
/// the rest. For a due job, the occurrence for its *current* `next_run_at` (the
/// possibly-missed slot) is inserted and `next_run_at` is advanced to the next
/// future occurrence — both in the same transaction, so pod death cannot turn
/// into a missed run (`run_once` misfire).
///
/// A job-local defect pauses its job and logs the error: a stored cron that fails to
/// parse or has no next occurrence, or an invalid task name, queue or `max_attempts`
/// (all validated on create, so only direct SQL stores one, and a task name or queue only
/// once its identifier CHECK is dropped by hand). Such a job is never silently advanced.
/// Its `job_args` are copied in SQL, never decoded, and only database errors end the pass.
///
/// It requires, and enforces, READ COMMITTED, which its cursor and pause recheck relies on. Each
/// job's transaction begins at READ COMMITTED whatever the session default. On a connection already
/// in a transaction each job runs in a savepoint of it, so that transaction must be READ
/// COMMITTED (READ UNCOMMITTED also passes, as PostgreSQL runs it as READ COMMITTED) and its
/// settings are left alone: at any other level this returns [`SchedulerError::Config`] and materializes
/// nothing. (diesel-async cannot see a transaction opened with raw SQL; don't call this in one.)
///
/// Otherwise each job's transaction sets `lock_timeout` and `statement_timeout` to 30 s, keeping
/// a stricter non-zero session value, and `idle_in_transaction_session_timeout` to 30 s, so a
/// held lock on a schedule table fails the pass within 30 s.
pub async fn materialize_due_jobs(conn: &mut AsyncPgConnection) -> Result<usize, SchedulerError> {
    // diesel-async cannot begin a READ COMMITTED transaction inside another one.
    let nested = AnsiTransactionManager::transaction_manager_status_mut(conn)
        .transaction_depth()?
        .is_some();
    if nested {
        let isolation = diesel::sql_query(ISOLATION)
            .get_result::<Isolation>(conn)
            .await?
            .isolation;
        if isolation != "read committed" && isolation != "read uncommitted" {
            return Err(SchedulerError::Config(format!(
                "materialize_due_jobs needs READ COMMITTED; the caller's transaction is {isolation}"
            )));
        }
    }
    let mut inserted = 0;
    for _ in 0..100 {
        let one = if nested {
            conn.transaction(async |c| materialize_one(c, true).await)
                .await?
        } else {
            conn.build_transaction()
                .read_committed()
                .run(async |c| materialize_one(c, false).await)
                .await?
        };
        match one {
            Some(n) => inserted += n,
            None => break,
        }
    }
    Ok(inserted)
}

/// What a due job's row yields once every job-local column has parsed.
struct Occurrence {
    next: DateTime<Utc>,
    task_name: TaskName,
    queue: QueueName,
    max_attempts: MaxAttempts,
}

/// Parse a due job's stored values; `Err` describes the first defect.
fn parse_due(job: &DueJob) -> Result<Occurrence, String> {
    // Base the next run on the DB clock, never the Rust clock.
    let next = CronExpression::parse(&job.cron_expression)
        .and_then(|cron| cron.next_after(job.db_now))
        .map_err(|e| e.to_string())?;
    Ok(Occurrence {
        next,
        task_name: TaskName::try_from(job.task_name.as_str())
            .map_err(|e| format!("task name: {e}"))?,
        queue: QueueName::try_from(job.queue.as_str()).map_err(|e| format!("queue: {e}"))?,
        max_attempts: MaxAttempts::from_db_i32(job.max_attempts).map_err(|e| e.to_string())?,
    })
}

/// Process the earliest due job, inside its transaction: `None` when none is due, else the tasks
/// inserted (0 or 1).
///
/// `nested` skips `IDLE_GUARD` and `TIMEOUTS`: their transaction-local settings survive
/// `RELEASE SAVEPOINT` and would apply to the caller's transaction.
async fn materialize_one(
    c: &mut AsyncPgConnection,
    nested: bool,
) -> Result<Option<usize>, SchedulerError> {
    if !nested {
        diesel::sql_query(IDLE_GUARD).execute(c).await?;
        diesel::sql_query(TIMEOUTS).execute(c).await?;
    }
    let Some(job) = diesel::sql_query(SELECT_DUE)
        .get_results::<DueJob>(c)
        .await?
        .into_iter()
        .next()
    else {
        return Ok(None);
    };
    let Some(current) = diesel::sql_query(RECHECK)
        .bind::<sql_types::Uuid, _>(job.id)
        .get_results::<Current>(c)
        .await?
        .into_iter()
        .next()
    else {
        return Ok(Some(0)); // the cursor is gone: nothing to materialize
    };
    // Paused, or the cursor moved past `db_now`, since SELECT_DUE's snapshot.
    if current.paused || current.next_run_at > job.db_now {
        return Ok(Some(0));
    }

    let occurrence = match parse_due(&job) {
        Ok(occurrence) => occurrence,
        Err(e) => {
            tracing::error!(job_id = %job.id, error = %e, "unschedulable job; pausing it");
            diesel::sql_query(PAUSE)
                .bind::<sql_types::Uuid, _>(job.id)
                .execute(c)
                .await?;
            return Ok(Some(0));
        }
    };
    // run_once misfire: the occurrence is for the OLD next_run_at.
    let key = schedule_task_key(JobId(job.id), current.next_run_at)?;
    let inserted = diesel::sql_query(INSERT_TASK)
        .bind::<sql_types::Text, _>(key.as_str())
        .bind::<sql_types::Text, _>(occurrence.task_name.as_str())
        .bind::<sql_types::Timestamptz, _>(current.next_run_at)
        .bind::<sql_types::Integer, _>(occurrence.max_attempts.to_i32())
        .bind::<sql_types::Text, _>(occurrence.queue.as_str())
        .bind::<sql_types::Uuid, _>(job.id)
        .execute(c)
        .await?;
    if inserted == 1 {
        diesel::sql_query(NOTIFY).execute(c).await?;
    }
    diesel::sql_query(ADVANCE)
        .bind::<sql_types::Timestamptz, _>(occurrence.next)
        .bind::<sql_types::Uuid, _>(job.id)
        .execute(c)
        .await?;
    Ok(Some(inserted))
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn schedule_task_key_is_stable() {
        let at =
            Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap() + chrono::Duration::microseconds(6);
        let key = schedule_task_key(JobId(uuid::Uuid::nil()), at).unwrap();
        assert_eq!(
            key.as_str(),
            "schedule:00000000-0000-0000-0000-000000000000:2026-01-02T03:04:05.000006Z"
        );
    }
}
