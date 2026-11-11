use chrono::{DateTime, SecondsFormat, Utc};
use diesel::sql_types;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

use crate::cron::CronExpression;
use crate::error::SchedulerError;
use crate::ids::{IdentifierError, JobId, TaskKey};

#[derive(diesel::QueryableByName)]
struct DueJob {
    #[diesel(sql_type = sql_types::Uuid)]
    id: uuid::Uuid,
    #[diesel(sql_type = sql_types::Timestamptz)]
    next_run_at: DateTime<Utc>,
    #[diesel(sql_type = sql_types::Text)]
    cron_expression: String,
    #[diesel(sql_type = sql_types::Timestamptz)]
    db_now: DateTime<Utc>,
    #[diesel(sql_type = sql_types::Text)]
    name: String,
    #[diesel(sql_type = sql_types::Jsonb)]
    job_args: serde_json::Value,
    #[diesel(sql_type = sql_types::Integer)]
    max_attempts: i32,
}

const SELECT_DUE: &str = "\
    SELECT id, next_run_at, cron_expression, now() AS db_now, name, job_args, \
           max_attempts \
    FROM scheduler_jobs \
    WHERE next_run_at <= now() AND is_paused = false \
    ORDER BY next_run_at ASC FOR UPDATE SKIP LOCKED LIMIT 1";
const INSERT_TASK: &str = "\
    INSERT INTO queue_tasks (task_key, task_name, payload, due_at, max_attempts) \
    VALUES ($1, $2, $3, $4, $5) \
    ON CONFLICT (task_key) DO NOTHING";
const NOTIFY: &str = "SELECT pg_notify('queue_available', '')";
const ADVANCE: &str =
    "UPDATE scheduler_jobs SET next_run_at = $1, updated_at = now() WHERE id = $2";
const PAUSE: &str = "UPDATE scheduler_jobs SET is_paused = true, updated_at = now() WHERE id = $1";

/// The idempotency key of one occurrence of a schedule: re-materializing the same
/// `(job, scheduled_for)` hits `queue_tasks.task_key` and inserts nothing.
pub(crate) fn schedule_task_key(
    id: JobId,
    scheduled_for: DateTime<Utc>,
) -> Result<TaskKey, IdentifierError> {
    TaskKey::try_from(format!(
        "schedule:{}:{}",
        id.0,
        scheduled_for.to_rfc3339_opts(SecondsFormat::Micros, true)
    ))
}

/// Materialize all due jobs, one transaction per job. Returns the number of
/// tasks inserted: an occurrence already present as a task, or a job paused for
/// a corrupt cron, counts nothing.
///
/// Each job is handled in its own transaction so a single bad row cannot block
/// the rest. For a due job, the occurrence for its *current* `next_run_at` (the
/// possibly-missed slot) is inserted and `next_run_at` is advanced to the next
/// future occurrence — both in the same transaction, so pod death cannot turn
/// into a missed run (`run_once` misfire).
///
/// If the stored cron fails to re-parse (corruption — it was validated on
/// create), the job is paused and the error logged; it is never silently
/// advanced.
pub async fn materialize_due_jobs(conn: &mut AsyncPgConnection) -> Result<usize, SchedulerError> {
    let mut inserted = 0;
    for _ in 0..100 {
        match materialize_one(conn).await? {
            Some(n) => inserted += n,
            None => break,
        }
    }
    Ok(inserted)
}

/// Process the earliest due job: `None` when none is due, else the tasks inserted (0 or 1).
async fn materialize_one(conn: &mut AsyncPgConnection) -> Result<Option<usize>, SchedulerError> {
    conn.transaction::<_, SchedulerError, _>(async |c| {
        let Some(job) = diesel::sql_query(SELECT_DUE)
            .get_results::<DueJob>(c)
            .await?
            .into_iter()
            .next()
        else {
            return Ok(None);
        };

        let inserted = match CronExpression::parse(&job.cron_expression) {
            Ok(cron) => {
                // Base the next run on the DB clock, never the Rust clock.
                let next = cron.next_after(job.db_now)?;
                // run_once misfire: the occurrence is for the OLD next_run_at.
                let key = schedule_task_key(JobId(job.id), job.next_run_at)?;
                let inserted = diesel::sql_query(INSERT_TASK)
                    .bind::<sql_types::Text, _>(key.as_str())
                    .bind::<sql_types::Text, _>(job.name)
                    .bind::<sql_types::Jsonb, _>(job.job_args)
                    .bind::<sql_types::Timestamptz, _>(job.next_run_at)
                    .bind::<sql_types::Integer, _>(job.max_attempts)
                    .execute(c)
                    .await?;
                if inserted == 1 {
                    diesel::sql_query(NOTIFY).execute(c).await?;
                }
                diesel::sql_query(ADVANCE)
                    .bind::<sql_types::Timestamptz, _>(next)
                    .bind::<sql_types::Uuid, _>(job.id)
                    .execute(c)
                    .await?;
                inserted
            }
            Err(e) => {
                tracing::error!(job_id = %job.id, error = %e, "corrupt cron; pausing job");
                diesel::sql_query(PAUSE)
                    .bind::<sql_types::Uuid, _>(job.id)
                    .execute(c)
                    .await?;
                0
            }
        };
        Ok(Some(inserted))
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

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
