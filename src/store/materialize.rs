use chrono::{DateTime, Utc};
use diesel::sql_types;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use scoped_futures::ScopedFutureExt;

use crate::cron::CronExpression;
use crate::error::SchedulerError;

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
    #[diesel(sql_type = sql_types::Interval)]
    lease_duration: diesel::pg::data_types::PgInterval,
    #[diesel(sql_type = sql_types::Interval)]
    retry_backoff: diesel::pg::data_types::PgInterval,
}

const SELECT_DUE: &str = "\
    SELECT id, next_run_at, cron_expression, now() AS db_now, name, job_args, \
           max_attempts, lease_duration, retry_backoff \
    FROM scheduler_jobs \
    WHERE next_run_at <= now() AND is_paused = false \
    ORDER BY next_run_at ASC FOR UPDATE SKIP LOCKED LIMIT 1";
const INSERT_RUN: &str = "\
    INSERT INTO scheduler_runs (\
        job_id, job_name, job_args, scheduled_for, available_at, max_attempts,\
        lease_duration, retry_backoff\
    ) VALUES ($1, $2, $3, $4, $4, $5, $6, $7) \
    ON CONFLICT (job_id, scheduled_for) WHERE job_id IS NOT NULL DO NOTHING";
const ADVANCE: &str =
    "UPDATE scheduler_jobs SET next_run_at = $1, updated_at = now() WHERE id = $2";
const PAUSE: &str = "UPDATE scheduler_jobs SET is_paused = true, updated_at = now() WHERE id = $1";

enum Tick {
    Processed,
    Idle,
}

/// Materialize all due jobs, one transaction per job. Returns the number of
/// jobs processed.
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
    let mut count = 0;
    while count < 100 {
        match materialize_one(conn).await? {
            Tick::Idle => break,
            Tick::Processed => count += 1,
        }
    }
    Ok(count)
}

async fn materialize_one(conn: &mut AsyncPgConnection) -> Result<Tick, SchedulerError> {
    conn.transaction::<Tick, SchedulerError, _>(|c| {
        async move {
            let Some(job) = diesel::sql_query(SELECT_DUE)
                .get_results::<DueJob>(c)
                .await?
                .into_iter()
                .next()
            else {
                return Ok(Tick::Idle);
            };

            match CronExpression::parse(&job.cron_expression) {
                Ok(cron) => {
                    // Base the next run on the DB clock, never the Rust clock.
                    let next = cron.next_after(job.db_now)?;
                    // run_once misfire: the occurrence is for the OLD next_run_at.
                    diesel::sql_query(INSERT_RUN)
                        .bind::<sql_types::Uuid, _>(job.id)
                        .bind::<sql_types::Text, _>(job.name)
                        .bind::<sql_types::Jsonb, _>(job.job_args)
                        .bind::<sql_types::Timestamptz, _>(job.next_run_at)
                        .bind::<sql_types::Integer, _>(job.max_attempts)
                        .bind::<sql_types::Interval, _>(job.lease_duration)
                        .bind::<sql_types::Interval, _>(job.retry_backoff)
                        .execute(c)
                        .await?;
                    diesel::sql_query(ADVANCE)
                        .bind::<sql_types::Timestamptz, _>(next)
                        .bind::<sql_types::Uuid, _>(job.id)
                        .execute(c)
                        .await?;
                }
                Err(e) => {
                    tracing::error!(job_id = %job.id, error = %e, "corrupt cron; pausing job");
                    diesel::sql_query(PAUSE)
                        .bind::<sql_types::Uuid, _>(job.id)
                        .execute(c)
                        .await?;
                }
            }
            Ok(Tick::Processed)
        }
        .scope_boxed()
    })
    .await
}
