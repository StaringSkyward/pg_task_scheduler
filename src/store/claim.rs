use std::num::{NonZeroU32, NonZeroUsize};

use chrono::{DateTime, Utc};
use diesel::sql_types;
use diesel_async::{AsyncPgConnection, RunQueryDsl};

use crate::error::SchedulerError;
use crate::ids::{JobId, JobName, LeaseToken, RunId, WorkerId};
use crate::models::{ClaimedRun, LeaseDuration};

#[derive(diesel::QueryableByName)]
struct ClaimedRow {
    #[diesel(sql_type = sql_types::Uuid)]
    run_id: RunId,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Uuid>)]
    job_id: Option<JobId>,
    #[diesel(sql_type = sql_types::Text)]
    job_name: JobName,
    #[diesel(sql_type = sql_types::Jsonb)]
    job_args: serde_json::Value,
    #[diesel(sql_type = sql_types::Timestamptz)]
    scheduled_for: DateTime<Utc>,
    #[diesel(sql_type = sql_types::Integer)]
    attempt: i32,
    #[diesel(sql_type = sql_types::Uuid)]
    lease_token: LeaseToken,
    #[diesel(sql_type = sql_types::Timestamptz)]
    lease_expires_at: DateTime<Utc>,
    #[diesel(sql_type = sql_types::Interval)]
    lease_duration: diesel::pg::data_types::PgInterval,
}

const CLAIM_BATCH: &str = r#"
    WITH candidates AS (
        SELECT id
        FROM scheduler_runs
        WHERE state = 'pending'::scheduler_run_state
          AND available_at <= now()
          AND job_name = ANY($1)
        ORDER BY priority DESC, available_at ASC, id ASC
        FOR UPDATE SKIP LOCKED
        LIMIT $2
    ), claimed AS (
        UPDATE scheduler_runs AS r
        SET state = 'running'::scheduler_run_state,
            attempt_count = r.attempt_count + 1,
            worker_id = $3,
            lease_token = gen_random_uuid(),
            started_at = clock_timestamp(),
            lease_expires_at = clock_timestamp() + r.lease_duration,
            updated_at = clock_timestamp()
        FROM candidates AS c
        WHERE r.id = c.id
        RETURNING r.id AS run_id, r.job_id, r.job_name, r.job_args, r.scheduled_for,
                  r.attempt_count AS attempt, r.lease_token, r.lease_expires_at,
                  r.lease_duration, r.worker_id, r.started_at
    ), recorded AS (
        INSERT INTO scheduler_run_attempts (
            run_id, attempt_number, worker_id, lease_token, started_at, lease_expires_at
        )
        SELECT run_id, attempt, worker_id, lease_token, started_at, lease_expires_at
        FROM claimed
        RETURNING run_id
    )
    SELECT run_id, job_id, job_name, job_args, scheduled_for, attempt,
           lease_token, lease_expires_at, lease_duration
    FROM claimed
    WHERE EXISTS (SELECT 1 FROM recorded WHERE recorded.run_id = claimed.run_id)
    ORDER BY scheduled_for, run_id
"#;

/// Atomically claims up to `limit` pending tasks in one PostgreSQL statement.
pub async fn claim_batch(
    conn: &mut AsyncPgConnection,
    worker_id: &WorkerId,
    names: &[String],
    limit: NonZeroUsize,
) -> Result<Vec<ClaimedRun>, SchedulerError> {
    let limit = i64::try_from(limit.get())
        .map_err(|_| SchedulerError::Config("claim batch is too large".into()))?;
    let rows = diesel::sql_query(CLAIM_BATCH)
        .bind::<sql_types::Array<sql_types::Text>, _>(names)
        .bind::<sql_types::BigInt, _>(limit)
        .bind::<sql_types::Text, _>(worker_id)
        .get_results::<ClaimedRow>(conn)
        .await?;

    rows.into_iter()
        .map(|row| {
            let attempt = u32::try_from(row.attempt)
                .ok()
                .and_then(NonZeroU32::new)
                .ok_or(SchedulerError::Invariant(
                    "attempt_count must be positive after claim",
                ))?;
            let lease_duration =
                LeaseDuration::from_pg_interval(row.lease_duration).map_err(|_| {
                    SchedulerError::Invariant("claimed task has invalid lease duration")
                })?;
            Ok(ClaimedRun {
                run_id: row.run_id,
                job_id: row.job_id,
                job_name: row.job_name,
                job_args: row.job_args,
                scheduled_for: row.scheduled_for,
                attempt,
                lease_token: row.lease_token,
                lease_expires_at: row.lease_expires_at,
                lease_duration,
            })
        })
        .collect()
}

pub async fn claim_one(
    conn: &mut AsyncPgConnection,
    worker_id: &WorkerId,
    names: &[String],
) -> Result<Option<ClaimedRun>, SchedulerError> {
    let mut claimed = claim_batch(conn, worker_id, names, NonZeroUsize::new(1).unwrap()).await?;
    Ok(claimed.pop())
}
