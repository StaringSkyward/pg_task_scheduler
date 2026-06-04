use diesel_async::{AsyncPgConnection, RunQueryDsl};

use crate::error::SchedulerError;

const REAP: &str = "\
    INSERT INTO scheduler_run_outcomes (run_id, outcome, last_error) \
    SELECT r.id, 'failed'::run_outcome, 'lease expired; max attempts exhausted' \
    FROM scheduler_runs r \
    JOIN scheduler_jobs j ON j.id = r.job_id \
    JOIN scheduler_run_leases l ON l.run_id = r.id \
    LEFT JOIN scheduler_run_outcomes o ON o.run_id = r.id \
    WHERE o.run_id IS NULL \
      AND l.lease_expires_at <= now() \
      AND r.attempt_count >= j.max_attempts";

/// Dead-letter every run whose lease expired and whose attempts are exhausted.
/// The AFTER INSERT trigger clears each lease. Returns the count failed.
pub async fn reap_expired(conn: &mut AsyncPgConnection) -> Result<usize, SchedulerError> {
    Ok(diesel::sql_query(REAP).execute(conn).await?)
}
