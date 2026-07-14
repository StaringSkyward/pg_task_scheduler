use std::num::NonZeroUsize;

use diesel::sql_types;
use diesel_async::{AsyncPgConnection, RunQueryDsl};

use crate::error::SchedulerError;
use crate::models::StoredRunState;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoverySummary {
    pub requeued: usize,
    pub failed: usize,
}

#[derive(diesel::QueryableByName)]
struct RecoveredRow {
    #[diesel(sql_type = crate::schema::sql_types::SchedulerRunState)]
    state: StoredRunState,
}

const RECOVER: &str = r#"
    WITH candidates AS (
        SELECT id, lease_token, attempt_count, max_attempts
        FROM scheduler_runs
        WHERE state = 'running'::scheduler_run_state
          AND lease_expires_at <= clock_timestamp()
        ORDER BY lease_expires_at, id
        FOR UPDATE SKIP LOCKED
        LIMIT $1
    ), recorded AS (
        UPDATE scheduler_run_attempts AS a
        SET finished_at = clock_timestamp(), outcome = 'expired'::scheduler_attempt_outcome,
            error = 'lease expired'
        FROM candidates AS c
        WHERE a.lease_token = c.lease_token AND a.outcome IS NULL
        RETURNING a.run_id
    ), recovered AS (
        UPDATE scheduler_runs AS r
        SET state = CASE
                WHEN c.attempt_count < c.max_attempts THEN 'pending'::scheduler_run_state
                ELSE 'failed'::scheduler_run_state
            END,
            available_at = CASE
                WHEN c.attempt_count < c.max_attempts THEN clock_timestamp()
                ELSE r.available_at
            END,
            worker_id = NULL, lease_token = NULL, lease_expires_at = NULL, started_at = NULL,
            finished_at = CASE
                WHEN c.attempt_count < c.max_attempts THEN NULL
                ELSE clock_timestamp()
            END,
            last_error = CASE
                WHEN c.attempt_count < c.max_attempts THEN NULL
                ELSE 'lease expired; max attempts exhausted'
            END,
            updated_at = clock_timestamp()
        FROM candidates AS c
        WHERE r.id = c.id
          AND EXISTS (SELECT 1 FROM recorded WHERE recorded.run_id = r.id)
        RETURNING r.state
    )
    SELECT state FROM recovered
"#;

pub async fn recover_expired(
    conn: &mut AsyncPgConnection,
    limit: NonZeroUsize,
) -> Result<RecoverySummary, SchedulerError> {
    let limit = i64::try_from(limit.get())
        .map_err(|_| SchedulerError::Config("recovery batch is too large".into()))?;
    let rows = diesel::sql_query(RECOVER)
        .bind::<sql_types::BigInt, _>(limit)
        .get_results::<RecoveredRow>(conn)
        .await?;
    let mut summary = RecoverySummary::default();
    for row in rows {
        match row.state {
            StoredRunState::Pending => summary.requeued += 1,
            StoredRunState::Failed => summary.failed += 1,
            _ => {
                return Err(SchedulerError::Invariant(
                    "expired recovery produced invalid state",
                ));
            }
        }
    }
    Ok(summary)
}

/// Compatibility helper: recovers a bounded batch and returns how many tasks
/// became terminal failures.
pub async fn reap_expired(conn: &mut AsyncPgConnection) -> Result<usize, SchedulerError> {
    Ok(recover_expired(conn, NonZeroUsize::new(1_000).unwrap())
        .await?
        .failed)
}
