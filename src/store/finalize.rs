use chrono::{DateTime, Utc};
use diesel::sql_types;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use scoped_futures::ScopedFutureExt;

use crate::error::SchedulerError;
use crate::ids::{LeaseToken, RunId};
use crate::models::{
    FailureOutcome, FinalizeOutcome, Outcome, RenewalOutcome, StoredAttemptOutcome, StoredRunState,
};

const FINALIZE: &str = r#"
    UPDATE scheduler_runs
    SET state = $3, worker_id = NULL, lease_token = NULL, lease_expires_at = NULL,
        started_at = NULL, finished_at = clock_timestamp(), last_error = $4,
        updated_at = clock_timestamp()
    WHERE id = $1 AND state = 'running'::scheduler_run_state AND lease_token = $2
      AND lease_expires_at > clock_timestamp()
    RETURNING id
"#;

const RECORD_ATTEMPT: &str = r#"
    UPDATE scheduler_run_attempts
    SET finished_at = clock_timestamp(), outcome = $2, error = $3
    WHERE lease_token = $1 AND outcome IS NULL
"#;

const IS_TERMINAL: &str = r#"
    SELECT EXISTS (
        SELECT 1 FROM scheduler_runs
        WHERE id = $1 AND state IN (
            'completed'::scheduler_run_state, 'failed'::scheduler_run_state,
            'cancelled'::scheduler_run_state
        )
    ) AS terminal
"#;

const FAIL: &str = r#"
    UPDATE scheduler_runs
    SET state = CASE
            WHEN $3 AND attempt_count < max_attempts THEN 'pending'::scheduler_run_state
            ELSE 'failed'::scheduler_run_state
        END,
        available_at = CASE
            WHEN $3 AND attempt_count < max_attempts THEN clock_timestamp() + retry_backoff
            ELSE available_at
        END,
        worker_id = NULL, lease_token = NULL, lease_expires_at = NULL, started_at = NULL,
        finished_at = CASE
            WHEN $3 AND attempt_count < max_attempts THEN NULL
            ELSE clock_timestamp()
        END,
        last_error = CASE
            WHEN $3 AND attempt_count < max_attempts THEN NULL
            ELSE $4
        END,
        updated_at = clock_timestamp()
    WHERE id = $1 AND state = 'running'::scheduler_run_state AND lease_token = $2
      AND lease_expires_at > clock_timestamp()
    RETURNING state, available_at
"#;

const RENEW: &str = r#"
    UPDATE scheduler_runs
    SET lease_expires_at = clock_timestamp() + lease_duration, updated_at = clock_timestamp()
    WHERE id = $1 AND state = 'running'::scheduler_run_state AND lease_token = $2
      AND lease_expires_at > clock_timestamp()
    RETURNING lease_expires_at
"#;

const RENEW_ATTEMPT: &str = r#"
    UPDATE scheduler_run_attempts SET lease_expires_at = $2
    WHERE lease_token = $1 AND outcome IS NULL
"#;

#[derive(diesel::QueryableByName)]
struct IdRow {
    #[diesel(sql_type = sql_types::Uuid)]
    #[allow(dead_code)]
    id: RunId,
}

#[derive(diesel::QueryableByName)]
struct TerminalRow {
    #[diesel(sql_type = sql_types::Bool)]
    terminal: bool,
}

#[derive(diesel::QueryableByName)]
struct FailureRow {
    #[diesel(sql_type = crate::schema::sql_types::SchedulerRunState)]
    state: StoredRunState,
    #[diesel(sql_type = sql_types::Timestamptz)]
    available_at: DateTime<Utc>,
}

#[derive(diesel::QueryableByName)]
struct RenewalRow {
    #[diesel(sql_type = sql_types::Timestamptz)]
    lease_expires_at: DateTime<Utc>,
}

async fn terminal(conn: &mut AsyncPgConnection, run_id: RunId) -> Result<bool, SchedulerError> {
    Ok(diesel::sql_query(IS_TERMINAL)
        .bind::<sql_types::Uuid, _>(run_id)
        .get_result::<TerminalRow>(conn)
        .await?
        .terminal)
}

pub async fn finalize_run(
    conn: &mut AsyncPgConnection,
    run_id: RunId,
    lease_token: LeaseToken,
    outcome: Outcome,
) -> Result<FinalizeOutcome, SchedulerError> {
    let (state, attempt_outcome, error) = match outcome {
        Outcome::Completed => (
            StoredRunState::Completed,
            StoredAttemptOutcome::Completed,
            None,
        ),
        Outcome::Failed(error) => (
            StoredRunState::Failed,
            StoredAttemptOutcome::Failed,
            Some(error),
        ),
    };

    conn.transaction::<FinalizeOutcome, SchedulerError, _>(|c| {
        async move {
            let applied = diesel::sql_query(FINALIZE)
                .bind::<sql_types::Uuid, _>(run_id)
                .bind::<sql_types::Uuid, _>(lease_token)
                .bind::<crate::schema::sql_types::SchedulerRunState, _>(state)
                .bind::<sql_types::Nullable<sql_types::Text>, _>(&error)
                .get_results::<IdRow>(c)
                .await?;
            if applied.is_empty() {
                return Ok(if terminal(c, run_id).await? {
                    FinalizeOutcome::AlreadyTerminal
                } else {
                    FinalizeOutcome::Fenced
                });
            }
            let attempts = diesel::sql_query(RECORD_ATTEMPT)
                .bind::<sql_types::Uuid, _>(lease_token)
                .bind::<crate::schema::sql_types::SchedulerAttemptOutcome, _>(attempt_outcome)
                .bind::<sql_types::Nullable<sql_types::Text>, _>(error)
                .execute(c)
                .await?;
            if attempts != 1 {
                return Err(SchedulerError::Invariant(
                    "finalized task did not have one live attempt",
                ));
            }
            Ok(FinalizeOutcome::Applied)
        }
        .scope_boxed()
    })
    .await
}

pub async fn fail_run(
    conn: &mut AsyncPgConnection,
    run_id: RunId,
    lease_token: LeaseToken,
    error: String,
    retryable: bool,
) -> Result<FailureOutcome, SchedulerError> {
    conn.transaction::<FailureOutcome, SchedulerError, _>(|c| {
        async move {
            let rows = diesel::sql_query(FAIL)
                .bind::<sql_types::Uuid, _>(run_id)
                .bind::<sql_types::Uuid, _>(lease_token)
                .bind::<sql_types::Bool, _>(retryable)
                .bind::<sql_types::Text, _>(&error)
                .get_results::<FailureRow>(c)
                .await?;
            let Some(row) = rows.into_iter().next() else {
                return Ok(if terminal(c, run_id).await? {
                    FailureOutcome::AlreadyTerminal
                } else {
                    FailureOutcome::Fenced
                });
            };
            let attempts = diesel::sql_query(RECORD_ATTEMPT)
                .bind::<sql_types::Uuid, _>(lease_token)
                .bind::<crate::schema::sql_types::SchedulerAttemptOutcome, _>(
                    StoredAttemptOutcome::Failed,
                )
                .bind::<sql_types::Nullable<sql_types::Text>, _>(Some(error))
                .execute(c)
                .await?;
            if attempts != 1 {
                return Err(SchedulerError::Invariant(
                    "failed task did not have one live attempt",
                ));
            }
            Ok(match row.state {
                StoredRunState::Pending => FailureOutcome::Retried {
                    available_at: row.available_at,
                },
                StoredRunState::Failed => FailureOutcome::Failed,
                _ => return Err(SchedulerError::Invariant("failure produced invalid state")),
            })
        }
        .scope_boxed()
    })
    .await
}

pub async fn renew_lease(
    conn: &mut AsyncPgConnection,
    run_id: RunId,
    lease_token: LeaseToken,
) -> Result<RenewalOutcome, SchedulerError> {
    conn.transaction::<RenewalOutcome, SchedulerError, _>(|c| {
        async move {
            let rows = diesel::sql_query(RENEW)
                .bind::<sql_types::Uuid, _>(run_id)
                .bind::<sql_types::Uuid, _>(lease_token)
                .get_results::<RenewalRow>(c)
                .await?;
            let Some(row) = rows.into_iter().next() else {
                return Ok(RenewalOutcome::Fenced);
            };
            let attempts = diesel::sql_query(RENEW_ATTEMPT)
                .bind::<sql_types::Uuid, _>(lease_token)
                .bind::<sql_types::Timestamptz, _>(row.lease_expires_at)
                .execute(c)
                .await?;
            if attempts != 1 {
                return Err(SchedulerError::Invariant(
                    "renewed task did not have one live attempt",
                ));
            }
            Ok(RenewalOutcome::Renewed {
                lease_expires_at: row.lease_expires_at,
            })
        }
        .scope_boxed()
    })
    .await
}
