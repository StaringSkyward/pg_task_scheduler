use diesel_async::{AsyncPgConnection, RunQueryDsl};

use crate::error::SchedulerError;
use crate::ids::RunId;
use crate::models::{
    AttemptState, Lease, RunState, StatusRow, StoredAttemptOutcome, StoredRunState, TaskAttempt,
};

const SELECT: &str = r#"
    SELECT id, job_id, scheduled_for, attempt_count, worker_id, lease_token,
           lease_expires_at, started_at, state, finished_at, last_error
    FROM scheduler_runs WHERE id = $1
"#;

pub async fn run_state(
    conn: &mut AsyncPgConnection,
    run_id: RunId,
) -> Result<Option<RunState>, SchedulerError> {
    let mut rows: Vec<StatusRow> = diesel::sql_query(SELECT)
        .bind::<diesel::sql_types::Uuid, _>(run_id)
        .get_results(conn)
        .await?;
    rows.pop().map(state_of).transpose()
}

fn state_of(row: StatusRow) -> Result<RunState, SchedulerError> {
    match row.state {
        StoredRunState::Pending => Ok(RunState::Pending),
        StoredRunState::Running => Ok(RunState::Running(Lease {
            worker_id: row
                .worker_id
                .ok_or(SchedulerError::Invariant("running task without worker"))?,
            lease_token: row
                .lease_token
                .ok_or(SchedulerError::Invariant("running task without token"))?,
            lease_expires_at: row
                .lease_expires_at
                .ok_or(SchedulerError::Invariant("running task without expiry"))?,
            started_at: row
                .started_at
                .ok_or(SchedulerError::Invariant("running task without start"))?,
        })),
        StoredRunState::Completed => Ok(RunState::Completed {
            finished_at: row
                .finished_at
                .ok_or(SchedulerError::Invariant("completed task without finish"))?,
        }),
        StoredRunState::Failed => Ok(RunState::Failed {
            finished_at: row
                .finished_at
                .ok_or(SchedulerError::Invariant("failed task without finish"))?,
            error: row
                .last_error
                .ok_or(SchedulerError::Invariant("failed task without error"))?,
        }),
        StoredRunState::Cancelled => Ok(RunState::Cancelled {
            finished_at: row
                .finished_at
                .ok_or(SchedulerError::Invariant("cancelled task without finish"))?,
        }),
    }
}

#[derive(diesel::QueryableByName)]
struct AttemptRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    attempt_number: i32,
    #[diesel(sql_type = diesel::sql_types::Text)]
    worker_id: crate::WorkerId,
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    started_at: chrono::DateTime<chrono::Utc>,
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    lease_expires_at: chrono::DateTime<chrono::Utc>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
    finished_at: Option<chrono::DateTime<chrono::Utc>>,
    #[diesel(sql_type = diesel::sql_types::Nullable<crate::schema::sql_types::SchedulerAttemptOutcome>)]
    outcome: Option<StoredAttemptOutcome>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    error: Option<String>,
}

pub async fn task_attempts(
    conn: &mut AsyncPgConnection,
    run_id: RunId,
) -> Result<Vec<TaskAttempt>, SchedulerError> {
    let rows = diesel::sql_query(
        r#"SELECT attempt_number, worker_id, started_at, lease_expires_at,
                  finished_at, outcome, error
           FROM scheduler_run_attempts
           WHERE run_id = $1
           ORDER BY attempt_number"#,
    )
    .bind::<diesel::sql_types::Uuid, _>(run_id)
    .get_results::<AttemptRow>(conn)
    .await?;
    rows.into_iter()
        .map(|row| {
            let attempt = u32::try_from(row.attempt_number)
                .ok()
                .and_then(std::num::NonZeroU32::new)
                .ok_or(SchedulerError::Invariant(
                    "attempt history contains a non-positive number",
                ))?;
            let state = match row.outcome {
                None => AttemptState::Running,
                Some(StoredAttemptOutcome::Completed) => AttemptState::Completed,
                Some(StoredAttemptOutcome::Failed) => AttemptState::Failed { error: row.error },
                Some(StoredAttemptOutcome::Expired) => AttemptState::Expired { error: row.error },
                Some(StoredAttemptOutcome::Cancelled) => AttemptState::Cancelled,
            };
            Ok(TaskAttempt {
                attempt,
                worker_id: row.worker_id,
                started_at: row.started_at,
                lease_expires_at: row.lease_expires_at,
                finished_at: row.finished_at,
                state,
            })
        })
        .collect()
}
