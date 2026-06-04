use diesel_async::{AsyncPgConnection, RunQueryDsl};

use crate::error::SchedulerError;
use crate::ids::RunId;
use crate::models::{Lease, RunOutcome, RunState, StatusRow};

const SELECT: &str = "\
    SELECT id, job_id, scheduled_for, attempt_count, worker_id, lease_token, \
           lease_expires_at, started_at, outcome, finished_at, last_error \
    FROM scheduler_runs_status WHERE id = $1";

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
    match (row.outcome, row.lease_token) {
        (Some(_), Some(_)) => Err(SchedulerError::Invariant(
            "run has both a lease and an outcome",
        )),
        (Some(outcome), None) => {
            let finished_at = row
                .finished_at
                .ok_or(SchedulerError::Invariant("outcome without finished_at"))?;
            Ok(match outcome {
                RunOutcome::Completed => RunState::Completed { finished_at },
                RunOutcome::Failed => RunState::Failed {
                    finished_at,
                    error: row
                        .last_error
                        .ok_or(SchedulerError::Invariant("failed outcome without error"))?,
                },
            })
        }
        (None, Some(lease_token)) => Ok(RunState::Running(Lease {
            worker_id: row
                .worker_id
                .ok_or(SchedulerError::Invariant("lease without worker_id"))?,
            lease_token,
            lease_expires_at: row
                .lease_expires_at
                .ok_or(SchedulerError::Invariant("lease without expiry"))?,
            started_at: row
                .started_at
                .ok_or(SchedulerError::Invariant("lease without started_at"))?,
        })),
        (None, None) => Ok(RunState::Pending),
    }
}
