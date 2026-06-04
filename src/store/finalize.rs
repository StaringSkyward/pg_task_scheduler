use diesel::sql_types;
use diesel_async::{AsyncPgConnection, RunQueryDsl};

use crate::error::SchedulerError;
use crate::ids::{LeaseToken, RunId};
use crate::models::{Outcome, RunOutcome};

const FINALIZE: &str = "\
    INSERT INTO scheduler_run_outcomes (run_id, outcome, last_error) \
    SELECT $1, $2::run_outcome, $3 FROM scheduler_run_leases \
    WHERE run_id = $1 AND lease_token = $4 \
    RETURNING run_id";

/// Insert the run's outcome iff the caller still holds the lease (fencing). The
/// AFTER INSERT trigger clears the lease. Returns whether it applied.
pub async fn finalize_run(
    conn: &mut AsyncPgConnection,
    run_id: RunId,
    lease_token: LeaseToken,
    outcome: Outcome,
) -> Result<bool, SchedulerError> {
    let (db_outcome, error): (RunOutcome, Option<String>) = match outcome {
        Outcome::Completed => (RunOutcome::Completed, None),
        Outcome::Failed(e) => (RunOutcome::Failed, Some(e)),
    };
    let affected = diesel::sql_query(FINALIZE)
        .bind::<sql_types::Uuid, _>(run_id)
        .bind::<crate::schema::sql_types::RunOutcome, _>(db_outcome)
        .bind::<sql_types::Nullable<sql_types::Text>, _>(error)
        .bind::<sql_types::Uuid, _>(lease_token)
        .execute(conn)
        .await?;
    Ok(affected > 0)
}
