use diesel::sql_types;
use diesel_async::{AsyncPgConnection, RunQueryDsl};

use crate::error::SchedulerError;
use crate::ids::{LeaseToken, RunId};
use crate::models::{FinalizeOutcome, Outcome, RunOutcome};

const FINALIZE: &str = "\
    INSERT INTO scheduler_run_outcomes (run_id, outcome, last_error) \
    SELECT $1, $2::run_outcome, $3 FROM scheduler_run_leases \
    WHERE run_id = $1 AND lease_token = $4 \
    ON CONFLICT (run_id) DO NOTHING \
    RETURNING run_id";

const IS_TERMINAL: &str =
    "SELECT EXISTS (SELECT 1 FROM scheduler_run_outcomes WHERE run_id = $1) AS terminal";

#[derive(diesel::QueryableByName)]
struct TerminalRow {
    #[diesel(sql_type = sql_types::Bool)]
    terminal: bool,
}

/// Record the run's terminal outcome iff the caller still holds the lease (fencing).
/// The AFTER INSERT trigger clears the lease. A concurrent finalizer/reaper that
/// already recorded the outcome is a benign no-op (`AlreadyTerminal`), not an error.
pub async fn finalize_run(
    conn: &mut AsyncPgConnection,
    run_id: RunId,
    lease_token: LeaseToken,
    outcome: Outcome,
) -> Result<FinalizeOutcome, SchedulerError> {
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
    if affected > 0 {
        return Ok(FinalizeOutcome::Applied);
    }
    // 0 rows: either our token didn't match (fenced) or an outcome already exists
    // (a peer/reaper won the race). A fresh-snapshot read distinguishes them and
    // correctly sees a concurrently-committed outcome, so the race-loser is
    // AlreadyTerminal rather than an error.
    let terminal = diesel::sql_query(IS_TERMINAL)
        .bind::<sql_types::Uuid, _>(run_id)
        .get_result::<TerminalRow>(conn)
        .await?
        .terminal;
    Ok(if terminal {
        FinalizeOutcome::AlreadyTerminal
    } else {
        FinalizeOutcome::Fenced
    })
}
