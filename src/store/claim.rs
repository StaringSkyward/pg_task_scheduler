use std::num::NonZeroU32;

use chrono::{DateTime, Utc};
use diesel::sql_types;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use scoped_futures::ScopedFutureExt;

use crate::error::SchedulerError;
use crate::ids::{JobId, JobName, LeaseToken, RunId, WorkerId};
use crate::models::ClaimedRun;

/// The oldest runnable occurrence selected (and row-locked) by `SELECT_CANDIDATE`.
/// `attempt` is `r.attempt_count + 1` — the attempt number this claim represents.
#[derive(diesel::QueryableByName)]
struct Candidate {
    #[diesel(sql_type = sql_types::Uuid)]
    run_id: RunId,
    #[diesel(sql_type = sql_types::Uuid)]
    job_id: JobId,
    #[diesel(sql_type = sql_types::Text)]
    job_name: JobName,
    #[diesel(sql_type = sql_types::Jsonb)]
    job_args: serde_json::Value,
    #[diesel(sql_type = sql_types::Timestamptz)]
    scheduled_for: DateTime<Utc>,
    #[diesel(sql_type = sql_types::Integer)]
    attempt: i32,
}

/// The fresh lease as it now stands in the DB, returned by the upsert. These are
/// the authoritative token + deadline carried by the returned `ClaimedRun` — they
/// are never fabricated in Rust.
#[derive(diesel::QueryableByName)]
struct LeaseRow {
    #[diesel(sql_type = sql_types::Uuid)]
    lease_token: LeaseToken,
    #[diesel(sql_type = sql_types::Timestamptz)]
    lease_expires_at: DateTime<Utc>,
}

/// Lock the oldest runnable occurrence for one of the worker's registered job
/// names. "Runnable" = no outcome AND (no lease OR an expired lease still under
/// `max_attempts`). `FOR UPDATE OF r SKIP LOCKED` locks ONLY the run row (the
/// mutex), never the outer-joined lease/outcome rows.
const SELECT_CANDIDATE: &str = "\
    SELECT r.id AS run_id, r.job_id, j.name AS job_name, j.job_args, \
           r.scheduled_for, r.attempt_count + 1 AS attempt \
    FROM scheduler_runs r \
    JOIN scheduler_jobs j ON j.id = r.job_id \
    LEFT JOIN scheduler_run_leases   l ON l.run_id = r.id \
    LEFT JOIN scheduler_run_outcomes o ON o.run_id = r.id \
    WHERE o.run_id IS NULL \
      AND j.name = ANY($1) \
      AND (l.run_id IS NULL OR (l.lease_expires_at <= now() AND r.attempt_count < j.max_attempts)) \
    ORDER BY r.scheduled_for ASC FOR UPDATE OF r SKIP LOCKED LIMIT 1";

/// Bump the attempt counter for the locked run.
const BUMP: &str =
    "UPDATE scheduler_runs SET attempt_count = attempt_count + 1, updated_at = now() WHERE id = $1";

/// Take (or reclaim) the lease, deriving the deadline from the job's
/// `lease_duration`. `ON CONFLICT (run_id) DO UPDATE` reclaims an expired lease,
/// resetting `started_at`. `RETURNING` yields the authoritative token + expiry.
const UPSERT_LEASE: &str = "\
    INSERT INTO scheduler_run_leases (run_id, worker_id, lease_token, lease_expires_at) \
    SELECT $1, $2, $3, now() + j.lease_duration \
    FROM scheduler_jobs j JOIN scheduler_runs r ON r.job_id = j.id WHERE r.id = $1 \
    ON CONFLICT (run_id) DO UPDATE \
      SET worker_id = EXCLUDED.worker_id, lease_token = EXCLUDED.lease_token, \
          lease_expires_at = EXCLUDED.lease_expires_at, started_at = now() \
    RETURNING lease_token, lease_expires_at";

/// Claim the oldest runnable occurrence for one of `names`, fenced by a fresh
/// token. Runs select+lock → bump → upsert-lease in a single transaction; the
/// candidate's `FOR UPDATE OF r SKIP LOCKED` is the mutex that prevents two
/// workers double-claiming the same run. Returns `None` when nothing is runnable.
/// A returned `ClaimedRun` always carries a complete lease (token + expiry from
/// the upsert's `RETURNING`) and `attempt >= 1`.
pub async fn claim_one(
    conn: &mut AsyncPgConnection,
    worker_id: &WorkerId,
    names: &[String],
) -> Result<Option<ClaimedRun>, SchedulerError> {
    let worker = worker_id.clone();
    let names = names.to_vec();
    conn.transaction::<Option<ClaimedRun>, SchedulerError, _>(|c| {
        async move {
            let Some(cand) = diesel::sql_query(SELECT_CANDIDATE)
                .bind::<sql_types::Array<sql_types::Text>, _>(&names)
                .get_results::<Candidate>(c)
                .await?
                .into_iter()
                .next()
            else {
                return Ok(None);
            };

            // A claimed run's attempt is `attempt_count + 1`, always >= 1. A
            // non-positive value here means a structural guarantee was violated;
            // surface it loudly rather than papering over it.
            let attempt = u32::try_from(cand.attempt)
                .ok()
                .and_then(NonZeroU32::new)
                .ok_or(SchedulerError::Invariant(
                    "attempt_count must be >= 1 after claim",
                ))?;

            diesel::sql_query(BUMP)
                .bind::<sql_types::Uuid, _>(cand.run_id)
                .execute(c)
                .await?;

            let token = LeaseToken::generate();
            let lease: LeaseRow = diesel::sql_query(UPSERT_LEASE)
                .bind::<sql_types::Uuid, _>(cand.run_id)
                .bind::<sql_types::Text, _>(&worker)
                .bind::<sql_types::Uuid, _>(token)
                .get_result(c)
                .await?;

            Ok(Some(ClaimedRun {
                run_id: cand.run_id,
                job_id: cand.job_id,
                job_name: cand.job_name,
                job_args: cand.job_args,
                scheduled_for: cand.scheduled_for,
                attempt,
                lease_token: lease.lease_token,
                lease_expires_at: lease.lease_expires_at,
            }))
        }
        .scope_boxed()
    })
    .await
}
