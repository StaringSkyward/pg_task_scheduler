//! Programmatic job CRUD. The cron is already validated (`CreateJob` holds a
//! `CronExpression`). Every cursor (`next_run_at`) is computed from the database
//! clock — `now()` inside the operation's transaction — never the host clock, so
//! all scheduling derives from the single PostgreSQL clock authority.
//!
//! Cursor invariant: only the materializer (inside its insert-run + advance
//! transaction) or an explicit [`reschedule`] may move a job's `next_run_at`
//! past a due occurrence. Strict [`create`] and idempotent [`ensure_job`]
//! never advance an existing cursor.

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use scoped_futures::ScopedFutureExt;

use crate::cron::CronExpression;
use crate::error::SchedulerError;
use crate::ids::{JobId, JobName};
use crate::models::{Job, JobLifecycle, LeaseDuration, MaxAttempts, NewJob, SchedulerJob};
use crate::schema::scheduler_jobs::dsl as j;

#[derive(Debug, Clone)]
pub struct CreateJob {
    name: JobName,
    cron: CronExpression,
    args: serde_json::Value,
    lease_duration: LeaseDuration,
    max_attempts: MaxAttempts,
    lifecycle: JobLifecycle,
}

impl CreateJob {
    /// The five leading parameters are distinct, non-`Serialize` types, so their
    /// positional order is type-checked. `args` accepts any `Serialize` and is
    /// intentionally last (the one slot a transposition could reach). It is
    /// serialized here, so a raw `serde_json::Value` only crosses the boundary if
    /// the caller deliberately passes one.
    pub fn new(
        name: JobName,
        cron: CronExpression,
        lease_duration: LeaseDuration,
        max_attempts: MaxAttempts,
        lifecycle: JobLifecycle,
        args: impl serde::Serialize,
    ) -> Result<Self, SchedulerError> {
        Ok(CreateJob {
            name,
            cron,
            lease_duration,
            max_attempts,
            lifecycle,
            args: serde_json::to_value(args)?, // SchedulerError::Serde on failure
        })
    }
}

/// How [`reschedule`] should move a job's `next_run_at`. This is the only
/// non-materializer way to advance the cursor past a due occurrence.
#[derive(Debug, Clone)]
pub enum ScheduleUpdate {
    /// `next_run_at := stored_cron.next_after(db_now)`. Applies a changed cron
    /// from now on; explicitly drops any currently-due slot.
    ResetFromNow,
    /// Pin the next occurrence to an exact instant.
    SetNextRunAt(DateTime<Utc>),
}

/// Whether a job mutation addressed an existing row. The `Err` channel is reserved
/// for genuine failures; a missing job is a normal, expected outcome the caller
/// must handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Applied {
    /// The addressed job existed and the transition was applied.
    Changed,
    /// No job has that id; nothing changed.
    NotFound,
}

#[derive(diesel::QueryableByName)]
struct NowRow {
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    now: DateTime<Utc>,
}

/// The database transaction clock (`now()` is the transaction start time). All
/// cursor computation uses this, matching `store::materialize`.
async fn fetch_db_now(conn: &mut AsyncPgConnection) -> Result<DateTime<Utc>, SchedulerError> {
    Ok(diesel::sql_query("SELECT now() AS now")
        .get_result::<NowRow>(conn)
        .await?
        .now)
}

fn new_job(spec: &CreateJob, db_now: DateTime<Utc>) -> Result<NewJob, SchedulerError> {
    Ok(NewJob {
        name: spec.name.clone(),
        cron_expression: spec.cron.as_str().to_owned(),
        job_args: spec.args.clone(),
        next_run_at: spec.cron.next_after(db_now)?,
        lease_duration: spec.lease_duration.to_pg_interval(),
        max_attempts: spec.max_attempts.to_i32(),
        is_paused: spec.lifecycle.is_paused(),
    })
}

/// Strict insert: errors if a job with this name already exists. The initial
/// cursor is computed from the DB clock.
pub async fn create(conn: &mut AsyncPgConnection, spec: CreateJob) -> Result<Job, SchedulerError> {
    let row = conn
        .transaction::<SchedulerJob, SchedulerError, _>(|c| {
            async move {
                let db_now = fetch_db_now(c).await?;
                let row = new_job(&spec, db_now)?;
                Ok(diesel::insert_into(j::scheduler_jobs)
                    .values(row)
                    .returning(SchedulerJob::as_returning())
                    .get_result(c)
                    .await?)
            }
            .scope_boxed()
        })
        .await?;
    Job::try_from(row)
}

/// Idempotent declarative registration. Inserts the job if its name is new (the
/// cursor computed from `db_now`); on conflict reconciles config columns (cron,
/// args, lease, max_attempts) but PRESERVES the existing `next_run_at` and
/// `is_paused`. Because the cursor is never advanced here, ensure-on-startup can
/// never skip a due occurrence, and a redeploy can never resurrect a job an
/// operator paused. Use [`reschedule`] to move the cursor explicitly.
pub async fn ensure_job(
    conn: &mut AsyncPgConnection,
    spec: CreateJob,
) -> Result<Job, SchedulerError> {
    let row = conn
        .transaction::<SchedulerJob, SchedulerError, _>(|c| {
            async move {
                let db_now = fetch_db_now(c).await?;
                let row = new_job(&spec, db_now)?;
                // next_run_at and is_paused are supplied as INSERT values but are
                // deliberately ABSENT from the DO UPDATE SET list, so a conflict
                // leaves the cursor and lifecycle untouched.
                Ok(diesel::insert_into(j::scheduler_jobs)
                    .values(&row)
                    .on_conflict(j::name)
                    .do_update()
                    .set((
                        j::cron_expression.eq(&row.cron_expression),
                        j::job_args.eq(&row.job_args),
                        j::lease_duration.eq(&row.lease_duration),
                        j::max_attempts.eq(row.max_attempts),
                        j::updated_at.eq(db_now),
                    ))
                    .returning(SchedulerJob::as_returning())
                    .get_result(c)
                    .await?)
            }
            .scope_boxed()
        })
        .await?;
    Job::try_from(row)
}

/// Explicitly move a job's `next_run_at` — the only non-materializer operation
/// permitted to advance the cursor past a due occurrence. Runs in one
/// transaction. Returns `Ok(None)` if no job has the given id.
pub async fn reschedule(
    conn: &mut AsyncPgConnection,
    id: JobId,
    update: ScheduleUpdate,
) -> Result<Option<Job>, SchedulerError> {
    let row = conn
        .transaction::<Option<SchedulerJob>, SchedulerError, _>(|c| {
            async move {
                let db_now = fetch_db_now(c).await?;
                let next = match update {
                    ScheduleUpdate::SetNextRunAt(ts) => ts,
                    ScheduleUpdate::ResetFromNow => {
                        // Lock the row and read its stored cron; absent => no such job.
                        let cron_str: Option<String> = j::scheduler_jobs
                            .find(id)
                            .select(j::cron_expression)
                            .for_update()
                            .first(c)
                            .await
                            .optional()?;
                        let Some(cron_str) = cron_str else {
                            return Ok(None);
                        };
                        // A corrupt stored cron surfaces loudly as CorruptJob; never a silent skip.
                        CronExpression::parse_stored(id, &cron_str)?.next_after(db_now)?
                    }
                };
                // `.optional()` yields Ok(None) for an unknown id: in the
                // SetNextRunAt branch no row was locked so the UPDATE matches
                // nothing; the ResetFromNow branch already returned early above
                // when its FOR UPDATE read found no row.
                Ok(diesel::update(j::scheduler_jobs.find(id))
                    .set((j::next_run_at.eq(next), j::updated_at.eq(db_now)))
                    .returning(SchedulerJob::as_returning())
                    .get_result(c)
                    .await
                    .optional()?)
            }
            .scope_boxed()
        })
        .await?;
    row.map(Job::try_from).transpose()
}

pub async fn get(conn: &mut AsyncPgConnection, id: JobId) -> Result<Option<Job>, SchedulerError> {
    let row: Option<SchedulerJob> = j::scheduler_jobs
        .find(id)
        .select(SchedulerJob::as_returning())
        .first(conn)
        .await
        .optional()?;
    row.map(Job::try_from).transpose()
}

pub async fn list(conn: &mut AsyncPgConnection) -> Result<Vec<Job>, SchedulerError> {
    let rows: Vec<SchedulerJob> = j::scheduler_jobs
        .order(j::name.asc())
        .select(SchedulerJob::as_returning())
        .load(conn)
        .await?;
    rows.into_iter().map(Job::try_from).collect()
}

/// Map a Diesel affected-row count to `Applied`. Centralizes the 0/1 meaning for
/// the by-primary-key mutations (`set_paused`, `delete`), where the count is 0 or 1.
fn applied_from_affected(affected: usize) -> Applied {
    if affected > 0 {
        Applied::Changed
    } else {
        Applied::NotFound
    }
}

pub async fn pause(conn: &mut AsyncPgConnection, id: JobId) -> Result<Applied, SchedulerError> {
    set_paused(conn, id, true).await
}

pub async fn resume(conn: &mut AsyncPgConnection, id: JobId) -> Result<Applied, SchedulerError> {
    set_paused(conn, id, false).await
}

async fn set_paused(
    conn: &mut AsyncPgConnection,
    id: JobId,
    paused: bool,
) -> Result<Applied, SchedulerError> {
    let affected = diesel::update(j::scheduler_jobs.find(id))
        .set((j::is_paused.eq(paused), j::updated_at.eq(Utc::now())))
        .execute(conn)
        .await?;
    Ok(applied_from_affected(affected))
}

pub async fn delete(conn: &mut AsyncPgConnection, id: JobId) -> Result<Applied, SchedulerError> {
    let affected = diesel::delete(j::scheduler_jobs.find(id))
        .execute(conn)
        .await?;
    Ok(applied_from_affected(affected))
}
