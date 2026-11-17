//! Recurring schedules (the `cron` feature): create, list, pause, resume, reschedule and
//! delete rows in `scheduler_jobs`. Running schedulers turn each due occurrence into an
//! ordinary queue task.
//!
//! A schedule has its own unique name ([`ScheduleName`]) and runs a task
//! ([`TaskName`]); many schedules may run one task. A job is paused while it has a
//! row in `scheduler_job_pauses`; deleting the job deletes its pause.
//!
//! The cron is already validated (`CreateJob` holds a
//! `CronExpression`). Every cursor (`next_run_at`) is computed from the database
//! clock — `now()` inside the operation's transaction — never the host clock, so
//! all scheduling derives from the single PostgreSQL clock authority.
//!
//! Cursor invariant: only the materializer (inside its insert-run + advance
//! transaction) or an explicit [`reschedule`] may move a job's `next_run_at`
//! past a due occurrence. Strict [`create`] and idempotent [`ensure_job`]
//! never advance an existing cursor.

use chrono::DateTime;
use chrono::Utc;
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;

use crate::cron::CronExpression;
use crate::error::SchedulerError;
use crate::ids::JobId;
use crate::ids::QueueName;
use crate::ids::ScheduleName;
use crate::ids::TaskName;
use crate::models::Job;
use crate::models::JobLifecycle;
use crate::models::MaxAttempts;
use crate::models::NewJob;
use crate::models::SchedulerJob;
use crate::queue::Task;
use crate::schema::scheduler_job_pauses::dsl as p;
use crate::schema::scheduler_jobs::dsl as j;

/// A schedule to insert with [`create`] or [`ensure_job`].
#[derive(Debug, Clone)]
pub struct CreateJob {
    name: ScheduleName,
    task_name: TaskName,
    cron: CronExpression,
    args: serde_json::Value,
    max_attempts: MaxAttempts,
    lifecycle: JobLifecycle,
    queue: QueueName,
}

impl CreateJob {
    /// A schedule `name` that runs task `T` with `args` at each `cron` occurrence, in
    /// `T::QUEUE`. The preferred constructor: the task name, queue and argument type
    /// all come from `T`. Fails if `T::NAME` or `T::QUEUE` is not a valid identifier,
    /// or `args` does not serialize.
    pub fn for_task<T: Task>(
        name: ScheduleName,
        cron: CronExpression,
        max_attempts: MaxAttempts,
        lifecycle: JobLifecycle,
        args: T::Args,
    ) -> Result<Self, SchedulerError> {
        let task_name = TaskName::try_from(T::NAME)?;
        let queue = QueueName::try_from(T::QUEUE)?;
        Ok(Self::new(name, task_name, cron, max_attempts, lifecycle, args)?.queue(queue))
    }

    /// A schedule `name` that runs task `task_name` with `args` at each `cron`
    /// occurrence, in the `default` queue. Fails only if `args` does not serialize.
    ///
    /// The untyped boundary: nothing ties `task_name` to a handler or `args` to its
    /// argument type. Prefer [`CreateJob::for_task`] when the [`Task`] type is at hand.
    /// The leading parameters are distinct types, so their order is type-checked;
    /// `args` accepts any `Serialize` and is last.
    pub fn new(
        name: ScheduleName,
        task_name: TaskName,
        cron: CronExpression,
        max_attempts: MaxAttempts,
        lifecycle: JobLifecycle,
        args: impl serde::Serialize,
    ) -> Result<Self, SchedulerError> {
        Ok(CreateJob {
            name,
            task_name,
            cron,
            max_attempts,
            lifecycle,
            queue: QueueName::default(),
            args: serde_json::to_value(args)?, // SchedulerError::Serde on failure
        })
    }

    /// The queue for the tasks this schedule produces (default `default`, or
    /// `T::QUEUE` from [`CreateJob::for_task`]).
    pub fn queue(mut self, queue: QueueName) -> Self {
        self.queue = queue;
        self
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
#[must_use = "a NotFound outcome means the job did not exist; handle it"]
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
        task_name: spec.task_name.clone(),
        cron_expression: spec.cron.as_str().to_owned(),
        job_args: spec.args.clone(),
        next_run_at: spec.cron.next_after(db_now)?,
        max_attempts: spec.max_attempts.to_i32(),
        queue: spec.queue.clone(),
    })
}

/// A row with its pause state: `paused_at` is set iff a pause row exists.
fn project((row, paused_at): (SchedulerJob, Option<DateTime<Utc>>)) -> Result<Job, SchedulerError> {
    let lifecycle = match paused_at {
        Some(_) => JobLifecycle::Paused,
        None => JobLifecycle::Active,
    };
    Job::from_row(row, lifecycle)
}

/// The job with this id and its pause state, if any.
async fn load(conn: &mut AsyncPgConnection, id: JobId) -> Result<Option<Job>, SchedulerError> {
    j::scheduler_jobs
        .left_join(p::scheduler_job_pauses)
        .filter(j::id.eq(id))
        .select((SchedulerJob::as_select(), p::paused_at.nullable()))
        .first(conn)
        .await
        .optional()?
        .map(project)
        .transpose()
}

/// `load` for a job written earlier in the same transaction, so it must exist.
async fn reload(conn: &mut AsyncPgConnection, id: JobId) -> Result<Job, SchedulerError> {
    load(conn, id).await?.ok_or(SchedulerError::Invariant(
        "job missing in the transaction that wrote it",
    ))
}

/// Pause job `id`; a no-op if it is already paused.
async fn insert_pause(conn: &mut AsyncPgConnection, id: JobId) -> Result<(), SchedulerError> {
    diesel::insert_into(p::scheduler_job_pauses)
        .values(p::job_id.eq(id))
        .on_conflict_do_nothing()
        .execute(conn)
        .await?;
    Ok(())
}

/// Strict insert: errors if a job with this name already exists. The initial
/// cursor is computed from the DB clock; a `Paused` spec also gets its pause row.
pub async fn create(conn: &mut AsyncPgConnection, spec: CreateJob) -> Result<Job, SchedulerError> {
    conn.transaction::<Job, SchedulerError, _>(async |c| {
        let db_now = fetch_db_now(c).await?;
        let id: JobId = diesel::insert_into(j::scheduler_jobs)
            .values(new_job(&spec, db_now)?)
            .returning(j::id)
            .get_result(c)
            .await?;
        if spec.lifecycle == JobLifecycle::Paused {
            insert_pause(c, id).await?;
        }
        reload(c, id).await
    })
    .await
}

/// Idempotent declarative registration. Inserts the job if its name is new (the
/// cursor computed from `db_now`, plus a pause row for a `Paused` spec); otherwise
/// reconciles the config columns (task name, cron, args, max_attempts, queue) but
/// PRESERVES the existing `next_run_at` and pause state. Because the cursor is never
/// advanced here, ensure-on-startup can never skip a due occurrence, and a redeploy
/// can never resurrect a job an operator paused (or pause one they resumed). Use
/// [`reschedule`] to move the cursor explicitly.
pub async fn ensure_job(
    conn: &mut AsyncPgConnection,
    spec: CreateJob,
) -> Result<Job, SchedulerError> {
    conn.transaction::<Job, SchedulerError, _>(async |c| {
        let db_now = fetch_db_now(c).await?;
        let row = new_job(&spec, db_now)?;
        // A concurrent insert of the same name waits for the other transaction, then
        // does nothing; the UPDATE below then sees the committed row.
        let inserted: Option<JobId> = diesel::insert_into(j::scheduler_jobs)
            .values(&row)
            .on_conflict(j::name)
            .do_nothing()
            .returning(j::id)
            .get_result(c)
            .await
            .optional()?;
        let id = match inserted {
            Some(id) => {
                if spec.lifecycle == JobLifecycle::Paused {
                    insert_pause(c, id).await?;
                }
                id
            }
            // next_run_at and the pause state are deliberately absent from the SET.
            None => {
                diesel::update(j::scheduler_jobs.filter(j::name.eq(&row.name)))
                    .set((
                        j::task_name.eq(&row.task_name),
                        j::cron_expression.eq(&row.cron_expression),
                        j::job_args.eq(&row.job_args),
                        j::max_attempts.eq(row.max_attempts),
                        j::queue.eq(&row.queue),
                        j::updated_at.eq(db_now),
                    ))
                    .returning(j::id)
                    .get_result(c)
                    .await?
            }
        };
        reload(c, id).await
    })
    .await
}

/// Explicitly move a job's `next_run_at` — the only non-materializer operation
/// permitted to advance the cursor past a due occurrence. Runs in one
/// transaction. Returns `Ok(None)` if no job has the given id.
pub async fn reschedule(
    conn: &mut AsyncPgConnection,
    id: JobId,
    update: ScheduleUpdate,
) -> Result<Option<Job>, SchedulerError> {
    conn.transaction::<Option<Job>, SchedulerError, _>(async |c| {
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
        let updated = diesel::update(j::scheduler_jobs.find(id))
            .set((j::next_run_at.eq(next), j::updated_at.eq(db_now)))
            .execute(c)
            .await?;
        if updated == 0 {
            return Ok(None);
        }
        load(c, id).await
    })
    .await
}

/// The schedule with this id, if any.
pub async fn get(conn: &mut AsyncPgConnection, id: JobId) -> Result<Option<Job>, SchedulerError> {
    load(conn, id).await
}

/// Every schedule, ordered by name.
pub async fn list(conn: &mut AsyncPgConnection) -> Result<Vec<Job>, SchedulerError> {
    j::scheduler_jobs
        .left_join(p::scheduler_job_pauses)
        .order(j::name.asc())
        .select((SchedulerJob::as_select(), p::paused_at.nullable()))
        .load(conn)
        .await?
        .into_iter()
        .map(project)
        .collect()
}

/// Stops a schedule creating tasks. Tasks it already created are unaffected.
/// Pausing a paused job changes nothing and is still `Changed`.
pub async fn pause(conn: &mut AsyncPgConnection, id: JobId) -> Result<Applied, SchedulerError> {
    set_paused(conn, id, true).await
}

/// Lets a paused schedule create tasks again. Resuming an active job changes
/// nothing and is still `Changed`.
pub async fn resume(conn: &mut AsyncPgConnection, id: JobId) -> Result<Applied, SchedulerError> {
    set_paused(conn, id, false).await
}

/// Add or remove the pause row. `Changed` iff the job exists.
async fn set_paused(
    conn: &mut AsyncPgConnection,
    id: JobId,
    paused: bool,
) -> Result<Applied, SchedulerError> {
    conn.transaction::<Applied, SchedulerError, _>(async |c| {
        // FOR KEY SHARE: the job cannot be deleted before the pause row references it.
        let found: Option<JobId> = j::scheduler_jobs
            .find(id)
            .select(j::id)
            .for_key_share()
            .first(c)
            .await
            .optional()?;
        if found.is_none() {
            return Ok(Applied::NotFound);
        }
        if paused {
            insert_pause(c, id).await?;
        } else {
            diesel::delete(p::scheduler_job_pauses.find(id))
                .execute(c)
                .await?;
        }
        Ok(Applied::Changed)
    })
    .await
}

/// Deletes a schedule and its pause. Tasks it already created are unaffected.
pub async fn delete(conn: &mut AsyncPgConnection, id: JobId) -> Result<Applied, SchedulerError> {
    let affected = diesel::delete(j::scheduler_jobs.find(id))
        .execute(conn)
        .await?;
    Ok(if affected > 0 {
        Applied::Changed
    } else {
        Applied::NotFound
    })
}
