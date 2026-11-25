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
//! clock — `statement_timestamp()` of the operation — never the host clock, so
//! all scheduling derives from the single PostgreSQL clock authority.
//!
//! Cursor invariant: only the materializer (inside its insert-run + advance
//! transaction) or an explicit [`reschedule`] may move a job's `next_run_at`
//! past a due occurrence. Strict [`create`] and idempotent [`ensure_job`]
//! never advance an existing cursor.
//!
//! The cursor is a relation of its own, `scheduler_job_cursors`. Every write to it happens
//! under its job's row lock (`FOR UPDATE` on `scheduler_jobs`): the materializer's and
//! [`reschedule`]'s lock the job first, and [`create`] and [`ensure_job`] write a new job's
//! cursor in the statement that inserts the job. So the lock order is job → cursor → pause.
//! A job's `updated_at` is changed by [`ensure_job`] (on every call, even one that changes
//! nothing) and definition updates, never by the materializer or [`reschedule`].

use chrono::DateTime;
use chrono::Utc;
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use futures::future::BoxFuture;
use serde::de::DeserializeOwned;

use crate::cron::CronExpression;
use crate::error::SchedulerError;
use crate::ids::JobId;
use crate::ids::QueueName;
use crate::ids::ScheduleName;
use crate::ids::TaskName;
use crate::models::Job;
use crate::models::JobLifecycle;
use crate::models::MaxAttempts;
use crate::models::SchedulerJob;
use crate::queue::Task;
use crate::schema::scheduler_job_cursors::dsl as cur;
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
    /// `check_stored::<T::Args>` from [`CreateJob::for_task`]; `None` for the untyped `new`.
    stored_check: Option<StoredCheck>,
}

/// [`crate::queue::check_stored`] for one argument type, as a plain function pointer.
type StoredCheck = for<'a> fn(
    &'a mut AsyncPgConnection,
    &'a serde_json::Value,
) -> BoxFuture<'a, Result<(), SchedulerError>>;

fn stored_check<'a, A: DeserializeOwned + PartialEq + 'static>(
    conn: &'a mut AsyncPgConnection,
    payload: &'a serde_json::Value,
) -> BoxFuture<'a, Result<(), SchedulerError>> {
    Box::pin(crate::queue::check_stored::<A>(conn, payload))
}

impl CreateJob {
    /// A schedule `name` that runs task `T` with `args` at each `cron` occurrence, in
    /// `T::QUEUE`. The preferred constructor: the task name, queue and argument type
    /// all come from `T`. Fails if `T::NAME` or `T::QUEUE` is not a valid identifier,
    /// or `args` does not serialize to JSON the worker can parse back and decode as
    /// `T::Args` equal to `args` ([`SchedulerError::Serde`] if it does not decode,
    /// [`SchedulerError::PayloadNotPreserved`] if it decodes as another value, such as
    /// `Some(f64::INFINITY)`, which JSON writes as `null`). [`create`] and [`ensure_job`] also return
    /// [`SchedulerError::PayloadNotPreserved`], writing nothing, if `args` decode as different
    /// values once stored as JSONB, as `enqueue` does.
    pub fn for_task<T: Task>(
        name: ScheduleName,
        cron: CronExpression,
        max_attempts: MaxAttempts,
        lifecycle: JobLifecycle,
        args: T::Args,
    ) -> Result<Self, SchedulerError> {
        let payload = crate::queue::typed_payload(&args)?;
        Ok(CreateJob {
            name,
            task_name: TaskName::try_from(T::NAME)?,
            cron,
            max_attempts,
            lifecycle,
            queue: QueueName::try_from(T::QUEUE)?,
            args: payload,
            stored_check: Some(stored_check::<T::Args>),
        })
    }

    /// A schedule `name` that runs task `task_name` with `args` at each `cron`
    /// occurrence, in the `default` queue. Fails only if `args` does not serialize to
    /// JSON the worker can parse back (for example, nested deeper than 128).
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
            args: crate::queue::to_payload(&args)?, // SchedulerError::Serde on failure
            stored_check: None,
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
    /// Pin the next occurrence to an instant, rounded up to the next whole microsecond so it
    /// is never earlier than given; the returned `next_run_at` is the effective instant. A
    /// leap-second instant (`:60.f`) is stored as the same fraction of the following second,
    /// later and never earlier.
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
    /// The job exists and was already in the requested state (`pause` on a paused job,
    /// `resume` on an active one); nothing changed.
    Unchanged,
    /// No job has that id; nothing changed.
    NotFound,
}

#[derive(diesel::QueryableByName)]
struct NowRow {
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    now: DateTime<Utc>,
}

/// The database statement clock (`statement_timestamp()`, the time of this statement). All
/// cursor computation uses this, matching `store::materialize`.
async fn fetch_db_now(conn: &mut AsyncPgConnection) -> Result<DateTime<Utc>, SchedulerError> {
    Ok(diesel::sql_query("SELECT statement_timestamp() AS now")
        .get_result::<NowRow>(conn)
        .await?
        .now)
}

#[derive(diesel::QueryableByName)]
struct IdRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: JobId,
}

/// Insert `spec` as a job and its cursor at `cron.next_after(db_now)`, in one statement, and
/// return its id: `None` when `ensure` and the name is taken. A new job's row is its insert's own
/// until commit, so its cursor is written under its lock. Without `ensure`, a taken name is the
/// unique violation.
async fn insert_job(
    conn: &mut AsyncPgConnection,
    spec: &CreateJob,
    db_now: DateTime<Utc>,
    ensure: bool,
) -> Result<Option<JobId>, SchedulerError> {
    use diesel::sql_types::Integer;
    use diesel::sql_types::Jsonb;
    use diesel::sql_types::Text;
    use diesel::sql_types::Timestamptz;
    let on_conflict = if ensure {
        "ON CONFLICT (name) DO NOTHING"
    } else {
        ""
    };
    let row = diesel::sql_query(format!(
        "WITH job AS ( \
             INSERT INTO scheduler_jobs \
                 (name, task_name, cron_expression, job_args, max_attempts, queue) \
             VALUES ($1, $2, $3, $4, $5, $6) {on_conflict} RETURNING id) \
         INSERT INTO scheduler_job_cursors (job_id, next_run_at) SELECT id, $7 FROM job \
         RETURNING job_id AS id"
    ))
    .bind::<Text, _>(spec.name.as_str())
    .bind::<Text, _>(spec.task_name.as_str())
    .bind::<Text, _>(spec.cron.as_str())
    .bind::<Jsonb, _>(&spec.args)
    .bind::<Integer, _>(spec.max_attempts.to_i32())
    .bind::<Text, _>(spec.queue.as_str())
    .bind::<Timestamptz, _>(spec.cron.next_after(db_now)?)
    .get_result::<IdRow>(conn)
    .await
    .optional()?;
    Ok(row.map(|r| r.id))
}

/// A row with its cursor and pause state: `paused_at` is set iff a pause row exists.
fn project(
    (row, next_run_at, paused_at): (SchedulerJob, DateTime<Utc>, Option<DateTime<Utc>>),
) -> Result<Job, SchedulerError> {
    let lifecycle = match paused_at {
        Some(_) => JobLifecycle::Paused,
        None => JobLifecycle::Active,
    };
    Job::from_row(row, next_run_at, lifecycle)
}

/// The job with this id, its cursor and its pause state, if any.
async fn load(conn: &mut AsyncPgConnection, id: JobId) -> Result<Option<Job>, SchedulerError> {
    j::scheduler_jobs
        .inner_join(cur::scheduler_job_cursors)
        .left_join(p::scheduler_job_pauses)
        .filter(j::id.eq(id))
        .select((
            SchedulerJob::as_select(),
            cur::next_run_at,
            p::paused_at.nullable(),
        ))
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
/// Returns the rows inserted: 0 when the job was already paused.
async fn insert_pause(conn: &mut AsyncPgConnection, id: JobId) -> Result<usize, SchedulerError> {
    Ok(diesel::insert_into(p::scheduler_job_pauses)
        .values(p::job_id.eq(id))
        .on_conflict_do_nothing()
        .execute(conn)
        .await?)
}

/// Strict insert: errors if a job with this name already exists. The initial
/// cursor is computed from the DB clock; a `Paused` spec also gets its pause row.
/// A spec from [`CreateJob::for_task`] first has its arguments checked as `enqueue` checks
/// them, at one extra round trip: arguments that decode as different values once stored as
/// JSONB are [`SchedulerError::PayloadNotPreserved`], and nothing is written.
pub async fn create(conn: &mut AsyncPgConnection, spec: CreateJob) -> Result<Job, SchedulerError> {
    if let Some(check) = spec.stored_check {
        check(conn, &spec.args).await?;
    }
    conn.transaction::<Job, SchedulerError, _>(async |c| {
        let db_now = fetch_db_now(c).await?;
        let id = insert_job(c, &spec, db_now, false)
            .await?
            .ok_or(SchedulerError::Invariant("a strict insert returned no job"))?;
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
/// [`reschedule`] to move the cursor explicitly. If concurrent deletes of the job keep
/// invalidating the operation, three passes later it returns [`SchedulerError::Contention`].
/// A spec from [`CreateJob::for_task`] first has its arguments checked as [`create`] does, at
/// one extra round trip; on [`SchedulerError::PayloadNotPreserved`] nothing is written.
pub async fn ensure_job(
    conn: &mut AsyncPgConnection,
    spec: CreateJob,
) -> Result<Job, SchedulerError> {
    if let Some(check) = spec.stored_check {
        check(conn, &spec.args).await?;
    }
    conn.transaction::<Job, SchedulerError, _>(async |c| {
        let db_now = fetch_db_now(c).await?;
        for _ in 0..3 {
            // A concurrent insert of the same name waits for the other transaction, then
            // does nothing; the UPDATE below then sees the committed row.
            let id = match insert_job(c, &spec, db_now, true).await? {
                Some(id) => {
                    if spec.lifecycle == JobLifecycle::Paused {
                        insert_pause(c, id).await?;
                    }
                    Some(id)
                }
                // The cursor and the pause state are deliberately left alone.
                None => diesel::update(j::scheduler_jobs.filter(j::name.eq(&spec.name)))
                    .set((
                        j::task_name.eq(&spec.task_name),
                        j::cron_expression.eq(spec.cron.as_str()),
                        j::job_args.eq(&spec.args),
                        j::max_attempts.eq(spec.max_attempts.to_i32()),
                        j::queue.eq(&spec.queue),
                        j::updated_at.eq(db_now),
                    ))
                    .returning(j::id)
                    .get_result(c)
                    .await
                    .optional()?,
            };
            // None: a concurrent delete removed the row while the UPDATE waited on its
            // lock; the next pass re-inserts it.
            if let Some(id) = id {
                return reload(c, id).await;
            }
        }
        Err(SchedulerError::Contention(
            "schedule deleted during ensure_job",
        ))
    })
    .await
}

/// Explicitly move a job's `next_run_at` — the only non-materializer operation
/// permitted to advance the cursor past a due occurrence. Runs in one
/// transaction, which locks the job row (`FOR UPDATE`) before it writes the cursor, as
/// every cursor write does. The definition and `updated_at` are unchanged. Returns
/// `Ok(None)` if no job has the given id.
pub async fn reschedule(
    conn: &mut AsyncPgConnection,
    id: JobId,
    update: ScheduleUpdate,
) -> Result<Option<Job>, SchedulerError> {
    conn.transaction::<Option<Job>, SchedulerError, _>(async |c| {
        let db_now = fetch_db_now(c).await?;
        // Lock the job and read its stored cron; absent => no such job.
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
        let next = match update {
            ScheduleUpdate::SetNextRunAt(ts) => crate::models::round_up_to_micros(ts)?,
            // A corrupt stored cron surfaces loudly as CorruptJob; never a silent skip.
            ScheduleUpdate::ResetFromNow => {
                CronExpression::parse_stored(id, &cron_str)?.next_after(db_now)?
            }
        };
        diesel::update(cur::scheduler_job_cursors.find(id))
            .set(cur::next_run_at.eq(next))
            .execute(c)
            .await?;
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
        .inner_join(cur::scheduler_job_cursors)
        .left_join(p::scheduler_job_pauses)
        .order(j::name.asc())
        .select((
            SchedulerJob::as_select(),
            cur::next_run_at,
            p::paused_at.nullable(),
        ))
        .load(conn)
        .await?
        .into_iter()
        .map(project)
        .collect()
}

/// Stops a schedule creating tasks. Tasks it already created are unaffected.
/// Pausing a paused job changes nothing and is `Unchanged`.
pub async fn pause(conn: &mut AsyncPgConnection, id: JobId) -> Result<Applied, SchedulerError> {
    set_paused(conn, id, true).await
}

/// Lets a paused schedule create tasks again. Resuming an active job changes
/// nothing and is `Unchanged`.
pub async fn resume(conn: &mut AsyncPgConnection, id: JobId) -> Result<Applied, SchedulerError> {
    set_paused(conn, id, false).await
}

/// Add or remove the pause row. `NotFound` if the job is missing, `Unchanged` if the row count
/// was zero.
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
        let rows = if paused {
            insert_pause(c, id).await?
        } else {
            diesel::delete(p::scheduler_job_pauses.find(id))
                .execute(c)
                .await?
        };
        Ok(if rows > 0 {
            Applied::Changed
        } else {
            Applied::Unchanged
        })
    })
    .await
}

/// Deletes a schedule, its cursor and its pause. Tasks it already created are unaffected.
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
