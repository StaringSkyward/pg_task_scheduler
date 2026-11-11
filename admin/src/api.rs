//! JSON routes under `/api`: task inspection, audited replay and (with `cron`) job
//! pause, resume and delete.
use axum::extract::{Path, State};
use axum::http::StatusCode;
#[cfg(feature = "cron")]
use axum::routing::delete;
use axum::routing::{get, post};
use axum::{Json, Router};
#[cfg(feature = "cron")]
use pg_task_scheduler::JobId;
#[cfg(feature = "cron")]
use pg_task_scheduler::jobs::{self, Applied};
use pg_task_scheduler::{SchedulerError, SchedulerPool, TaskId, TaskView, queue, store};

use crate::{AppState, Operator, db};

type ApiError = (StatusCode, String);

pub(crate) fn routes<P: SchedulerPool>() -> Router<AppState<P>> {
    let router = Router::new()
        .route("/api/tasks/:id", get(task::<P>))
        .route("/api/tasks/:id/replay", post(replay_task::<P>));
    #[cfg(feature = "cron")]
    let router = router
        .route("/api/jobs", get(list_jobs::<P>))
        .route("/api/jobs/:id/pause", post(pause_job::<P>))
        .route("/api/jobs/:id/resume", post(resume_job::<P>))
        .route("/api/jobs/:id", delete(delete_job::<P>));
    router
}

/// A pool failure is 503, anything else 500.
fn error(e: SchedulerError) -> ApiError {
    let status = match e {
        SchedulerError::Pool(_) => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, e.to_string())
}

async fn acquire<P: SchedulerPool>(pool: &P) -> Result<P::Conn, ApiError> {
    pool.acquire()
        .await
        .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e.to_string()))
}

#[cfg(feature = "cron")]
#[derive(serde::Serialize)]
struct JobView {
    id: JobId,
    name: String,
    cron_expression: String,
    is_paused: bool,
}

#[cfg(feature = "cron")]
impl From<pg_task_scheduler::Job> for JobView {
    fn from(j: pg_task_scheduler::Job) -> Self {
        JobView {
            id: j.id,
            name: j.name.as_str().to_owned(),
            cron_expression: j.cron.as_str().to_owned(),
            is_paused: matches!(j.lifecycle, pg_task_scheduler::JobLifecycle::Paused),
        }
    }
}

#[cfg(feature = "cron")]
async fn list_jobs<P: SchedulerPool>(
    State(s): State<AppState<P>>,
) -> Result<Json<Vec<JobView>>, ApiError> {
    let jobs = db::read(&s.pool, s.config.statement_timeout, |c| {
        Box::pin(jobs::list(c))
    })
    .await
    .map_err(error)?;
    Ok(Json(jobs.into_iter().map(JobView::from).collect()))
}

#[cfg(feature = "cron")]
fn applied(a: Applied, id: JobId) -> Result<StatusCode, ApiError> {
    match a {
        Applied::Changed => Ok(StatusCode::NO_CONTENT),
        Applied::NotFound => Err((StatusCode::NOT_FOUND, format!("no job with id {}", id.0))),
    }
}

#[cfg(feature = "cron")]
async fn pause_job<P: SchedulerPool>(
    State(s): State<AppState<P>>,
    Path(id): Path<JobId>,
) -> Result<StatusCode, ApiError> {
    let mut c = acquire(&s.pool).await?;
    applied(jobs::pause(&mut c, id).await.map_err(error)?, id)
}

#[cfg(feature = "cron")]
async fn resume_job<P: SchedulerPool>(
    State(s): State<AppState<P>>,
    Path(id): Path<JobId>,
) -> Result<StatusCode, ApiError> {
    let mut c = acquire(&s.pool).await?;
    applied(jobs::resume(&mut c, id).await.map_err(error)?, id)
}

#[cfg(feature = "cron")]
async fn delete_job<P: SchedulerPool>(
    State(s): State<AppState<P>>,
    Path(id): Path<JobId>,
) -> Result<StatusCode, ApiError> {
    let mut c = acquire(&s.pool).await?;
    applied(jobs::delete(&mut c, id).await.map_err(error)?, id)
}

#[derive(serde::Serialize)]
struct TaskJson {
    id: i64,
    task_key: String,
    task_name: String,
    queue: String,
    status: &'static str,
    attempts: i32,
    max_attempts: i32,
    due_at: chrono::DateTime<chrono::Utc>,
    locked_by: Option<String>,
    last_error: Option<String>,
    completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<TaskView> for TaskJson {
    fn from(t: TaskView) -> Self {
        TaskJson {
            id: t.id.0,
            task_key: t.task_key.as_str().to_owned(),
            task_name: t.task_name.as_str().to_owned(),
            queue: t.queue.as_str().to_owned(),
            status: t.status.as_str(),
            attempts: t.attempts,
            max_attempts: t.max_attempts,
            due_at: t.due_at,
            locked_by: t.locked_by.map(|w| w.as_str().to_owned()),
            last_error: t.last_error,
            completed_at: t.completed_at,
        }
    }
}

async fn task<P: SchedulerPool>(
    State(s): State<AppState<P>>,
    Path(id): Path<i64>,
) -> Result<Json<TaskJson>, ApiError> {
    let t = db::read(&s.pool, s.config.statement_timeout, |c| {
        Box::pin(store::task(c, TaskId(id)))
    })
    .await
    .map_err(error)?
    .ok_or((StatusCode::NOT_FOUND, format!("no task with id {id}")))?;
    Ok(Json(t.into()))
}

/// Any `operator` field in the body is ignored: the operator is who authenticated.
#[derive(serde::Deserialize)]
struct ReplayRequest {
    reason: String,
}

async fn replay_task<P: SchedulerPool>(
    State(s): State<AppState<P>>,
    Operator(operator): Operator,
    Path(id): Path<i64>,
    Json(req): Json<ReplayRequest>,
) -> Result<StatusCode, ApiError> {
    // Trimmed and required, like the forms.
    let reason = match req.reason.trim() {
        "" => return Err((StatusCode::BAD_REQUEST, "a reason is required".to_owned())),
        r => r,
    };
    let mut c = acquire(&s.pool).await?;
    if queue::replay(&mut c, TaskId(id), &operator, reason)
        .await
        .map_err(error)?
    {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((
            StatusCode::CONFLICT,
            "task is not dead or cancelled, or does not exist".to_owned(),
        ))
    }
}
