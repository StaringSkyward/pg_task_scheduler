//! Optional Axum admin routes: task inspection, audited replay and (with `cron`) job CRUD.
//! Enable with the `axum` feature.
use axum::extract::{Path, State};
use axum::http::StatusCode;
#[cfg(feature = "cron")]
use axum::routing::delete;
use axum::routing::{get, post};
use axum::{Json, Router};

#[cfg(feature = "cron")]
use crate::JobId;
#[cfg(feature = "cron")]
use crate::jobs::{self, Applied};
use crate::pool::SchedulerPool;
use crate::{TaskId, TaskView, queue, store};

#[cfg(feature = "cron")]
#[derive(serde::Serialize)]
struct JobView {
    id: JobId,
    name: String,
    cron_expression: String,
    is_paused: bool,
}

#[cfg(feature = "cron")]
impl From<crate::models::Job> for JobView {
    fn from(j: crate::models::Job) -> Self {
        JobView {
            id: j.id,
            name: j.name.as_str().to_owned(),
            cron_expression: j.cron.as_str().to_owned(),
            is_paused: j.lifecycle.is_paused(),
        }
    }
}

/// Admin routes. The host MUST authenticate these routes itself and supply a trustworthy
/// `operator` for replay: the body's `operator` is recorded in the audit table as given (§26).
pub fn router<P: SchedulerPool>(pool: P) -> Router {
    let router = Router::new()
        .route("/tasks/:id", get(task::<P>))
        .route("/tasks/:id/replay", post(replay_task::<P>));
    #[cfg(feature = "cron")]
    let router = router
        .route("/jobs", get(list_jobs::<P>))
        .route("/jobs/:id/pause", post(pause_job::<P>))
        .route("/jobs/:id/resume", post(resume_job::<P>))
        .route("/jobs/:id", delete(delete_job::<P>));
    router.with_state(pool)
}

async fn acquire<P: SchedulerPool>(pool: &P) -> Result<P::Conn, (StatusCode, String)> {
    pool.acquire()
        .await
        .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e.to_string()))
}

#[cfg(feature = "cron")]
async fn list_jobs<P: SchedulerPool>(
    State(pool): State<P>,
) -> Result<Json<Vec<JobView>>, (StatusCode, String)> {
    let mut c = acquire(&pool).await?;
    let jobs = jobs::list(&mut c)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(jobs.into_iter().map(JobView::from).collect()))
}

#[cfg(feature = "cron")]
async fn pause_job<P: SchedulerPool>(
    State(pool): State<P>,
    Path(id): Path<uuid::Uuid>,
) -> Result<StatusCode, (StatusCode, String)> {
    let mut c = acquire(&pool).await?;
    match jobs::pause(&mut c, JobId(id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    {
        Applied::Changed => Ok(StatusCode::NO_CONTENT),
        Applied::NotFound => Err((StatusCode::NOT_FOUND, format!("no job with id {id}"))),
    }
}

#[cfg(feature = "cron")]
async fn resume_job<P: SchedulerPool>(
    State(pool): State<P>,
    Path(id): Path<uuid::Uuid>,
) -> Result<StatusCode, (StatusCode, String)> {
    let mut c = acquire(&pool).await?;
    match jobs::resume(&mut c, JobId(id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    {
        Applied::Changed => Ok(StatusCode::NO_CONTENT),
        Applied::NotFound => Err((StatusCode::NOT_FOUND, format!("no job with id {id}"))),
    }
}

#[cfg(feature = "cron")]
async fn delete_job<P: SchedulerPool>(
    State(pool): State<P>,
    Path(id): Path<uuid::Uuid>,
) -> Result<StatusCode, (StatusCode, String)> {
    let mut c = acquire(&pool).await?;
    match jobs::delete(&mut c, JobId(id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    {
        Applied::Changed => Ok(StatusCode::NO_CONTENT),
        Applied::NotFound => Err((StatusCode::NOT_FOUND, format!("no job with id {id}"))),
    }
}

#[derive(serde::Serialize)]
struct TaskJson {
    id: i64,
    task_key: String,
    task_name: String,
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
    State(pool): State<P>,
    Path(id): Path<i64>,
) -> Result<Json<TaskJson>, (StatusCode, String)> {
    let mut c = acquire(&pool).await?;
    let t = store::task(&mut c, TaskId(id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or((StatusCode::NOT_FOUND, format!("no task with id {id}")))?;
    Ok(Json(t.into()))
}

#[derive(serde::Deserialize)]
struct ReplayRequest {
    operator: String,
    reason: String,
}

async fn replay_task<P: SchedulerPool>(
    State(pool): State<P>,
    Path(id): Path<i64>,
    Json(req): Json<ReplayRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let mut c = acquire(&pool).await?;
    if queue::replay(&mut c, TaskId(id), &req.operator, &req.reason)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((
            StatusCode::CONFLICT,
            "task is not dead or does not exist".to_owned(),
        ))
    }
}
