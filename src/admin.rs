//! Optional Axum admin routes over job CRUD. Enable with the `axum` feature.
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Json, Router};

use crate::ids::JobId;
use crate::jobs;
use crate::jobs::Applied;
use crate::pool::SchedulerPool;
use crate::{CancelOutcome, RunId, RunState, queue, store};

#[derive(serde::Serialize)]
struct JobView {
    id: JobId,
    name: String,
    cron_expression: String,
    is_paused: bool,
}

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

pub fn router<P: SchedulerPool>(pool: P) -> Router {
    Router::new()
        .route("/jobs", get(list_jobs::<P>))
        .route("/jobs/:id/pause", post(pause_job::<P>))
        .route("/jobs/:id/resume", post(resume_job::<P>))
        .route("/jobs/:id", delete(delete_job::<P>))
        .route("/tasks/:id", get(task_state::<P>))
        .route("/tasks/:id/cancel", post(cancel_task::<P>))
        .with_state(pool)
}

async fn acquire<P: SchedulerPool>(pool: &P) -> Result<P::Conn, (StatusCode, String)> {
    pool.acquire()
        .await
        .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e.to_string()))
}

async fn list_jobs<P: SchedulerPool>(
    State(pool): State<P>,
) -> Result<Json<Vec<JobView>>, (StatusCode, String)> {
    let mut c = acquire(&pool).await?;
    let jobs = jobs::list(&mut c)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(jobs.into_iter().map(JobView::from).collect()))
}

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
struct TaskStateView {
    state: &'static str,
    worker_id: Option<String>,
    finished_at: Option<chrono::DateTime<chrono::Utc>>,
    error: Option<String>,
}

impl From<RunState> for TaskStateView {
    fn from(state: RunState) -> Self {
        match state {
            RunState::Pending => Self {
                state: "pending",
                worker_id: None,
                finished_at: None,
                error: None,
            },
            RunState::Running(lease) => Self {
                state: "running",
                worker_id: Some(lease.worker_id.as_str().to_owned()),
                finished_at: None,
                error: None,
            },
            RunState::Completed { finished_at } => Self {
                state: "completed",
                worker_id: None,
                finished_at: Some(finished_at),
                error: None,
            },
            RunState::Failed { finished_at, error } => Self {
                state: "failed",
                worker_id: None,
                finished_at: Some(finished_at),
                error: Some(error),
            },
            RunState::Cancelled { finished_at } => Self {
                state: "cancelled",
                worker_id: None,
                finished_at: Some(finished_at),
                error: None,
            },
        }
    }
}

async fn task_state<P: SchedulerPool>(
    State(pool): State<P>,
    Path(id): Path<uuid::Uuid>,
) -> Result<Json<TaskStateView>, (StatusCode, String)> {
    let mut connection = acquire(&pool).await?;
    let state = store::run_state(&mut connection, RunId(id))
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or((StatusCode::NOT_FOUND, format!("no task with id {id}")))?;
    Ok(Json(state.into()))
}

async fn cancel_task<P: SchedulerPool>(
    State(pool): State<P>,
    Path(id): Path<uuid::Uuid>,
) -> Result<StatusCode, (StatusCode, String)> {
    let mut connection = acquire(&pool).await?;
    match queue::cancel(&mut connection, RunId(id))
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
    {
        CancelOutcome::Cancelled => Ok(StatusCode::NO_CONTENT),
        CancelOutcome::AlreadyTerminal => Ok(StatusCode::NO_CONTENT),
        CancelOutcome::NotFound => Err((StatusCode::NOT_FOUND, format!("no task with id {id}"))),
    }
}
