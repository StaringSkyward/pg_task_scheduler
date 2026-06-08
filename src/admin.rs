//! Optional Axum admin routes over job CRUD. Enable with the `axum` feature.
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Json, Router};

use crate::ids::JobId;
use crate::jobs;
use crate::jobs::Applied;
use crate::pool::SchedulerPool;

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
