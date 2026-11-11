//! Form actions: cancel, replay, queue pause/resume and bulk cancel/replay. Every POST
//! needs an `Operator` (recorded in the audit) and a non-empty `reason`.
#![allow(clippy::result_large_err)] // Err is the ready-to-send error page.

use askama::Template;
use axum::Form;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use pg_task_scheduler::store::{self, BULK_BUDGET, BulkFilter, CancelOutcome};
use pg_task_scheduler::{QueueName, SchedulerError, SchedulerPool, TaskId, TaskName, replay};
use serde::Deserialize;

use crate::pages::{ErrorPage, render};
use crate::{AppState, Operator};

pub(crate) fn routes<P: SchedulerPool>() -> Router<AppState<P>> {
    Router::new()
        .route("/tasks/:id/cancel", post(cancel::<P>))
        .route("/tasks/:id/replay", post(replay_task::<P>))
        .route("/queues/pause", post(|s, o, f| queue(s, o, f, true)))
        .route("/queues/resume", post(|s, o, f| queue(s, o, f, false)))
        .route("/bulk", get(confirm::<P>).post(bulk::<P>))
}

fn fail(
    state: &AppState<impl SchedulerPool>,
    status: StatusCode,
    msg: impl Into<String>,
) -> Response {
    let config = &state.config;
    render(
        ErrorPage {
            title: "Action failed",
            base: config.base_path.clone(),
            refresh: config.refresh_seconds,
            error: msg.into(),
        },
        status,
    )
}

fn db_fail(state: &AppState<impl SchedulerPool>, e: SchedulerError) -> Response {
    let status = match e {
        SchedulerError::Pool(_) => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    fail(state, status, e.to_string())
}

/// The trimmed reason, or the 400 response for an empty one.
fn reason<P: SchedulerPool>(state: &AppState<P>, r: &str) -> Result<String, Response> {
    match r.trim() {
        "" => Err(fail(
            state,
            StatusCode::BAD_REQUEST,
            "A reason is required.",
        )),
        r => Ok(r.to_owned()),
    }
}

#[derive(Deserialize)]
struct ReasonForm {
    #[serde(default)]
    reason: String,
}

async fn cancel<P: SchedulerPool>(
    State(state): State<AppState<P>>,
    Operator(op): Operator,
    Path(id): Path<i64>,
    Form(f): Form<ReasonForm>,
) -> Response {
    let reason = match reason(&state, &f.reason) {
        Ok(r) => r,
        Err(e) => return e,
    };
    let mut conn = match state.pool.acquire().await {
        Ok(c) => c,
        Err(e) => return db_fail(&state, SchedulerError::Pool(e.to_string())),
    };
    match store::cancel(&mut conn, TaskId(id), &op, &reason).await {
        Ok(CancelOutcome::Cancelled) => to_task(&state, id),
        Ok(CancelOutcome::Busy) => fail(&state, StatusCode::CONFLICT, "Task is busy; try again."),
        Ok(CancelOutcome::NotCancellable) => fail(
            &state,
            StatusCode::CONFLICT,
            "Task is already finished or does not exist.",
        ),
        Err(e) => db_fail(&state, e),
    }
}

async fn replay_task<P: SchedulerPool>(
    State(state): State<AppState<P>>,
    Operator(op): Operator,
    Path(id): Path<i64>,
    Form(f): Form<ReasonForm>,
) -> Response {
    let reason = match reason(&state, &f.reason) {
        Ok(r) => r,
        Err(e) => return e,
    };
    let mut conn = match state.pool.acquire().await {
        Ok(c) => c,
        Err(e) => return db_fail(&state, SchedulerError::Pool(e.to_string())),
    };
    match replay(&mut conn, TaskId(id), &op, &reason).await {
        Ok(true) => to_task(&state, id),
        Ok(false) => fail(
            &state,
            StatusCode::CONFLICT,
            "Task is not dead or cancelled, or does not exist.",
        ),
        Err(e) => db_fail(&state, e),
    }
}

fn to_task(state: &AppState<impl SchedulerPool>, id: i64) -> Response {
    Redirect::to(&format!("{}/tasks/{id}", state.config.base_path)).into_response()
}

#[derive(Deserialize)]
struct QueueForm {
    #[serde(default)]
    queue: String,
    #[serde(default)]
    reason: String,
}

async fn queue<P: SchedulerPool>(
    State(state): State<AppState<P>>,
    Operator(op): Operator,
    Form(f): Form<QueueForm>,
    pause: bool,
) -> Response {
    let reason = match reason(&state, &f.reason) {
        Ok(r) => r,
        Err(e) => return e,
    };
    let Ok(queue) = QueueName::try_from(f.queue) else {
        return fail(&state, StatusCode::BAD_REQUEST, "Invalid queue name.");
    };
    let mut conn = match state.pool.acquire().await {
        Ok(c) => c,
        Err(e) => return db_fail(&state, SchedulerError::Pool(e.to_string())),
    };
    let r = if pause {
        store::pause_queue(&mut conn, &queue, &op, &reason).await
    } else {
        store::resume_queue(&mut conn, &queue, &op, &reason).await
    };
    match r {
        Ok(_) => Redirect::to(&format!("{}/overview", state.config.base_path)).into_response(),
        Err(e) => db_fail(&state, e),
    }
}

/// Query (GET) and form (POST) fields of the bulk page; empty `queue`/`error` mean "any".
#[derive(Deserialize, Default)]
struct BulkParams {
    #[serde(default)]
    action: String,
    #[serde(default)]
    task_name: String,
    #[serde(default)]
    queue: String,
    #[serde(default)]
    error: String,
    #[serde(default)]
    confirm_name: String,
    #[serde(default)]
    reason: String,
}

struct Bulk {
    replay: bool,
    filter: BulkFilter,
    error: Option<String>,
}

fn parse<P: SchedulerPool>(state: &AppState<P>, p: &BulkParams) -> Result<Bulk, Response> {
    let bad = |m| fail(state, StatusCode::BAD_REQUEST, m);
    let replay = match p.action.as_str() {
        "cancel" => false,
        "replay" => true,
        _ => return Err(bad("Unknown action.")),
    };
    let task_name =
        TaskName::try_from(p.task_name.as_str()).map_err(|_| bad("Invalid task name."))?;
    let queue = match p.queue.as_str() {
        "" => None,
        q => Some(QueueName::try_from(q).map_err(|_| bad("Invalid queue name."))?),
    };
    let error = Some(p.error.clone()).filter(|e| !e.is_empty());
    Ok(Bulk {
        replay,
        filter: BulkFilter { task_name, queue },
        error,
    })
}

#[derive(Template)]
#[template(path = "confirm.html")]
struct ConfirmPage<'a> {
    base: &'a str,
    refresh: u64,
    p: &'a BulkParams,
    verb: &'a str,
    count: String,
}

#[derive(Template)]
#[template(path = "bulk_result.html")]
struct ResultPage<'a> {
    base: &'a str,
    refresh: u64,
    p: &'a BulkParams,
    verb: &'a str,
    affected: i64,
    more: bool,
}

async fn confirm<P: SchedulerPool>(
    State(state): State<AppState<P>>,
    Query(p): Query<BulkParams>,
) -> Response {
    let b = match parse(&state, &p) {
        Ok(b) => b,
        Err(e) => return e,
    };
    let cap = state.config.overview.count_cap;
    let mut conn = match state.pool.acquire().await {
        Ok(c) => c,
        Err(e) => return db_fail(&state, SchedulerError::Pool(e.to_string())),
    };
    let n = if b.replay {
        store::count_replayable(&mut conn, &b.filter, b.error.as_deref(), cap).await
    } else {
        store::count_cancellable(&mut conn, &b.filter, cap).await
    };
    match n {
        Ok(n) => render(
            ConfirmPage {
                base: &state.config.base_path,
                refresh: state.config.refresh_seconds,
                p: &p,
                verb: if b.replay { "replay" } else { "cancel" },
                count: if n >= cap {
                    format!("{n}+")
                } else {
                    n.to_string()
                },
            },
            StatusCode::OK,
        ),
        Err(e) => db_fail(&state, e),
    }
}

async fn bulk<P: SchedulerPool>(
    State(state): State<AppState<P>>,
    Operator(op): Operator,
    Form(p): Form<BulkParams>,
) -> Response {
    let b = match parse(&state, &p) {
        Ok(b) => b,
        Err(e) => return e,
    };
    if p.confirm_name != p.task_name {
        return fail(
            &state,
            StatusCode::BAD_REQUEST,
            "Type the task name to confirm.",
        );
    }
    let reason = match reason(&state, &p.reason) {
        Ok(r) => r,
        Err(e) => return e,
    };
    let mut conn = match state.pool.acquire().await {
        Ok(c) => c,
        Err(e) => return db_fail(&state, SchedulerError::Pool(e.to_string())),
    };
    let out = if b.replay {
        store::replay_matching(
            &mut conn,
            &b.filter,
            b.error.as_deref(),
            &op,
            &reason,
            BULK_BUDGET,
        )
        .await
    } else {
        store::cancel_matching(&mut conn, &b.filter, &op, &reason, BULK_BUDGET).await
    };
    match out {
        Ok(o) => render(
            ResultPage {
                base: &state.config.base_path,
                refresh: state.config.refresh_seconds,
                p: &p,
                verb: if b.replay { "replayed" } else { "cancelled" },
                affected: o.affected,
                more: o.more_remain,
            },
            StatusCode::OK,
        ),
        Err(e) => db_fail(&state, e),
    }
}
