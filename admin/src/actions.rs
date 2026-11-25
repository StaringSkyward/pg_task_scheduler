//! Form actions: cancel, replay, queue pause/resume and bulk cancel/replay. Every POST
//! needs an `Operator` (recorded in the audit) and a non-empty `reason`.
#![allow(clippy::result_large_err)] // Err is the ready-to-send error page.

use askama::Template;
use axum::Form;
use axum::Router;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Redirect;
use axum::response::Response;
use axum::routing::get;
use axum::routing::post;
use pg_task_scheduler::QueueName;
use pg_task_scheduler::SchedulerError;
use pg_task_scheduler::SchedulerPool;
use pg_task_scheduler::TaskId;
use pg_task_scheduler::TaskName;
use pg_task_scheduler::replay;
use pg_task_scheduler::store::AdminAction;
use pg_task_scheduler::store::AdminActionTarget;
use pg_task_scheduler::store::BULK_BUDGET;
use pg_task_scheduler::store::BulkAction;
use pg_task_scheduler::store::BulkFilter;
use pg_task_scheduler::store::BulkOutcome;
use pg_task_scheduler::store::BulkStop;
use pg_task_scheduler::store::CancelOutcome;
use pg_task_scheduler::store::ErrorFilter;
use pg_task_scheduler::store::{self};
use serde::Deserialize;

use crate::AppState;
use crate::Operator;
use crate::pages::ErrorPage;
use crate::pages::error_filter;
use crate::pages::render;

pub(crate) fn routes<P: SchedulerPool>() -> Router<AppState<P>> {
    Router::new()
        .route("/tasks/:id/cancel", post(cancel::<P>))
        .route("/tasks/:id/replay", post(replay_task::<P>))
        .route("/queues/pause", post(|s, o, f| queue(s, o, f, true)))
        .route("/queues/resume", post(|s, o, f| queue(s, o, f, false)))
        .route("/bulk", get(confirm::<P>).post(bulk::<P>))
}

/// The error page, never auto-refreshed: reloading a POST result would offer to resubmit it.
fn fail(
    state: &AppState<impl SchedulerPool>,
    status: StatusCode,
    msg: impl Into<String>,
) -> Response {
    render(
        ErrorPage {
            title: "Action failed",
            base: state.config.base_path.clone(),
            refresh: 0,
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
        Err(e) => return db_fail(&state, SchedulerError::Pool(e)),
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
        Err(e) => return db_fail(&state, SchedulerError::Pool(e)),
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
        Err(e) => return db_fail(&state, SchedulerError::Pool(e)),
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

/// Query (GET) and form (POST) fields of the bulk page; an empty `queue` means "any", and
/// `error_filter` and `error` read as `pages::error_filter`. A POST with an `action_id`
/// (Continue) resumes that action, and the other fields are ignored.
#[derive(Deserialize, Default)]
struct BulkParams {
    #[serde(default)]
    action_id: String,
    #[serde(default)]
    action: String,
    #[serde(default)]
    task_name: String,
    #[serde(default)]
    queue: String,
    #[serde(default)]
    error_filter: Option<String>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    confirm_name: String,
    #[serde(default)]
    reason: String,
}

struct Bulk {
    action: BulkAction,
    filter: BulkFilter,
}

/// The form as the action it asks for, or the 400 message.
fn parse(p: &BulkParams) -> Result<Bulk, &'static str> {
    let error = error_filter(p.error_filter.as_deref(), p.error.as_deref())?;
    let action = match (p.action.as_str(), error) {
        ("cancel", ErrorFilter::Any) => BulkAction::Cancel,
        ("cancel", _) => return Err("Cancel takes no error filter."),
        ("replay", error) => BulkAction::Replay { error },
        _ => return Err("Unknown action."),
    };
    let task_name = TaskName::try_from(p.task_name.as_str()).map_err(|_| "Invalid task name.")?;
    let queue = match p.queue.as_str() {
        "" => None,
        q => Some(QueueName::try_from(q).map_err(|_| "Invalid queue name.")?),
    };
    Ok(Bulk {
        action,
        filter: BulkFilter { task_name, queue },
    })
}

fn verb(action: &BulkAction) -> &'static str {
    match action {
        BulkAction::Replay { .. } => "replay",
        BulkAction::Cancel => "cancel",
    }
}

#[derive(Template)]
#[template(path = "confirm.html")]
struct ConfirmPage<'a> {
    base: &'a str,
    refresh: u64,
    p: &'a BulkParams,
    verb: &'a str,
    /// The replay matches only tasks with no error.
    no_error: bool,
    /// The replay matches only this exact error.
    error: Option<&'a str>,
    count: String,
}

#[derive(Template)]
#[template(path = "bulk_result.html")]
struct ResultPage<'a> {
    base: &'a str,
    refresh: u64,
    p: &'a BulkParams,
    verb: &'a str,
    action_id: i64,
    affected: i64,
    budget_spent: bool,
}

async fn confirm<P: SchedulerPool>(
    State(state): State<AppState<P>>,
    Query(p): Query<BulkParams>,
) -> Response {
    let b = match parse(&p) {
        Ok(b) => b,
        Err(m) => return fail(&state, StatusCode::BAD_REQUEST, m),
    };
    let cap = state.config.overview.count_cap;
    let mut conn = match state.pool.acquire().await {
        Ok(c) => c,
        Err(e) => return db_fail(&state, SchedulerError::Pool(e)),
    };
    let n = match &b.action {
        BulkAction::Replay { error } => {
            store::count_replayable(&mut conn, &b.filter, error, cap).await
        }
        BulkAction::Cancel => store::count_cancellable(&mut conn, &b.filter, cap).await,
    };
    let (no_error, error) = match &b.action {
        BulkAction::Replay {
            error: ErrorFilter::Missing,
        } => (true, None),
        BulkAction::Replay {
            error: ErrorFilter::Exact(e),
        } => (false, Some(e.as_str())),
        _ => (false, None),
    };
    match n {
        Ok(n) => render(
            ConfirmPage {
                base: &state.config.base_path,
                refresh: state.config.refresh_seconds,
                p: &p,
                verb: verb(&b.action),
                no_error,
                error,
                count: if n >= i64::from(cap.get()) {
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
    if !p.action_id.is_empty() {
        return resume(&state, &p.action_id).await;
    }
    let b = match parse(&p) {
        Ok(b) => b,
        Err(m) => return fail(&state, StatusCode::BAD_REQUEST, m),
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
        Err(e) => return db_fail(&state, SchedulerError::Pool(e)),
    };
    let out = match &b.action {
        BulkAction::Replay { error } => {
            store::replay_matching(&mut conn, &b.filter, error, &op, &reason, BULK_BUDGET).await
        }
        BulkAction::Cancel => {
            store::cancel_matching(&mut conn, &b.filter, &op, &reason, BULK_BUDGET).await
        }
    };
    match out {
        Ok(o) => result(&state, &p, &b.action, o),
        Err(e) => db_fail(&state, e),
    }
}

/// Continue: more batches of the bulk action `id`, under its own filter, operator and reason.
async fn resume<P: SchedulerPool>(state: &AppState<P>, id: &str) -> Response {
    let Ok(id) = id.parse::<i64>() else {
        return fail(state, StatusCode::BAD_REQUEST, "Invalid action id.");
    };
    let not_found = || fail(state, StatusCode::NOT_FOUND, "No bulk action has that id.");
    let mut conn = match state.pool.acquire().await {
        Ok(c) => c,
        Err(e) => return db_fail(state, SchedulerError::Pool(e)),
    };
    let found = match store::admin_action(&mut conn, id, state.config.overview.count_cap).await {
        Ok(a) => a,
        Err(e) => return db_fail(state, e),
    };
    let Some(AdminAction {
        target:
            AdminActionTarget::Bulk {
                action,
                task_name,
                queue,
                ..
            },
        reason,
        ..
    }) = found
    else {
        return not_found();
    };
    // The action's own fields, for the Run again form a short batch offers (no mode is `any`).
    let (error_filter, error) = match &action {
        BulkAction::Replay {
            error: ErrorFilter::Missing,
        } => (Some("missing"), None),
        BulkAction::Replay {
            error: ErrorFilter::Exact(e),
        } => (Some("exact"), Some(e.clone())),
        _ => (None, None),
    };
    let p = BulkParams {
        action: verb(&action).into(),
        task_name: task_name.as_str().into(),
        queue: queue.map(|q| q.as_str().into()).unwrap_or_default(),
        error_filter: error_filter.map(str::to_owned),
        error,
        confirm_name: task_name.as_str().into(),
        reason,
        ..Default::default()
    };
    match store::resume_bulk(&mut conn, id, BULK_BUDGET).await {
        Ok(Some(o)) => result(state, &p, &action, o),
        Ok(None) => not_found(),
        Err(e) => db_fail(state, e),
    }
}

fn result(
    state: &AppState<impl SchedulerPool>,
    p: &BulkParams,
    action: &BulkAction,
    o: BulkOutcome,
) -> Response {
    render(
        ResultPage {
            base: &state.config.base_path,
            refresh: state.config.refresh_seconds,
            p,
            verb: match action {
                BulkAction::Replay { .. } => "replayed",
                BulkAction::Cancel => "cancelled",
            },
            action_id: o.action_id,
            affected: o.affected,
            budget_spent: o.stopped == BulkStop::BudgetSpent,
        },
        StatusCode::OK,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(query: &str) -> Result<BulkAction, &'static str> {
        parse(&serde_urlencoded::from_str::<BulkParams>(query).unwrap()).map(|b| b.action)
    }

    #[test]
    fn parse_reads_the_error_filter_into_its_meaning() {
        let replay = |error| Ok(BulkAction::Replay { error });
        assert_eq!(
            parsed("action=replay&task_name=a&error_filter=exact&error="),
            replay(ErrorFilter::Exact(String::new()))
        );
        assert_eq!(
            parsed("action=replay&task_name=a&error_filter=missing"),
            replay(ErrorFilter::Missing)
        );
        assert_eq!(
            parsed("action=replay&task_name=a"),
            replay(ErrorFilter::Any)
        );
        assert_eq!(parsed("action=cancel&task_name=a"), Ok(BulkAction::Cancel));
    }

    #[test]
    fn parse_rejects_a_cancel_with_an_error_filter() {
        assert_eq!(
            parsed("action=cancel&task_name=a&error_filter=exact&error=x"),
            Err("Cancel takes no error filter.")
        );
        assert_eq!(
            parsed("action=replay&task_name=a&error_filter=exact"),
            Err("An exact error filter needs an error.")
        );
        assert_eq!(
            parsed("action=replay&task_name=a&error_filter=some"),
            Err("Unknown error filter.")
        );
        // An `error` without `exact` (an old link's shape) never widens to `any`.
        for q in [
            "action=replay&task_name=a&error=x",
            "action=replay&task_name=a&error_filter=missing&error=x",
            "action=cancel&task_name=a&error=",
        ] {
            assert_eq!(
                parsed(q),
                Err("error is only for error_filter=exact."),
                "{q}"
            );
        }
    }

    // The budget-spent branch needs 500+ rows and the fixed 10 s budget to reach end to end.
    #[test]
    fn budget_spent_result_offers_continue() {
        let p = BulkParams {
            action: "cancel".into(),
            task_name: "a".into(),
            ..Default::default()
        };
        let html = ResultPage {
            base: "",
            refresh: 0,
            p: &p,
            verb: "cancelled",
            action_id: 7,
            affected: 500,
            budget_spent: true,
        }
        .render()
        .unwrap();
        assert!(html.contains("Continue"), "{html}");
        assert!(!html.contains("Run again"), "{html}");
        // Continue posts only the action id, so it resumes that action.
        assert!(html.contains("name=\"action_id\" value=\"7\""), "{html}");
        assert!(!html.contains("name=\"task_name\""), "{html}");
    }
}
