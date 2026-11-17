//! List pages (workers, queued, running, completed, failed, cancelled) and task detail.
//! Cursors travel as flat query fields; names are only ever in query strings.
use askama::Template;
use axum::Router;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use chrono::DateTime;
use chrono::Utc;
use pg_task_scheduler::AttemptRun;
use pg_task_scheduler::AttemptState;
use pg_task_scheduler::QueueName;
use pg_task_scheduler::SchedulerPool;
use pg_task_scheduler::TaskId;
use pg_task_scheduler::TaskName;
use pg_task_scheduler::TaskState;
use pg_task_scheduler::store::FailedFilter;
use pg_task_scheduler::store::FailedStatus;
use pg_task_scheduler::store::FinishedCursor;
use pg_task_scheduler::store::QueuedCursor;
use pg_task_scheduler::store::RunningCursor;
use pg_task_scheduler::store::TaskRow;
use pg_task_scheduler::store::{self};
use serde::Deserialize;
use serde::Serialize;

use crate::AppState;
use crate::db;
use crate::health::fmt_duration;
use crate::pages::ErrorPage;
use crate::pages::cached_overview;
use crate::pages::render;
use crate::pages::unavailable;

pub(crate) fn routes<P: SchedulerPool>() -> Router<AppState<P>> {
    Router::new()
        .route("/workers", get(workers::<P>))
        .route("/queued", get(queued::<P>))
        .route("/running", get(running::<P>))
        .route("/completed", get(completed::<P>))
        .route("/failed", get(|s, q| failed(s, q, FailedStatus::Dead)))
        .route(
            "/cancelled",
            get(|s, q| failed(s, q, FailedStatus::Cancelled)),
        )
        .route("/tasks/:id", get(task::<P>))
}

fn ts(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%d %H:%M:%S UTC").to_string()
}

fn span(from: DateTime<Utc>, to: DateTime<Utc>) -> String {
    fmt_duration((to - from).to_std().unwrap_or_default())
}

fn bad(msg: &str) -> Response {
    (StatusCode::BAD_REQUEST, msg.to_owned()).into_response()
}

/// `?a=1&b=2` for `pairs` then `cursor`.
fn query(pairs: &[(&str, &str)], cursor: &impl Serialize) -> String {
    let mut q = serde_urlencoded::to_string(pairs).unwrap_or_default();
    let c = serde_urlencoded::to_string(cursor).unwrap_or_default();
    if !q.is_empty() {
        q.push('&');
    }
    q + &c
}

// ---- workers ----

struct WorkerRow {
    name: String,
    lost: bool,
    started: String,
    seen: String,
    status: String,
    last_error: String,
    queues: Vec<String>,
    task_names: String,
}

#[derive(Template)]
#[template(path = "workers.html")]
struct WorkersPage {
    base: String,
    refresh: u64,
    rows: Vec<WorkerRow>,
}

async fn workers<P: SchedulerPool>(State(state): State<AppState<P>>) -> Response {
    let config = &state.config;
    let r = db::read(&state.pool, config.statement_timeout, |c| {
        Box::pin(async move { store::workers(c).await })
    })
    .await;
    let list = match r {
        Ok(l) => l,
        Err(e) => return unavailable("Workers unavailable", config, e.to_string()),
    };
    let rows = list
        .into_iter()
        .map(|w| WorkerRow {
            name: w.label.unwrap_or_else(|| w.worker_id.as_str().to_owned()),
            lost: w.lost,
            started: ts(w.started_at),
            seen: ts(w.last_seen_at),
            status: format!("{:?}", w.status).to_lowercase(),
            last_error: w.last_error.unwrap_or_default(),
            queues: w
                .queues
                .iter()
                .map(|q| {
                    format!(
                        "{}: {} / {} slots{}",
                        q.name.as_str(),
                        q.in_use,
                        q.limit,
                        if q.paused { " (paused)" } else { "" }
                    )
                })
                .collect(),
            task_names: w
                .task_names
                .iter()
                .map(TaskName::as_str)
                .collect::<Vec<_>>()
                .join(", "),
        })
        .collect();
    render(
        WorkersPage {
            base: config.base_path.clone(),
            refresh: config.refresh_seconds,
            rows,
        },
        StatusCode::OK,
    )
}

// ---- queued ----

#[derive(Deserialize)]
struct QueuedQuery {
    queue: Option<String>,
    due: Option<bool>,
    priority: Option<i32>,
    due_at: Option<DateTime<Utc>>,
    id: Option<i64>,
    position: Option<i64>,
}

struct QueuedRow {
    position: i64,
    id: i64,
    task_name: String,
    priority: i32,
    wait: String,
    attempts: String,
    last_error: String,
}

#[derive(Template)]
#[template(path = "queued.html")]
struct QueuedPage {
    base: String,
    refresh: u64,
    queue: String,
    /// Known queues, shown when no queue was picked.
    queues: Vec<(String, String)>,
    rows: Vec<QueuedRow>,
    next: Option<String>,
}

async fn queued<P: SchedulerPool>(
    State(state): State<AppState<P>>,
    Query(q): Query<QueuedQuery>,
) -> Response {
    let config = &state.config;
    let base = config.base_path.clone();
    let Some(queue) = q.queue.filter(|s| !s.is_empty()) else {
        let o = cached_overview(&state).await;
        return match &*o {
            Err(e) => unavailable("Queues unavailable", config, e.clone()),
            Ok(o) => {
                let mut names: Vec<&str> = o.queues.iter().map(|q| q.queue.as_str()).collect();
                names.extend(o.paused.iter().map(|p| p.queue.as_str()));
                names.extend(
                    o.workers
                        .iter()
                        .flat_map(|w| w.queues.iter().map(|q| q.name.as_str())),
                );
                names.sort_unstable();
                names.dedup();
                let queues = names
                    .into_iter()
                    .map(|n| {
                        let link = format!(
                            "{base}/queued?{}",
                            serde_urlencoded::to_string([("queue", n)]).unwrap_or_default()
                        );
                        (n.to_owned(), link)
                    })
                    .collect();
                render(
                    QueuedPage {
                        base,
                        refresh: config.refresh_seconds,
                        queue: String::new(),
                        queues,
                        rows: vec![],
                        next: None,
                    },
                    StatusCode::OK,
                )
            }
        };
    };
    let Ok(name) = QueueName::try_from(queue.as_str()) else {
        return bad("invalid queue name");
    };
    let after = match (q.due, q.priority, q.due_at, q.id, q.position) {
        (None, None, None, None, None) => None,
        (Some(due), Some(priority), Some(due_at), Some(id), Some(position)) => Some(QueuedCursor {
            due,
            priority,
            due_at,
            id,
            position,
        }),
        _ => return bad("incomplete cursor"),
    };
    let limit = config.page_size;
    let start = after.as_ref().map_or(0, |c| c.position);
    let r = db::read(&state.pool, config.statement_timeout, {
        let name = name.clone();
        move |c| Box::pin(async move { store::queued_page(c, &name, after.as_ref(), limit).await })
    })
    .await;
    let page = match r {
        Ok(p) => p,
        Err(e) => return unavailable("Queue unavailable", config, e.to_string()),
    };
    let now = Utc::now();
    let rows = page
        .items
        .iter()
        .zip(start + 1..)
        .map(|(t, position)| QueuedRow {
            position,
            id: t.id.0,
            task_name: t.task_name.as_str().to_owned(),
            priority: t.priority,
            wait: if t.due_at <= now {
                span(t.due_at, now)
            } else {
                format!("due in {}", span(now, t.due_at))
            },
            attempts: format!("{} / {}", t.attempts, t.max_attempts.get()),
            last_error: t.last_error.clone().unwrap_or_default(),
        })
        .collect();
    let next = page
        .next
        .map(|c| format!("{base}/queued?{}", query(&[("queue", queue.as_str())], &c)));
    render(
        QueuedPage {
            base,
            refresh: config.refresh_seconds,
            queue,
            queues: vec![],
            rows,
            next,
        },
        StatusCode::OK,
    )
}

// ---- running / completed / failed / cancelled ----

#[derive(Deserialize)]
struct RunningQuery {
    locked_until: Option<DateTime<Utc>>,
    id: Option<i64>,
}

struct RunningRow {
    id: i64,
    task_name: String,
    queue: String,
    worker: String,
    attempt: String,
    run_time: String,
    lease: String,
}

#[derive(Template)]
#[template(path = "running.html")]
struct RunningPage {
    base: String,
    refresh: u64,
    rows: Vec<RunningRow>,
    next: Option<String>,
}

async fn running<P: SchedulerPool>(
    State(state): State<AppState<P>>,
    Query(q): Query<RunningQuery>,
) -> Response {
    let config = &state.config;
    let after = match (q.locked_until, q.id) {
        (None, None) => None,
        (Some(locked_until), Some(id)) => Some(RunningCursor { locked_until, id }),
        _ => return bad("incomplete cursor"),
    };
    let limit = config.page_size;
    let r = db::read(&state.pool, config.statement_timeout, move |c| {
        Box::pin(async move { store::running_page(c, after.as_ref(), limit).await })
    })
    .await;
    let page = match r {
        Ok(p) => p,
        Err(e) => return unavailable("Running tasks unavailable", config, e.to_string()),
    };
    let now = Utc::now();
    let rows = page
        .items
        .iter()
        .map(|t| {
            let (worker, lease) = match &t.state {
                TaskState::Running {
                    locked_by,
                    locked_until,
                } => (locked_by.as_str().to_owned(), ts(*locked_until)),
                _ => (String::new(), "—".into()),
            };
            RunningRow {
                id: t.id.0,
                task_name: t.task_name.as_str().to_owned(),
                queue: t.queue.as_str().to_owned(),
                worker,
                attempt: format!("{} / {}", t.attempts, t.max_attempts.get()),
                run_time: t.attempt.map_or("—".into(), |a| span(a.started_at, now)),
                lease,
            }
        })
        .collect();
    let next = page
        .next
        .map(|c| format!("{}/running?{}", config.base_path, query(&[], &c)));
    render(
        RunningPage {
            base: config.base_path.clone(),
            refresh: config.refresh_seconds,
            rows,
            next,
        },
        StatusCode::OK,
    )
}

#[derive(Deserialize)]
struct FinishedQuery {
    task_name: Option<String>,
    error: Option<String>,
    completed_at: Option<DateTime<Utc>>,
    id: Option<i64>,
}

struct FinishedRow {
    id: i64,
    task_name: String,
    queue: String,
    at: String,
    last_run: String,
    total: String,
    error: String,
}

#[derive(Template)]
#[template(path = "finished.html")]
struct FinishedPage {
    base: String,
    refresh: u64,
    title: &'static str,
    show_error: bool,
    task_name: String,
    error: String,
    rows: Vec<FinishedRow>,
    next: Option<String>,
}

fn finished_rows(items: &[TaskRow]) -> Vec<FinishedRow> {
    items
        .iter()
        .map(|t| {
            let at = match t.state {
                TaskState::Finished { completed_at, .. } => Some(completed_at),
                _ => None,
            };
            FinishedRow {
                id: t.id.0,
                task_name: t.task_name.as_str().to_owned(),
                queue: t.queue.as_str().to_owned(),
                at: at.map_or("—".into(), ts),
                last_run: match t.attempt {
                    Some(AttemptRun {
                        started_at,
                        state: AttemptState::Closed { finished_at, .. },
                    }) => span(started_at, finished_at),
                    _ => "—".into(),
                },
                total: at.map_or("—".into(), |c| span(t.created_at, c)),
                error: t.last_error.clone().unwrap_or_default(),
            }
        })
        .collect()
}

async fn completed<P: SchedulerPool>(
    State(state): State<AppState<P>>,
    Query(q): Query<FinishedQuery>,
) -> Response {
    let config = &state.config;
    let after = match cursor(&q) {
        Ok(c) => c,
        Err(m) => return bad(m),
    };
    let limit = config.page_size;
    let r = db::read(&state.pool, config.statement_timeout, move |c| {
        Box::pin(async move { store::done_page(c, after.as_ref(), limit).await })
    })
    .await;
    match r {
        Err(e) => unavailable("Completed tasks unavailable", config, e.to_string()),
        Ok(page) => finished_page(config, "Completed", false, &q, page),
    }
}

async fn failed<P: SchedulerPool>(
    State(state): State<AppState<P>>,
    Query(q): Query<FinishedQuery>,
    status: FailedStatus,
) -> Response {
    let config = &state.config;
    let after = match cursor(&q) {
        Ok(c) => c,
        Err(m) => return bad(m),
    };
    let task_name = match q.task_name.as_deref().filter(|s| !s.is_empty()) {
        None => None,
        Some(n) => match TaskName::try_from(n) {
            Ok(n) => Some(n),
            Err(_) => return bad("invalid task name"),
        },
    };
    let filter = FailedFilter {
        task_name,
        last_error: q.error.clone().filter(|s| !s.is_empty()),
    };
    let limit = config.page_size;
    let r = db::read(&state.pool, config.statement_timeout, move |c| {
        Box::pin(async move { store::failed_page(c, status, &filter, after.as_ref(), limit).await })
    })
    .await;
    let (title, show_error) = match status {
        FailedStatus::Dead => ("Failed", true),
        FailedStatus::Cancelled => ("Cancelled", true),
    };
    match r {
        Err(e) => unavailable("Tasks unavailable", config, e.to_string()),
        Ok(page) => finished_page(config, title, show_error, &q, page),
    }
}

fn cursor(q: &FinishedQuery) -> Result<Option<FinishedCursor>, &'static str> {
    match (q.completed_at, q.id) {
        (None, None) => Ok(None),
        (Some(completed_at), Some(id)) => Ok(Some(FinishedCursor { completed_at, id })),
        _ => Err("incomplete cursor"),
    }
}

fn finished_page(
    config: &crate::AdminConfig,
    title: &'static str,
    show_error: bool,
    q: &FinishedQuery,
    page: store::Page<TaskRow, FinishedCursor>,
) -> Response {
    let mut pairs = Vec::new();
    for (k, v) in [("task_name", &q.task_name), ("error", &q.error)] {
        if let Some(v) = v.as_deref().filter(|s| !s.is_empty()) {
            pairs.push((k, v));
        }
    }
    let path = match title {
        "Completed" => "completed",
        "Failed" => "failed",
        _ => "cancelled",
    };
    let next = page
        .next
        .as_ref()
        .map(|c| format!("{}/{path}?{}", config.base_path, query(&pairs, c)));
    render(
        FinishedPage {
            base: config.base_path.clone(),
            refresh: config.refresh_seconds,
            title,
            show_error,
            task_name: q.task_name.clone().unwrap_or_default(),
            error: q.error.clone().unwrap_or_default(),
            rows: finished_rows(&page.items),
            next,
        },
        StatusCode::OK,
    )
}

// ---- task detail ----

struct AttemptRow {
    no: i32,
    worker: String,
    started: String,
    finished: String,
    run_time: String,
    outcome: String,
    error: String,
}

struct ActionRow {
    at: String,
    action: String,
    operator: String,
    reason: String,
    affected: i64,
}

#[derive(Template)]
#[template(path = "task.html")]
struct TaskPage {
    base: String,
    refresh: u64,
    t: store::TaskDetail,
    status: &'static str,
    created: String,
    updated: String,
    due: String,
    completed: String,
    locked_by: String,
    locked_until: String,
    payload: String,
    attempts: Vec<AttemptRow>,
    actions: Vec<ActionRow>,
}

async fn task<P: SchedulerPool>(State(state): State<AppState<P>>, Path(id): Path<i64>) -> Response {
    let config = &state.config;
    let r = db::read(&state.pool, config.statement_timeout, move |c| {
        Box::pin(async move { store::task_detail(c, TaskId(id)).await })
    })
    .await;
    let d = match r {
        Err(e) => return unavailable("Task unavailable", config, e.to_string()),
        Ok(None) => {
            return render(
                ErrorPage {
                    title: "Task not found",
                    base: config.base_path.clone(),
                    refresh: config.refresh_seconds,
                    error: format!("no task with id {id}"),
                },
                StatusCode::NOT_FOUND,
            );
        }
        Ok(Some(d)) => d,
    };
    let v = &d.task;
    let none = || String::from("—");
    let (completed, locked_by, locked_until) = match &v.state {
        TaskState::Queued => (none(), none(), none()),
        TaskState::Running {
            locked_by,
            locked_until,
        } => (none(), locked_by.as_str().to_owned(), ts(*locked_until)),
        TaskState::Finished { completed_at, .. } => (ts(*completed_at), none(), none()),
    };
    let page = TaskPage {
        base: config.base_path.clone(),
        refresh: config.refresh_seconds,
        status: v.state.status().as_str(),
        created: ts(v.created_at),
        updated: ts(v.updated_at),
        due: ts(v.due_at),
        completed,
        locked_by,
        locked_until,
        payload: serde_json::to_string_pretty(&v.payload).unwrap_or_default(),
        attempts: d
            .attempts
            .iter()
            .map(|a| {
                let (finished, run_time, outcome) = match a.state {
                    AttemptState::Open => (none(), none(), none()),
                    AttemptState::Closed {
                        finished_at,
                        outcome,
                    } => (
                        ts(finished_at),
                        span(a.started_at, finished_at),
                        outcome.as_str().to_owned(),
                    ),
                };
                AttemptRow {
                    no: a.attempt_no,
                    worker: a.worker_id.as_str().to_owned(),
                    started: ts(a.started_at),
                    finished,
                    run_time,
                    outcome,
                    error: a.error.clone().unwrap_or_default(),
                }
            })
            .collect(),
        actions: d
            .actions
            .iter()
            .map(|a| ActionRow {
                at: ts(a.at),
                action: a.action.as_str().to_owned(),
                operator: a.operator.clone(),
                reason: a.reason.clone(),
                affected: a.affected,
            })
            .collect(),
        t: d,
    };
    render(page, StatusCode::OK)
}
