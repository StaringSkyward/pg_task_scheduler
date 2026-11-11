//! Server-rendered pages. Templates live in `admin/templates/`; askama escapes every
//! interpolated value.
use std::collections::BTreeMap;

use askama::Template;
use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use chrono::{DateTime, Utc};
use pg_task_scheduler::SchedulerPool;
use pg_task_scheduler::store::{self, Count, Overview, QueueOverview, WorkerView};

use crate::health::{QueueStatus, fmt_duration, verdict};
use crate::{AppState, db};

pub(crate) fn routes<P: SchedulerPool>() -> Router<AppState<P>> {
    Router::new()
        .route("/", get(overview::<P>))
        // Home link target: axum's `nest("/admin", ..)` mounts `/` at `/admin` only, so
        // `{base}/` would 404 under a prefix.
        .route("/overview", get(overview::<P>))
        .route(
            "/assets/admin.css",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
                    include_str!("../assets/admin.css"),
                )
            }),
        )
        .route(
            "/assets/admin.js",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
                    include_str!("../assets/admin.js"),
                )
            }),
        )
}

struct Row {
    name: String,
    status: &'static str,
    reason: String,
    workers: usize,
    slots: String,
    due: String,
    oldest: String,
    done: String,
    dead: String,
}

struct Group {
    task_name: String,
    error: String,
    dead: String,
    attempts: String,
}

struct ActionRow {
    at: String,
    operator: String,
    action: String,
    affected: i64,
    reason: String,
}

#[derive(Template)]
#[template(path = "overview.html")]
struct OverviewPage {
    base: String,
    refresh: u64,
    /// Queues that need attention now (stalled with no live worker, or paused).
    alert: Vec<(String, &'static str)>,
    alert_key: String,
    live: usize,
    lost: usize,
    last_seen: String,
    rows: Vec<Row>,
    done_header: String,
    dead_header: String,
    groups: Vec<Group>,
    no_handler: Vec<(String, String)>,
    actions: Vec<ActionRow>,
}

#[derive(Template)]
#[template(path = "error.html")]
struct ErrorPage {
    base: String,
    refresh: u64,
    error: String,
}

fn count(c: Count) -> String {
    let digits = c.value.to_string();
    let mut out = String::new();
    for (i, ch) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    if c.capped {
        out.push('+');
    }
    out
}

fn windowed_header(
    label: &str,
    covered: std::time::Duration,
    window: std::time::Duration,
) -> String {
    if covered < window {
        format!("{label} (last {})", fmt_duration(covered))
    } else {
        format!("{label} ({})", fmt_duration(window))
    }
}

fn empty_queue(name: &pg_task_scheduler::QueueName) -> QueueOverview {
    let zero = Count {
        value: 0,
        capped: false,
    };
    QueueOverview {
        queue: name.clone(),
        queued: zero,
        due: zero,
        oldest_due_age: None,
        running: 0,
        done: zero,
        dead: zero,
    }
}

fn build(o: &Overview, config: &crate::AdminConfig) -> OverviewPage {
    let now = o.generated_at;
    let live: Vec<&WorkerView> = o.workers.iter().filter(|w| !w.lost).collect();
    // One row per queue with queued or running tasks, a live worker, or a pause.
    let mut queues: BTreeMap<&str, QueueOverview> = BTreeMap::new();
    for q in &o.queues {
        queues.insert(q.queue.as_str(), q.clone());
    }
    for wq in live.iter().flat_map(|w| &w.queues) {
        queues
            .entry(wq.name.as_str())
            .or_insert_with(|| empty_queue(&wq.name));
    }
    for p in &o.paused {
        queues
            .entry(p.queue.as_str())
            .or_insert_with(|| empty_queue(&p.queue));
    }

    let mut rows = Vec::new();
    let mut alert = Vec::new();
    for (name, q) in &queues {
        let serving: Vec<&WorkerView> = live
            .iter()
            .copied()
            .filter(|w| w.queues.iter().any(|wq| wq.name.as_str() == *name))
            .collect();
        let paused = o.paused.iter().any(|p| p.queue.as_str() == *name);
        let unhandled = o
            .no_handler
            .iter()
            .find(|n| n.queue.as_str() == *name)
            .map_or(0, |n| n.task_names.len());
        let v = verdict(
            q,
            &serving,
            paused,
            unhandled,
            config.thresholds.for_queue(name),
            now,
        );
        if v.status == QueueStatus::Paused
            || (v.status == QueueStatus::Stalled && v.reason == "no live worker")
        {
            alert.push((name.to_string(), v.status.label()));
        }
        let slots: i64 = serving
            .iter()
            .flat_map(|w| w.queues.iter().filter(|wq| wq.name.as_str() == *name))
            .map(|wq| wq.limit)
            .sum();
        rows.push(Row {
            name: name.to_string(),
            status: v.status.label(),
            reason: v.reason,
            workers: serving.len(),
            slots: format!("{} / {slots}", q.running),
            due: count(q.due),
            oldest: q.oldest_due_age.map_or("-".into(), fmt_duration),
            done: count(q.done),
            dead: count(q.dead),
        });
    }
    // BTreeMap order is already sorted.
    let alert_key = alert
        .iter()
        .map(|(n, _)| n.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let ago = |t: DateTime<Utc>| {
        format!(
            "{} ago",
            fmt_duration((now - t).to_std().unwrap_or_default())
        )
    };

    OverviewPage {
        base: config.base_path.clone(),
        refresh: config.refresh_seconds,
        alert,
        alert_key,
        live: live.len(),
        lost: o.workers.len() - live.len(),
        last_seen: o
            .workers
            .iter()
            .map(|w| w.last_seen_at)
            .max()
            .map_or(String::new(), ago),
        rows,
        done_header: windowed_header("Done", o.done_covered, o.window),
        dead_header: windowed_header("Dead", o.dead_covered, o.window),
        groups: o
            .failure_groups
            .iter()
            .map(|g| Group {
                task_name: g.task_name.as_str().into(),
                error: g.error.clone(),
                dead: count(g.dead),
                attempts: count(g.failed_attempts),
            })
            .collect(),
        no_handler: o
            .no_handler
            .iter()
            .map(|n| {
                let names: Vec<&str> = n.task_names.iter().map(|t| t.as_str()).collect();
                (n.queue.as_str().to_owned(), names.join(", "))
            })
            .collect(),
        actions: o
            .recent_actions
            .iter()
            .map(|a| ActionRow {
                at: a.at.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
                operator: a.operator.clone(),
                action: format!("{:?}", a.action).to_lowercase(),
                affected: a.affected,
                reason: a.reason.clone(),
            })
            .collect(),
    }
}

fn render(t: impl Template, status: StatusCode) -> Response {
    match t.render() {
        Ok(html) => (status, Html(html)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn overview<P: SchedulerPool>(State(state): State<AppState<P>>) -> Response {
    let config = &state.config;
    let params = config.overview.clone();
    let timeout = config.statement_timeout;
    let pool = &state.pool;
    let result = state
        .cache
        .get(async {
            db::read(pool, timeout, move |c| {
                Box::pin(async move { store::overview(c, &params).await })
            })
            .await
            .map_err(|e| e.to_string())
        })
        .await;
    match &*result {
        Ok(o) => render(build(o, config), StatusCode::OK),
        Err(error) => render(
            ErrorPage {
                base: config.base_path.clone(),
                refresh: config.refresh_seconds,
                error: error.clone(),
            },
            StatusCode::SERVICE_UNAVAILABLE,
        ),
    }
}
