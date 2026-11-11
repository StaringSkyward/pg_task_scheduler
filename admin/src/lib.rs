//! Admin UI and JSON API for `pg_task_scheduler`.
//!
//! [`router`] serves every route behind two checks: a request without an [`Operator`]
//! extension gets 401 (GET included), and a cross-site POST gets 403 (see `csrf`).
//! Authentication is any tower layer that inserts [`Operator`]; [`auth`] ships two.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::FromRequestParts;
use axum::http::StatusCode;
use axum::http::request::Parts;
use axum::routing::get;
use axum::{Router, middleware};
use pg_task_scheduler::SchedulerPool;
use pg_task_scheduler::store::OverviewParams;

mod api;
pub mod auth;
mod csrf;
pub mod db;

/// The authenticated identity, inserted as a request extension by the auth layer.
/// Audited actions record it as their operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Operator(pub String);

#[axum::async_trait]
impl<S: Send + Sync> FromRequestParts<S> for Operator {
    type Rejection = StatusCode;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, StatusCode> {
        parts
            .extensions
            .get::<Operator>()
            .cloned()
            .ok_or(StatusCode::UNAUTHORIZED)
    }
}

/// When a queue's backlog or lack of progress turns its status amber.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueThresholds {
    pub backlog_after: Duration,
    pub stall_after: Duration,
}

impl Default for QueueThresholds {
    fn default() -> Self {
        Self {
            backlog_after: Duration::from_secs(300),
            stall_after: Duration::from_secs(300),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Thresholds {
    pub default: QueueThresholds,
    pub per_queue: HashMap<String, QueueThresholds>,
}

impl Thresholds {
    pub fn for_queue(&self, q: &str) -> &QueueThresholds {
        self.per_queue.get(q).unwrap_or(&self.default)
    }
}

#[derive(Debug, Clone)]
pub struct AdminConfig {
    /// Prefix the router is mounted under, for links and redirects (e.g. `/admin`).
    pub base_path: String,
    /// The browser-facing origin (e.g. `https://admin.example.com`) when a proxy
    /// rewrites `Host`; a POST whose `Origin` equals it passes the CSRF check.
    pub public_origin: Option<String>,
    /// Page auto-refresh interval; 0 turns it off.
    pub refresh_seconds: u64,
    pub overview_cache: Duration,
    pub page_size: i64,
    /// `statement_timeout` for every page read (see [`db::read`]).
    pub statement_timeout: Duration,
    pub overview: OverviewParams,
    pub thresholds: Thresholds,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            base_path: String::new(),
            public_origin: None,
            refresh_seconds: 10,
            overview_cache: Duration::from_secs(5),
            page_size: 50,
            statement_timeout: Duration::from_secs(5),
            overview: OverviewParams::default(),
            thresholds: Thresholds::default(),
        }
    }
}

#[derive(Clone)]
struct AppState<P> {
    pool: P,
    config: Arc<AdminConfig>,
}

/// The admin routes. Every route requires an [`Operator`] (401 without one) and every
/// POST passes the CSRF check (403). Add authentication with [`auth::with_basic_auth`],
/// [`auth::with_trusted_header`], or any layer that inserts [`Operator`].
pub fn router<P: SchedulerPool>(pool: P, config: AdminConfig) -> Router {
    let public_origin = config.public_origin.clone();
    let state = AppState {
        pool,
        config: Arc::new(config),
    };
    // Placeholder until the overview page lands.
    Router::new()
        .route("/", get(|| async { "ok" }))
        .merge(api::routes())
        // The last layer added runs first: the operator check, then CSRF.
        .route_layer(middleware::from_fn_with_state(public_origin, csrf::guard))
        .route_layer(middleware::from_fn(require_operator))
        .with_state(state)
}

async fn require_operator(
    _: Operator,
    req: axum::extract::Request,
    next: middleware::Next,
) -> axum::response::Response {
    next.run(req).await
}
