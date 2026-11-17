#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::FromRequestParts;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::http::header;
use axum::http::request::Parts;
use axum::middleware;
use axum::response::Response;
use pg_task_scheduler::SchedulerPool;
use pg_task_scheduler::store::OverviewParams;

mod actions;
mod api;
pub mod auth;
mod cache;
mod csrf;
pub mod db;
pub mod health;
mod lists;
mod pages;

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
    /// The queue reads `backlog` once its oldest due task has waited longer (default 5 min).
    pub backlog_after: Duration,
    /// A queue with due tasks reads `stalled` once no serving worker has finished one of
    /// its tasks for this long (default 5 min).
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

/// Status thresholds for every queue, with per-queue overrides.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Thresholds {
    /// Used for queues with no override.
    pub default: QueueThresholds,
    /// Overrides, by queue name.
    pub per_queue: HashMap<String, QueueThresholds>,
}

impl Thresholds {
    /// The thresholds that apply to queue `q`.
    pub fn for_queue(&self, q: &str) -> &QueueThresholds {
        self.per_queue.get(q).unwrap_or(&self.default)
    }
}

/// Settings for [`router`]. Start from `AdminConfig::default()` and set `base_path`.
#[derive(Debug, Clone)]
pub struct AdminConfig {
    /// Prefix the router is mounted under, for links and redirects (e.g. `/admin`).
    pub base_path: String,
    /// The browser-facing origin (e.g. `https://admin.example.com`) when a proxy
    /// rewrites `Host`; a POST whose `Origin` equals it passes the CSRF check.
    pub public_origin: Option<String>,
    /// Page auto-refresh interval; 0 turns it off.
    pub refresh_seconds: u64,
    /// The overview is computed at most once per this, shared by every viewer (default 5 s).
    pub overview_cache: Duration,
    /// Rows per list page (default 50).
    pub page_size: i64,
    /// `statement_timeout` for every page read (see [`db::read`]).
    pub statement_timeout: Duration,
    /// Bounds for the overview's queries.
    pub overview: OverviewParams,
    /// When a queue's status turns amber.
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
    cache: Arc<cache::OverviewCache>,
}

/// The admin routes. Every route requires an [`Operator`] (401 without one) and every
/// request other than GET, HEAD and OPTIONS passes the CSRF check (403). Every response
/// forbids framing. Add authentication with [`auth::with_basic_auth`],
/// [`auth::with_trusted_header`], or any layer that inserts [`Operator`].
pub fn router<P: SchedulerPool>(pool: P, config: AdminConfig) -> Router {
    let public_origin = config.public_origin.clone();
    let state = AppState {
        pool,
        cache: Arc::new(cache::OverviewCache::new(config.overview_cache)),
        config: Arc::new(config),
    };
    Router::new()
        .merge(pages::routes())
        .merge(api::routes())
        // The last layer added runs first: the operator check, then CSRF.
        .route_layer(middleware::from_fn_with_state(public_origin, csrf::guard))
        .route_layer(middleware::from_fn(require_operator))
        .with_state(state)
        // `layer`, not `route_layer`: unmatched paths and every error response too.
        .layer(middleware::map_response(no_framing))
}

/// Anti-clickjacking: no page may be shown in a frame. The auth layers add it too, so their
/// own 401s carry it.
pub(crate) async fn no_framing(mut res: Response) -> Response {
    let h = res.headers_mut();
    h.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    h.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static("frame-ancestors 'none'"),
    );
    res
}

async fn require_operator(
    _: Operator,
    req: axum::extract::Request,
    next: middleware::Next,
) -> axum::response::Response {
    next.run(req).await
}
