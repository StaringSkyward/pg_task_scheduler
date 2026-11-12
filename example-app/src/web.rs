//! The web role: the demo page, the enqueue buttons, and the embedded admin UI.

use std::error::Error;
use std::time::Duration;

use askama::Template;
use axum::Form;
use axum::Router;
use axum::extract::Query;
use axum::extract::State;
use axum::response::Html;
use axum::response::IntoResponse;
use axum::response::Redirect;
use axum::response::Response;
use axum::routing::get;
use axum::routing::post;
use diesel_async::AsyncConnection;
use pg_task_scheduler::CreateJob;
use pg_task_scheduler::CronExpression;
use pg_task_scheduler::EnqueueOptions;
use pg_task_scheduler::JobLifecycle;
use pg_task_scheduler::MaxAttempts;
use pg_task_scheduler::SchedulerError;
use pg_task_scheduler::Task;
use pg_task_scheduler::TaskKey;
use pg_task_scheduler::TaskName;
use pg_task_scheduler::enqueue;
use pg_task_scheduler::jobs;
use pg_task_scheduler_admin::AdminConfig;
use pg_task_scheduler_admin::auth::BasicUser;
use pg_task_scheduler_admin::auth::{self};
use pg_task_scheduler_admin::router;
use serde::Deserialize;
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::Pool;
use crate::panel::Panel;
use crate::panel::{self};
use crate::tasks::BulkImport;
use crate::tasks::BulkImportArgs;
use crate::tasks::Digest;
use crate::tasks::DigestArgs;
use crate::tasks::EmailArgs;
use crate::tasks::FlakyArgs;
use crate::tasks::FlakyDemo;
use crate::tasks::OrderArgs;
use crate::tasks::PlaceOrder;
use crate::tasks::SendEmail;
use crate::tasks::random_between;

/// What the banner shows after a button press. It travels in the query string of
/// the 303 redirect, so reloading the page never re-submits the form.
#[derive(Serialize, Deserialize, Default)]
pub struct Flash {
    pub kind: Option<FlashKind>,
    pub text: Option<String>,
    pub task: Option<i64>,
}

/// Banner colours: green, blue, amber and red.
#[derive(Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum FlashKind {
    Enqueued,
    Existing,
    Conflict,
    Error,
}

impl Flash {
    fn new(kind: FlashKind, text: String, task: Option<i64>) -> Self {
        Flash {
            kind: Some(kind),
            text: Some(text),
            task,
        }
    }

    /// Derived, never read from the query, so a crafted URL cannot make the
    /// banner link anywhere else.
    fn link(&self) -> String {
        match self.task {
            Some(id) => format!("/admin/tasks/{id}"),
            None => "/admin".to_string(),
        }
    }
}

impl FlashKind {
    /// Only banners about a task that exists link to the admin UI.
    fn watchable(self) -> bool {
        matches!(self, FlashKind::Enqueued | FlashKind::Existing)
    }

    fn css(self) -> &'static str {
        match self {
            FlashKind::Enqueued => "enqueued",
            FlashKind::Existing => "existing",
            FlashKind::Conflict => "conflict",
            FlashKind::Error => "error",
        }
    }
}

fn back(flash: Flash) -> Redirect {
    let query = serde_urlencoded::to_string(&flash).unwrap_or_default();
    Redirect::to(&format!("/?{query}"))
}

#[derive(Template)]
#[template(path = "index.html")]
pub struct IndexPage {
    pub flash: Option<Flash>,
    /// `None` when the database read failed: the panel says so.
    pub panel: Option<Panel>,
}

async fn load_panel(pool: &Pool) -> Result<Panel, BoxError> {
    Ok(panel::load(&mut *pool.get().await?).await?)
}

async fn index(State(pool): State<Pool>, flash: Option<Query<Flash>>) -> Response {
    let panel = load_panel(&pool)
        .await
        .map_err(|e| tracing::warn!("loading the database panel: {e}"))
        .ok();
    // A malformed query is no banner, not a 400.
    match (IndexPage {
        flash: flash.map(|Query(f)| f),
        panel,
    })
    .render()
    {
        Ok(html) => Html(html).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

type BoxError = Box<dyn Error + Send + Sync>;

/// Turns a handler's result into the redirect: an error becomes the red banner.
fn finish(result: Result<Flash, BoxError>) -> Redirect {
    back(result.unwrap_or_else(|e| Flash::new(FlashKind::Error, e.to_string(), None)))
}

async fn enqueue_emails(pool: &Pool) -> Result<Flash, BoxError> {
    let mut conn = pool.get().await?;
    // One transaction: all 100 tasks exist, or none do.
    conn.transaction::<_, BoxError, _>(async |c| {
        for i in 1..=100 {
            let args = EmailArgs {
                to: format!("user-{i}@example.com"),
                subject: format!("Welcome, user {i}"),
            };
            let key = TaskKey::try_from(format!("email:{}", Uuid::new_v4()))?;
            enqueue::<SendEmail>(c, key, args, EnqueueOptions::immediate()).await?;
        }
        Ok(())
    })
    .await?;
    let text = format!("Enqueued 100 \"{}\" tasks.", SendEmail::NAME);
    Ok(Flash::new(FlashKind::Enqueued, text, None))
}

async fn enqueue_bulk_import(pool: &Pool) -> Result<Flash, BoxError> {
    let mut conn = pool.get().await?;
    let key = TaskKey::try_from(format!("bulk-import:{}", Uuid::new_v4()))?;
    let task = enqueue::<BulkImport>(
        &mut conn,
        key,
        BulkImportArgs { rows: 10_000 },
        EnqueueOptions::immediate(),
    )
    .await?;
    let text = format!("Enqueued \"{}\" task #{}.", BulkImport::NAME, task.id.0);
    Ok(Flash::new(FlashKind::Enqueued, text, Some(task.id.0)))
}

async fn enqueue_flaky(pool: &Pool) -> Result<Flash, BoxError> {
    let mut conn = pool.get().await?;
    // One transaction: all 10 tasks exist, or none do.
    conn.transaction::<_, BoxError, _>(async |c| {
        for _ in 0..10 {
            let key = TaskKey::try_from(format!("flaky:{}", Uuid::new_v4()))?;
            enqueue::<FlakyDemo>(c, key, FlakyArgs {}, EnqueueOptions::immediate()).await?;
        }
        Ok(())
    })
    .await?;
    let text = format!(
        "Enqueued 10 \"{}\" tasks (they fail on purpose).",
        FlakyDemo::NAME
    );
    Ok(Flash::new(FlashKind::Enqueued, text, None))
}

async fn enqueue_orders(pool: &Pool) -> Result<Flash, BoxError> {
    let mut conn = pool.get().await?;
    // One transaction: all 5 tasks exist, or none do.
    conn.transaction::<_, BoxError, _>(async |c| {
        for _ in 0..5 {
            let key = TaskKey::try_from(format!("order:{}", Uuid::new_v4()))?;
            let args = OrderArgs {
                amount_cents: random_between(500, 5000) as i32,
            };
            enqueue::<PlaceOrder>(c, key, args, EnqueueOptions::immediate()).await?;
        }
        Ok(())
    })
    .await?;
    let text = format!("Enqueued 5 \"{}\" tasks.", PlaceOrder::NAME);
    Ok(Flash::new(FlashKind::Enqueued, text, None))
}

/// The Order input is only a hint (`type=number`), and the number ends up in a
/// task key, so check it here: decimal digits only, 1 to 999,999.
pub fn parse_order_no(input: &str) -> Option<u32> {
    let s = input.trim();
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok().filter(|n| (1..=999_999).contains(n))
}

#[derive(Deserialize)]
struct ReceiptForm {
    // Missing fields default to "", so they take the red-banner path.
    #[serde(default)]
    order: String,
    /// `same` or `changed`.
    #[serde(default)]
    variant: String,
}

async fn enqueue_receipt(pool: &Pool, form: ReceiptForm) -> Result<Flash, BoxError> {
    let Some(n) = parse_order_no(&form.order) else {
        return Ok(Flash::new(
            FlashKind::Error,
            "Order must be a whole number from 1 to 999999.".into(),
            None,
        ));
    };
    let key = format!("receipt:order-{n}");
    let mut subject = format!("Receipt for order {n}");
    if form.variant == "changed" {
        subject.push_str(" (changed)");
    }
    let args = EmailArgs {
        to: format!("customer-{n}@example.com"),
        subject,
    };
    let mut conn = pool.get().await?;
    let result = enqueue::<SendEmail>(
        &mut conn,
        TaskKey::try_from(key.clone())?,
        args,
        EnqueueOptions::immediate(),
    )
    .await;
    Ok(match result {
        Ok(task) if task.inserted => Flash::new(
            FlashKind::Enqueued,
            format!("Enqueued receipt task #{} for key {key}.", task.id.0),
            Some(task.id.0),
        ),
        Ok(task) => Flash::new(
            FlashKind::Existing,
            format!(
                "Already enqueued: key {key} is task #{}. No duplicate was created (inserted = false).",
                task.id.0
            ),
            Some(task.id.0),
        ),
        Err(SchedulerError::IdempotencyConflict(_)) => Flash::new(
            FlashKind::Conflict,
            format!(
                "Rejected: key {key} already exists with a different payload (idempotency conflict). The original task is unchanged."
            ),
            None,
        ),
        Err(e) => return Err(e.into()),
    })
}

async fn orders(State(pool): State<Pool>) -> Redirect {
    finish(enqueue_orders(&pool).await)
}

async fn receipt(State(pool): State<Pool>, Form(form): Form<ReceiptForm>) -> Redirect {
    finish(enqueue_receipt(&pool, form).await)
}

async fn emails(State(pool): State<Pool>) -> Redirect {
    finish(enqueue_emails(&pool).await)
}

async fn bulk_import(State(pool): State<Pool>) -> Redirect {
    finish(enqueue_bulk_import(&pool).await)
}

async fn flaky(State(pool): State<Pool>) -> Redirect {
    finish(enqueue_flaky(&pool).await)
}

pub async fn run(
    app_pool: Pool,
    admin_pool: Pool,
    admin_user: String,
    admin_password: String,
) -> Result<(), Box<dyn Error>> {
    // The digest job is created at startup. `ensure_job` is safe on every
    // restart: it never resurrects a job an operator paused.
    let job = CreateJob::new(
        TaskName::try_from(Digest::NAME)?,
        CronExpression::parse("*/5 * * * *")?,
        MaxAttempts::try_from(3u32)?,
        JobLifecycle::Active,
        DigestArgs {},
    )?;
    jobs::ensure_job(&mut *app_pool.get().await?, job).await?;

    // Fast refresh and a short cache, so the demo shows batches draining. The
    // defaults (10 s refresh, 5 s cache) suit production.
    let admin = router(
        admin_pool,
        AdminConfig {
            base_path: "/admin".into(),
            refresh_seconds: 2,
            overview_cache: Duration::from_secs(1),
            ..Default::default()
        },
    );
    // A real deployment stores only the hash (see `pg-task-admin hash-password`),
    // not the password. The demo hashes at startup to keep the .env simple.
    let admin = auth::with_basic_auth(
        admin,
        vec![BasicUser {
            name: admin_user,
            password_hash: auth::hash_password(&admin_password),
        }],
    )?;

    // The relay stops when the shutdown signal fires.
    let cancel = CancellationToken::new();
    tokio::spawn(crate::outbox::relay(app_pool.clone(), cancel.clone()));

    let app = Router::new()
        .route("/", get(index))
        .route("/enqueue/emails", post(emails))
        .route("/enqueue/bulk-import", post(bulk_import))
        .route("/enqueue/flaky", post(flaky))
        .route("/enqueue/orders", post(orders))
        .route("/enqueue/receipt", post(receipt))
        .with_state(app_pool)
        // `/admin/` is a 404 under `.nest`, so send it to `/admin`.
        .route("/admin/", get(|| async { Redirect::to("/admin") }))
        .nest("/admin", admin);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    tracing::info!(
        "demo at http://localhost:8080 — admin login is ADMIN_USER / ADMIN_PASSWORD from .env"
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            crate::shutdown_signal().await;
            cancel.cancel();
        })
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use askama::Template;

    use super::*;

    #[test]
    fn parse_order_no_accepts_only_1_to_999999() {
        assert_eq!(parse_order_no("1001"), Some(1001));
        assert_eq!(parse_order_no(" 42 "), Some(42));
        assert_eq!(parse_order_no("999999"), Some(999_999));
        for bad in [
            "0",
            "-1",
            "+5",
            "1000000",
            "abc",
            "",
            "1e3",
            "12.5",
            "99999999999999999999",
        ] {
            assert_eq!(parse_order_no(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn index_page_escapes_flash_text() {
        let page = IndexPage {
            flash: Some(Flash {
                kind: Some(FlashKind::Error),
                text: Some("<script>alert(1)</script>".into()),
                task: None,
            }),
            panel: None,
        };
        let html = page.render().unwrap();
        assert!(html.contains("&lt;script&gt;alert(1)&lt;/script&gt;"));
        assert!(!html.contains("<script>alert(1)"));
        assert!(!html.contains("Watch in the admin UI"));
    }

    #[test]
    fn enqueued_banner_links_to_the_admin_ui() {
        let page = IndexPage {
            flash: Some(Flash::new(FlashKind::Enqueued, "ok".into(), None)),
            panel: None,
        };
        assert!(page.render().unwrap().contains(r#"href="/admin""#));
    }
}
