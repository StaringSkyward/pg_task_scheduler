//! The web role: the demo page, the enqueue buttons, and the embedded admin UI.

use std::error::Error;
use std::time::Duration;

use askama::Template;
use axum::Router;
use axum::extract::{Query, State};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use diesel_async::AsyncConnection;
use pg_task_scheduler::{
    CreateJob, CronExpression, EnqueueOptions, JobLifecycle, MaxAttempts, Task, TaskKey, TaskName,
    enqueue, jobs,
};
use pg_task_scheduler_admin::auth::{self, BasicUser};
use pg_task_scheduler_admin::{AdminConfig, router};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::Pool;
use crate::tasks::{
    BulkImport, BulkImportArgs, Digest, DigestArgs, EmailArgs, FlakyArgs, FlakyDemo, SendEmail,
};

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
}

async fn index(flash: Option<Query<Flash>>) -> Response {
    // A malformed query is no banner, not a 400.
    match (IndexPage {
        flash: flash.map(|Query(f)| f),
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
    for _ in 0..10 {
        let key = TaskKey::try_from(format!("flaky:{}", Uuid::new_v4()))?;
        enqueue::<FlakyDemo>(&mut conn, key, FlakyArgs {}, EnqueueOptions::immediate()).await?;
    }
    let text = format!(
        "Enqueued 10 \"{}\" tasks (they fail on purpose).",
        FlakyDemo::NAME
    );
    Ok(Flash::new(FlashKind::Enqueued, text, None))
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

    let app = Router::new()
        .route("/", get(index))
        .route("/enqueue/emails", post(emails))
        .route("/enqueue/bulk-import", post(bulk_import))
        .route("/enqueue/flaky", post(flaky))
        .with_state(app_pool)
        // `/admin/` is a 404 under `.nest`, so send it to `/admin`.
        .route("/admin/", get(|| async { Redirect::to("/admin") }))
        .nest("/admin", admin);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    tracing::info!(
        "demo at http://localhost:8080 — admin login is ADMIN_USER / ADMIN_PASSWORD from .env"
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(crate::shutdown_signal())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use askama::Template;

    #[test]
    fn index_page_escapes_flash_text() {
        let page = IndexPage {
            flash: Some(Flash {
                kind: Some(FlashKind::Error),
                text: Some("<script>alert(1)</script>".into()),
                task: None,
            }),
        };
        let html = page.render().unwrap();
        assert!(html.contains("&lt;script&gt;alert(1)&lt;/script&gt;"));
        assert!(!html.contains("<script>alert(1)"));
        assert!(html.contains(r#"href="/admin""#));
    }
}
