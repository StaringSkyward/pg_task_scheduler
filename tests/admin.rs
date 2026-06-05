#![cfg(feature = "axum")]
mod common;
use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use chrono::Utc;
use common::TestDb;
use tower::ServiceExt; // for `oneshot`
use uuid::Uuid;

/// Send one request through the router and return the response status. `oneshot`
/// consumes the service, so callers pass `app.clone()` when sending twice.
async fn status_of(app: Router, method: Method, uri: String) -> StatusCode {
    app.oneshot(
        Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap()
    .status()
}

#[tokio::test]
async fn router_builds() {
    let db = TestDb::new().await;
    let _ = pg_task_scheduler::admin::router(db.pool.clone());
    db.cleanup().await;
}

#[tokio::test]
async fn pause_existing_204_missing_404() {
    let db = TestDb::new().await;
    let id = db
        .insert_job("j", "*/5 * * * *", Utc::now() + chrono::Duration::hours(1))
        .await;
    let app = pg_task_scheduler::admin::router(db.pool.clone());
    assert_eq!(
        status_of(app.clone(), Method::POST, format!("/jobs/{id}/pause")).await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        status_of(app, Method::POST, format!("/jobs/{}/pause", Uuid::new_v4())).await,
        StatusCode::NOT_FOUND
    );
    db.cleanup().await;
}

#[tokio::test]
async fn resume_existing_204_missing_404() {
    let db = TestDb::new().await;
    let id = db
        .insert_job("j", "*/5 * * * *", Utc::now() + chrono::Duration::hours(1))
        .await;
    let app = pg_task_scheduler::admin::router(db.pool.clone());
    assert_eq!(
        status_of(app.clone(), Method::POST, format!("/jobs/{id}/resume")).await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        status_of(
            app,
            Method::POST,
            format!("/jobs/{}/resume", Uuid::new_v4())
        )
        .await,
        StatusCode::NOT_FOUND
    );
    db.cleanup().await;
}

#[tokio::test]
async fn delete_existing_204_then_404() {
    let db = TestDb::new().await;
    let id = db
        .insert_job("j", "*/5 * * * *", Utc::now() + chrono::Duration::hours(1))
        .await;
    let app = pg_task_scheduler::admin::router(db.pool.clone());
    // First delete removes the row → 204.
    assert_eq!(
        status_of(app.clone(), Method::DELETE, format!("/jobs/{id}")).await,
        StatusCode::NO_CONTENT
    );
    // Deleting again → 404: proves the first call actually deleted (not a no-op/pause).
    assert_eq!(
        status_of(app, Method::DELETE, format!("/jobs/{id}")).await,
        StatusCode::NOT_FOUND
    );
    db.cleanup().await;
}
