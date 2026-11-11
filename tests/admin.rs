#![cfg(feature = "axum")]
mod common;
use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use common::TestDb;
use tower::ServiceExt; // for `oneshot`

struct AdminTask;

impl pg_task_scheduler::Task for AdminTask {
    const NAME: &'static str = "admin-task";
    type Args = serde_json::Value;
}

/// Send one request through the router and return the response status. `oneshot`
/// consumes the service, so callers pass `app.clone()` when sending twice.
#[cfg(feature = "cron")]
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

#[cfg(feature = "cron")]
#[tokio::test]
async fn pause_existing_204_missing_404() {
    let db = TestDb::new().await;
    let id = db
        .insert_job(
            "j",
            "*/5 * * * *",
            chrono::Utc::now() + chrono::Duration::hours(1),
        )
        .await;
    let app = pg_task_scheduler::admin::router(db.pool.clone());
    assert_eq!(
        status_of(app.clone(), Method::POST, format!("/jobs/{id}/pause")).await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        status_of(
            app,
            Method::POST,
            format!("/jobs/{}/pause", uuid::Uuid::new_v4())
        )
        .await,
        StatusCode::NOT_FOUND
    );
    db.cleanup().await;
}

#[cfg(feature = "cron")]
#[tokio::test]
async fn resume_existing_204_missing_404() {
    let db = TestDb::new().await;
    let id = db
        .insert_job(
            "j",
            "*/5 * * * *",
            chrono::Utc::now() + chrono::Duration::hours(1),
        )
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
            format!("/jobs/{}/resume", uuid::Uuid::new_v4())
        )
        .await,
        StatusCode::NOT_FOUND
    );
    db.cleanup().await;
}

#[cfg(feature = "cron")]
#[tokio::test]
async fn delete_existing_204_then_404() {
    let db = TestDb::new().await;
    let id = db
        .insert_job(
            "j",
            "*/5 * * * *",
            chrono::Utc::now() + chrono::Duration::hours(1),
        )
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

async fn enqueue_one(db: &TestDb) -> pg_task_scheduler::TaskId {
    let mut conn = db.pool.get().await.unwrap();
    pg_task_scheduler::enqueue::<AdminTask>(
        &mut conn,
        pg_task_scheduler::TaskKey::try_from("k").unwrap(),
        serde_json::json!({}),
        pg_task_scheduler::EnqueueOptions::immediate(),
    )
    .await
    .unwrap()
    .id
}

async fn send(
    app: Router,
    method: Method,
    uri: String,
    body: Option<serde_json::Value>,
) -> (StatusCode, Vec<u8>) {
    let mut req = Request::builder().method(method).uri(uri);
    let body = match body {
        Some(v) => {
            req = req.header("content-type", "application/json");
            Body::from(v.to_string())
        }
        None => Body::empty(),
    };
    let resp = app.oneshot(req.body(body).unwrap()).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, bytes.to_vec())
}

#[tokio::test]
async fn task_route_returns_view_and_404() {
    let db = TestDb::new().await;
    let id = enqueue_one(&db).await;
    let app = pg_task_scheduler::admin::router(db.pool.clone());
    let (status, body) = send(app.clone(), Method::GET, format!("/tasks/{}", id.0), None).await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["id"], id.0);
    assert_eq!(v["task_name"], "admin-task");
    assert_eq!(v["queue"], "default");
    assert_eq!(v["status"], "queued");
    assert_eq!(v["attempts"], 0);
    assert!(v["locked_by"].is_null() && v["last_error"].is_null() && v["completed_at"].is_null());
    assert!(v["task_key"].is_string() && v["due_at"].is_string() && v["max_attempts"].is_i64());
    let (status, _) = send(app.clone(), Method::GET, "/tasks/999999".into(), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send(app, Method::POST, format!("/tasks/{}/cancel", id.0), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    db.cleanup().await;
}

#[tokio::test]
async fn replay_route_requeues_dead_task_and_409s_otherwise() {
    let db = TestDb::new().await;
    let id = enqueue_one(&db).await;
    let app = pg_task_scheduler::admin::router(db.pool.clone());
    let uri = format!("/tasks/{}/replay", id.0);
    let req = serde_json::json!({"operator": "ops@example", "reason": "fixed config"});
    // Queued, not dead.
    let (status, body) = send(app.clone(), Method::POST, uri.clone(), Some(req.clone())).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body, b"task is not dead or does not exist");
    // Missing.
    let (status, _) = send(
        app.clone(),
        Method::POST,
        "/tasks/999999/replay".into(),
        Some(req.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    // Dead.
    let mut conn = db.pool.get().await.unwrap();
    diesel_async::RunQueryDsl::execute(
        diesel::sql_query(format!(
            "UPDATE queue_tasks SET status = 'dead', attempts = max_attempts, \
             last_error = 'boom', completed_at = now() WHERE id = {}",
            id.0
        )),
        &mut conn,
    )
    .await
    .unwrap();
    let (status, _) = send(app.clone(), Method::POST, uri.clone(), Some(req)).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let t = pg_task_scheduler::store::task(&mut conn, id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(t.status, pg_task_scheduler::TaskStatus::Queued);
    assert_eq!(t.attempts, 0);
    let n = db
        .scalar(&format!(
            "SELECT count(*) AS n FROM queue_task_replays WHERE task_id = {} \
             AND operator = 'ops@example' AND reason = 'fixed config'",
            id.0
        ))
        .await;
    assert_eq!(n, 1);
    // Now queued again: second replay conflicts.
    let again = serde_json::json!({"operator": "x", "reason": "y"});
    let (status, _) = send(app, Method::POST, uri, Some(again)).await;
    assert_eq!(status, StatusCode::CONFLICT);
    db.cleanup().await;
}
