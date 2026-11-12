#[path = "../../tests/common/mod.rs"]
mod common;
use axum::Extension;
use axum::Router;
use axum::body::Body;
use axum::http::Method;
use axum::http::Request;
use axum::http::StatusCode;
use common::TestDb;
use pg_task_scheduler_admin::AdminConfig;
use pg_task_scheduler_admin::Operator;
use pg_task_scheduler_admin::router;
use tower::ServiceExt; // for `oneshot`

struct AdminTask;

impl pg_task_scheduler::Task for AdminTask {
    const NAME: &'static str = "admin-task";
    type Args = serde_json::Value;
}

/// The admin router as an authenticated `ops@example` sees it.
fn app(db: &TestDb) -> Router {
    router(db.pool.clone(), AdminConfig::default()).layer(Extension(Operator("ops@example".into())))
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

#[cfg(feature = "cron")]
#[tokio::test]
async fn list_jobs_returns_view() {
    let db = TestDb::new().await;
    let id = db
        .insert_job(
            "j",
            "*/5 * * * *",
            chrono::Utc::now() + chrono::Duration::hours(1),
        )
        .await;
    let (status, body) = send(app(&db), Method::GET, "/api/jobs".into(), None).await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        v,
        serde_json::json!([{
            "id": id.to_string(),
            "name": "j",
            "cron_expression": "*/5 * * * *",
            "is_paused": false,
        }])
    );
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
    let app = app(&db);
    assert_eq!(
        status_of(app.clone(), Method::POST, format!("/api/jobs/{id}/pause")).await,
        StatusCode::NO_CONTENT
    );
    assert!(db.is_paused(id).await);
    assert_eq!(
        status_of(
            app,
            Method::POST,
            format!("/api/jobs/{}/pause", uuid::Uuid::new_v4())
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
        .insert_job_full(
            "j",
            "*/5 * * * *",
            chrono::Utc::now() + chrono::Duration::hours(1),
            3,
            true,
        )
        .await;
    let app = app(&db);
    assert_eq!(
        status_of(app.clone(), Method::POST, format!("/api/jobs/{id}/resume")).await,
        StatusCode::NO_CONTENT
    );
    assert!(!db.is_paused(id).await);
    assert_eq!(
        status_of(
            app,
            Method::POST,
            format!("/api/jobs/{}/resume", uuid::Uuid::new_v4())
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
    let app = app(&db);
    // First delete removes the row → 204.
    assert_eq!(
        status_of(app.clone(), Method::DELETE, format!("/api/jobs/{id}")).await,
        StatusCode::NO_CONTENT
    );
    // Deleting again → 404: proves the first call actually deleted (not a no-op/pause).
    assert_eq!(
        status_of(app, Method::DELETE, format!("/api/jobs/{id}")).await,
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
    let app = app(&db);
    let (status, body) = send(
        app.clone(),
        Method::GET,
        format!("/api/tasks/{}", id.0),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["id"], id.0);
    assert_eq!(v["task_name"], "admin-task");
    assert_eq!(v["queue"], "default");
    assert_eq!(v["status"], "queued");
    assert_eq!(v["attempts"], 0);
    assert!(v["locked_by"].is_null() && v["last_error"].is_null() && v["completed_at"].is_null());
    assert!(v["task_key"].is_string() && v["due_at"].is_string() && v["max_attempts"].is_i64());
    let (status, _) = send(app.clone(), Method::GET, "/api/tasks/999999".into(), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send(
        app,
        Method::POST,
        format!("/api/tasks/{}/cancel", id.0),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    db.cleanup().await;
}

#[tokio::test]
async fn replay_route_requeues_dead_task_and_409s_otherwise() {
    let db = TestDb::new().await;
    let id = enqueue_one(&db).await;
    let app = app(&db);
    let uri = format!("/api/tasks/{}/replay", id.0);
    // The body's `operator` is ignored: the audit records the authenticated identity.
    let req = serde_json::json!({"operator": "mallory", "reason": "fixed config"});
    // Queued, not dead.
    let (status, body) = send(app.clone(), Method::POST, uri.clone(), Some(req.clone())).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body, b"task is not dead or cancelled, or does not exist");
    // Missing.
    let (status, _) = send(
        app.clone(),
        Method::POST,
        "/api/tasks/999999/replay".into(),
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
            "SELECT count(*) AS n FROM queue_admin_actions WHERE action = 'replay' AND task_id = {} \
             AND operator = 'ops@example' AND reason = 'fixed config'",
            id.0
        ))
        .await;
    assert_eq!(n, 1);
    // Now queued again: second replay conflicts.
    let again = serde_json::json!({"reason": "y"});
    let (status, _) = send(app, Method::POST, uri, Some(again)).await;
    assert_eq!(status, StatusCode::CONFLICT);
    db.cleanup().await;
}

/// Like the forms, the JSON replay requires a reason: blank is 400 and changes nothing; the
/// audit records it trimmed.
#[tokio::test]
async fn replay_route_requires_reason() {
    let db = TestDb::new().await;
    let id = enqueue_one(&db).await;
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
    let uri = format!("/api/tasks/{}/replay", id.0);
    for reason in ["", "   "] {
        let req = serde_json::json!({ "reason": reason });
        let (status, body) = send(app(&db), Method::POST, uri.clone(), Some(req)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{reason:?}");
        assert_eq!(body, b"a reason is required");
    }
    let t = pg_task_scheduler::store::task(&mut conn, id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(t.status, pg_task_scheduler::TaskStatus::Dead);
    let req = serde_json::json!({ "reason": "  fixed  " });
    let (status, _) = send(app(&db), Method::POST, uri, Some(req)).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let reasons = db
        .scalar(
            "SELECT count(*) AS n FROM queue_admin_actions WHERE action = 'replay' \
             AND reason = 'fixed'",
        )
        .await;
    assert_eq!(reasons, 1);
    db.cleanup().await;
}

#[derive(diesel::QueryableByName)]
struct Setting {
    #[diesel(sql_type = diesel::sql_types::Text)]
    v: String,
}

const SHOW_TIMEOUT: &str = "SELECT current_setting('statement_timeout') AS v";
const SHOW_IDLE: &str = "SELECT current_setting('idle_in_transaction_session_timeout') AS v";

#[tokio::test]
async fn read_sets_statement_timeout_for_its_transaction_only() {
    use std::time::Duration;
    let db = TestDb::new().await;
    // Inside: the timeout is in force, and so is the same idle-in-transaction timeout, which
    // ends a transaction left open by a read whose future was dropped (client gone).
    let (inside, idle, pid) =
        pg_task_scheduler_admin::db::read(&db.pool, Duration::from_millis(1500), |c| {
            Box::pin(async move {
                let s = diesel_async::RunQueryDsl::get_result::<Setting>(
                    diesel::sql_query(SHOW_TIMEOUT),
                    &mut *c,
                )
                .await?;
                let i = diesel_async::RunQueryDsl::get_result::<Setting>(
                    diesel::sql_query(SHOW_IDLE),
                    &mut *c,
                )
                .await?;
                Ok((s.v, i.v, common::backend_pid(c).await))
            })
        })
        .await
        .unwrap();
    assert_eq!((inside.as_str(), idle.as_str()), ("1500ms", "1500ms"));
    // Back on the same pooled connection, outside the transaction, it did not leak.
    {
        let mut conn = db.pool.get().await.unwrap();
        assert_eq!(
            common::backend_pid(&mut conn).await,
            pid,
            "not the same connection"
        );
        let after = diesel_async::RunQueryDsl::get_result::<Setting>(
            diesel::sql_query(SHOW_TIMEOUT),
            &mut conn,
        )
        .await
        .unwrap()
        .v;
        assert_eq!(after, "0");
        let idle_after = diesel_async::RunQueryDsl::get_result::<Setting>(
            diesel::sql_query(SHOW_IDLE),
            &mut conn,
        )
        .await
        .unwrap()
        .v;
        assert_eq!(idle_after, "0");
    }
    // A statement over the timeout is cancelled.
    let slow = pg_task_scheduler_admin::db::read(&db.pool, Duration::from_millis(50), |c| {
        Box::pin(async move {
            diesel_async::RunQueryDsl::execute(diesel::sql_query("SELECT pg_sleep(2)"), c)
                .await
                .map_err(Into::into)
        })
    })
    .await;
    let err = slow.unwrap_err().to_string();
    assert!(err.contains("statement timeout"), "{err}");
    db.cleanup().await;
}
