#[path = "../../tests/common/mod.rs"]
mod common;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use axum::{Extension, Router};
use common::TestDb;
use diesel_async::SimpleAsyncConnection;
use pg_task_scheduler_admin::{AdminConfig, Operator, router};
use tower::ServiceExt;

async fn sql(db: &TestDb, stmt: &str) {
    let mut conn = db.pool.get().await.unwrap();
    conn.batch_execute(stmt).await.unwrap();
}

fn app(db: &TestDb) -> Router {
    router(db.pool.clone(), AdminConfig::default()).layer(Extension(Operator("ops@example".into())))
}

async fn get(app: Router, uri: &str) -> (StatusCode, String, String) {
    let res = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let ctype = res
        .headers()
        .get(header::CONTENT_TYPE)
        .map(|v| v.to_str().unwrap().to_owned())
        .unwrap_or_default();
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, ctype, String::from_utf8(body.to_vec()).unwrap())
}

const DUE_EMAIL: &str = "INSERT INTO queue_tasks (task_key, task_name, queue, due_at) \
    VALUES ('e1', 'send', 'email', now() - interval '1 minute')";

/// A worker row serving `queue` (limit 4), seen now and finished a task 5s ago.
fn worker_sql(queue: &str, lost_after: &str) -> String {
    format!(
        "INSERT INTO queue_workers (worker_id, label, lost_after, status, queues, task_names) \
         VALUES ('w1', 'box', {lost_after}, 'healthy', \
           jsonb_build_array(jsonb_build_object('name', '{queue}', 'limit', 4, 'in_use', 0, \
             'paused', false, 'last_finished_at', now() - interval '5 seconds')), '{{send}}')"
    )
}

#[tokio::test]
async fn overview_empty_install_renders() {
    let db = TestDb::new().await;
    let (status, _, body) = get(app(&db), "/").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("No workers registered"), "{body}");
    db.cleanup().await;
}

#[tokio::test]
async fn overview_alerts_unserved_queue() {
    let db = TestDb::new().await;
    sql(&db, DUE_EMAIL).await;
    let (status, _, body) = get(app(&db), "/").await;
    assert_eq!(status, StatusCode::OK);
    let alert = body.split("data-alert-key").nth(1).expect("alert present");
    assert!(alert.contains("email"), "{body}");
    assert!(body.contains("stalled"), "{body}");
    db.cleanup().await;
}

#[tokio::test]
async fn overview_shows_registered_worker_and_healthy_queue() {
    let db = TestDb::new().await;
    sql(&db, DUE_EMAIL).await;
    sql(&db, &worker_sql("email", "now() + interval '1 minute'")).await;
    let (_, _, body) = get(app(&db), "/").await;
    assert!(body.contains("box"), "{body}");
    assert!(body.contains("healthy"), "{body}");
    assert!(!body.contains("data-alert-key"), "{body}");
    db.cleanup().await;
}

#[tokio::test]
async fn lost_worker_does_not_serve_queue() {
    let db = TestDb::new().await;
    sql(&db, DUE_EMAIL).await;
    sql(&db, &worker_sql("email", "now() - interval '1 second'")).await;
    let (_, _, body) = get(app(&db), "/").await;
    let alert = body.split("data-alert-key").nth(1).expect("alert present");
    assert!(alert.contains("email"), "{body}");
    assert!(body.contains("stalled"), "{body}");
    assert!(body.contains("no live worker"), "{body}");
    db.cleanup().await;
}

#[tokio::test]
async fn overview_links_respect_base_path() {
    let db = TestDb::new().await;
    sql(&db, DUE_EMAIL).await;
    let config = AdminConfig {
        base_path: "/admin".into(),
        ..Default::default()
    };
    let app = Router::new()
        .nest("/admin", router(db.pool.clone(), config))
        .layer(Extension(Operator("ops@example".into())));
    let (status, _, body) = get(app.clone(), "/admin").await;
    assert_eq!(status, StatusCode::OK);
    let mut seen = 0;
    for attr in ["href=\"", "src=\"", "action=\""] {
        for part in body.split(attr).skip(1) {
            let url = part.split('"').next().unwrap();
            assert!(url.starts_with("/admin/"), "{attr}{url}");
            seen += 1;
        }
    }
    assert!(seen >= 3, "{body}");
    let (status, ctype, _) = get(app.clone(), "/admin/assets/admin.css").await;
    assert_eq!(
        (status, ctype.as_str()),
        (StatusCode::OK, "text/css; charset=utf-8")
    );
    let (status, ctype, _) = get(app, "/admin/assets/admin.js").await;
    assert_eq!(
        (status, ctype.as_str()),
        (StatusCode::OK, "text/javascript; charset=utf-8")
    );
    db.cleanup().await;
}

#[tokio::test]
async fn overview_reports_database_error() {
    let db = TestDb::new().await;
    sql(&db, "DROP TABLE queue_tasks CASCADE").await;
    let (status, _, body) = get(app(&db), "/").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("Overview unavailable"), "{body}");
    assert!(!body.contains("healthy"), "{body}");
    db.cleanup().await;
}

#[tokio::test]
async fn overview_requires_operator() {
    let db = TestDb::new().await;
    let (status, _, _) = get(router(db.pool.clone(), AdminConfig::default()), "/").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    db.cleanup().await;
}
