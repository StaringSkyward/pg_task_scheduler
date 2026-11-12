#[path = "../../tests/common/mod.rs"]
mod common;
use axum::Extension;
use axum::Router;
use axum::body::Body;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::header;
use common::TestDb;
use diesel_async::SimpleAsyncConnection;
use pg_task_scheduler::store;
use pg_task_scheduler_admin::AdminConfig;
use pg_task_scheduler_admin::Operator;
use pg_task_scheduler_admin::router;
use tower::ServiceExt;

async fn sql(db: &TestDb, stmt: &str) {
    let mut conn = db.pool.get().await.unwrap();
    conn.batch_execute(stmt).await.unwrap();
}

fn app(db: &TestDb) -> Router {
    router(db.pool.clone(), AdminConfig::default()).layer(Extension(Operator("ops".into())))
}

struct Res {
    status: StatusCode,
    location: String,
    body: String,
}

async fn send(app: Router, req: Request<Body>) -> Res {
    let res = app.oneshot(req).await.unwrap();
    let status = res.status();
    let location = res
        .headers()
        .get(header::LOCATION)
        .map(|v| v.to_str().unwrap().to_owned())
        .unwrap_or_default();
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    Res {
        status,
        location,
        body: String::from_utf8(body.to_vec()).unwrap(),
    }
}

async fn get(app: Router, uri: &str) -> Res {
    send(
        app,
        Request::builder().uri(uri).body(Body::empty()).unwrap(),
    )
    .await
}

async fn post(app: Router, uri: &str, fields: &[(&str, &str)]) -> Res {
    let req = Request::builder()
        .method("POST")
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .header(header::HOST, "admin.test")
        .header(header::ORIGIN, "http://admin.test")
        .body(Body::from(serde_urlencoded::to_string(fields).unwrap()))
        .unwrap();
    send(app, req).await
}

async fn queued(db: &TestDb, name: &str, queue: &str, key: &str) -> i64 {
    let mut conn = db.pool.get().await.unwrap();
    let id = diesel_async::RunQueryDsl::get_result::<Id>(
        diesel::sql_query(format!(
            "INSERT INTO queue_tasks (task_key, task_name, queue) \
             VALUES ('{key}', '{name}', '{}') RETURNING id AS n",
            queue.replace('\'', "''")
        )),
        &mut conn,
    )
    .await
    .unwrap();
    id.n
}

#[derive(diesel::QueryableByName)]
struct Id {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

async fn status_of(db: &TestDb, id: i64) -> String {
    let mut conn = db.pool.get().await.unwrap();
    store::task(&mut conn, pg_task_scheduler::TaskId(id))
        .await
        .unwrap()
        .unwrap()
        .status
        .as_str()
        .to_owned()
}

#[tokio::test]
async fn cancel_and_replay_via_forms() {
    let db = TestDb::new().await;
    let id = queued(&db, "a", "default", "k1").await;
    let page = get(app(&db), &format!("/tasks/{id}")).await;
    assert!(
        page.body
            .contains(&format!("action=\"/tasks/{id}/cancel\""))
    );
    assert!(!page.body.contains("/replay\""));

    let r = post(
        app(&db),
        &format!("/tasks/{id}/cancel"),
        &[("reason", "oops")],
    )
    .await;
    assert_eq!(
        (r.status, r.location.as_str()),
        (StatusCode::SEE_OTHER, format!("/tasks/{id}").as_str())
    );
    assert_eq!(status_of(&db, id).await, "cancelled");
    let page = get(app(&db), &format!("/tasks/{id}")).await;
    assert!(
        page.body
            .contains(&format!("action=\"/tasks/{id}/replay\""))
    );

    // Cancelling again is a conflict.
    let r = post(
        app(&db),
        &format!("/tasks/{id}/cancel"),
        &[("reason", "again")],
    )
    .await;
    assert_eq!(r.status, StatusCode::CONFLICT);

    let r = post(
        app(&db),
        &format!("/tasks/{id}/replay"),
        &[("reason", "retry")],
    )
    .await;
    assert_eq!(r.status, StatusCode::SEE_OTHER);
    assert_eq!(status_of(&db, id).await, "queued");
    // Queued tasks cannot be replayed.
    let r = post(app(&db), &format!("/tasks/{id}/replay"), &[("reason", "x")]).await;
    assert_eq!(r.status, StatusCode::CONFLICT);

    let n = db
        .scalar(&format!(
            "SELECT count(*) AS n FROM queue_admin_actions WHERE task_id = {id} \
             AND operator = 'ops' AND action IN ('cancel', 'replay')"
        ))
        .await;
    assert_eq!(n, 2);
    db.cleanup().await;
}

#[tokio::test]
async fn cancel_requires_reason() {
    let db = TestDb::new().await;
    let id = queued(&db, "a", "default", "k1").await;
    for reason in ["", "   "] {
        let r = post(
            app(&db),
            &format!("/tasks/{id}/cancel"),
            &[("reason", reason)],
        )
        .await;
        assert_eq!(r.status, StatusCode::BAD_REQUEST);
    }
    assert_eq!(status_of(&db, id).await, "queued");
    db.cleanup().await;
}

/// A failed action's page never auto-reloads: reloading a POST result asks to resubmit it,
/// and resubmitting could run the action again.
#[tokio::test]
async fn failed_action_page_does_not_auto_refresh() {
    let db = TestDb::new().await;
    let id = queued(&db, "a", "default", "k1").await;
    let blank = post(app(&db), &format!("/tasks/{id}/cancel"), &[("reason", "")]).await;
    let missing = post(app(&db), "/tasks/999999/cancel", &[("reason", "r")]).await;
    for (r, want) in [
        (blank, StatusCode::BAD_REQUEST),
        (missing, StatusCode::CONFLICT),
    ] {
        assert_eq!(r.status, want);
        assert!(r.body.contains(r#"data-refresh="0""#), "{}", r.body);
    }
    db.cleanup().await;
}

#[tokio::test]
async fn cross_site_form_post_is_rejected() {
    let db = TestDb::new().await;
    let id = queued(&db, "a", "default", "k1").await;
    let req = Request::builder()
        .method("POST")
        .uri(format!("/tasks/{id}/cancel"))
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .header(header::HOST, "admin.test")
        .header(header::ORIGIN, "http://evil.test")
        .body(Body::from("reason=x"))
        .unwrap();
    assert_eq!(send(app(&db), req).await.status, StatusCode::FORBIDDEN);
    assert_eq!(status_of(&db, id).await, "queued");
    db.cleanup().await;
}

#[tokio::test]
async fn pause_and_resume_queue_via_forms() {
    let db = TestDb::new().await;
    let r = post(
        app(&db),
        "/queues/pause",
        &[("queue", "email"), ("reason", "deploy")],
    )
    .await;
    assert_eq!(
        (r.status, r.location.as_str()),
        (StatusCode::SEE_OTHER, "/overview")
    );
    let mut conn = db.pool.get().await.unwrap();
    let p = store::paused_queues(&mut conn).await.unwrap();
    assert_eq!(p.len(), 1);
    assert_eq!(
        (p[0].queue.as_str(), p[0].operator.as_str()),
        ("email", "ops")
    );
    let r = post(
        app(&db),
        "/queues/resume",
        &[("queue", "email"), ("reason", "done")],
    )
    .await;
    assert_eq!(r.status, StatusCode::SEE_OTHER);
    assert!(store::paused_queues(&mut conn).await.unwrap().is_empty());
    let r = post(
        app(&db),
        "/queues/pause",
        &[("queue", "email"), ("reason", "")],
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    db.cleanup().await;
}

#[tokio::test]
async fn bulk_requires_typed_name() {
    let db = TestDb::new().await;
    let id = queued(&db, "a", "default", "k1").await;
    let r = post(
        app(&db),
        "/bulk",
        &[
            ("action", "cancel"),
            ("task_name", "a"),
            ("confirm_name", "b"),
            ("reason", "r"),
        ],
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(
        r.body.contains("Type the task name to confirm."),
        "{}",
        r.body
    );
    assert_eq!(status_of(&db, id).await, "queued");
    db.cleanup().await;
}

#[tokio::test]
async fn bulk_cancel_runs_and_offers_continue() {
    let db = TestDb::new().await;
    for i in 0..3 {
        queued(&db, "a", "default", &format!("k{i}")).await;
    }
    queued(&db, "other", "default", "o").await;
    let c = get(app(&db), "/bulk?action=cancel&task_name=a").await;
    assert_eq!(c.status, StatusCode::OK);
    assert!(
        c.body.contains("<strong class=\"match-count\">3</strong>"),
        "{}",
        c.body
    );
    assert!(c.body.contains("name=\"confirm_name\""));
    assert!(c.body.contains("data-busy"));

    let r = post(
        app(&db),
        "/bulk",
        &[
            ("action", "cancel"),
            ("task_name", "a"),
            ("queue", ""),
            ("error", ""),
            ("confirm_name", "a"),
            ("reason", "cleanup"),
        ],
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    assert!(
        r.body.contains("<p id=\"bulk-result\">3 cancelled</p>"),
        "{}",
        r.body
    );
    assert!(!r.body.contains("class=\"continue\""));
    let n = db
        .scalar(
            "SELECT count(*) AS n FROM queue_tasks WHERE status = 'cancelled' AND task_name = 'a'",
        )
        .await;
    assert_eq!(n, 3);
    let n = db
        .scalar("SELECT count(*) AS n FROM queue_admin_actions WHERE operator = 'ops' AND reason = 'cleanup' AND affected = 3")
        .await;
    assert_eq!(n, 1);

    // Replay-all from the failure-group link's confirm page.
    sql(&db, "UPDATE queue_tasks SET status = 'dead', last_error = 'boom <b>', completed_at = now(), attempts = max_attempts WHERE task_name = 'other'").await;
    let c = get(
        app(&db),
        "/bulk?action=replay&task_name=other&error=boom+%3Cb%3E",
    )
    .await;
    assert!(
        c.body.contains("<strong class=\"match-count\">1</strong>"),
        "{}",
        c.body
    );
    assert!(
        c.body.contains("value=\"boom &lt;b&gt;\"")
            || c.body.contains("value=\"boom &#60;b&#62;\""),
        "{}",
        c.body
    );
    db.cleanup().await;
}

#[tokio::test]
async fn forms_work_under_base_path() {
    let db = TestDb::new().await;
    let id = queued(&db, "a", "default", "k1").await;
    let config = AdminConfig {
        base_path: "/admin".into(),
        ..Default::default()
    };
    let app = Router::new()
        .nest("/admin", router(db.pool.clone(), config))
        .layer(Extension(Operator("ops".into())));
    let page = get(app.clone(), &format!("/admin/tasks/{id}")).await;
    let action = page
        .body
        .split("action=\"")
        .nth(1)
        .unwrap()
        .split('"')
        .next()
        .unwrap();
    assert_eq!(action, format!("/admin/tasks/{id}/cancel"));
    let r = post(app.clone(), action, &[("reason", "x")]).await;
    assert_eq!(r.status, StatusCode::SEE_OTHER);
    assert!(r.location.starts_with("/admin/tasks/"), "{}", r.location);
    let r = post(
        app,
        "/admin/queues/pause",
        &[("queue", "q"), ("reason", "x")],
    )
    .await;
    assert_eq!(r.location, "/admin/overview");
    db.cleanup().await;
}

#[tokio::test]
async fn pause_form_with_url_characters() {
    let db = TestDb::new().await;
    let name = "eu/bulk & co #1";
    queued(&db, "a", name, "k1").await;
    let page = get(app(&db), "/").await;
    // Find the pause form for this queue and replay its hidden field as a browser would.
    let form = page
        .body
        .split("<form")
        .find(|f| f.contains("/queues/pause"))
        .expect("pause form");
    assert!(form.contains("value=\"eu/bulk &amp; co #1\""), "{form}");
    let r = post(
        app(&db),
        "/queues/pause",
        &[("queue", name), ("reason", "x")],
    )
    .await;
    assert_eq!(r.status, StatusCode::SEE_OTHER);
    let mut conn = db.pool.get().await.unwrap();
    let p = store::paused_queues(&mut conn).await.unwrap();
    assert_eq!(p.len(), 1);
    assert_eq!(p[0].queue.as_str(), name);
    db.cleanup().await;
}

#[tokio::test]
async fn overview_offers_resume_and_replay_all() {
    let db = TestDb::new().await;
    queued(&db, "a", "email", "k1").await;
    sql(&db, "INSERT INTO queue_tasks (task_key, task_name, queue, status, last_error, completed_at, attempts, max_attempts) \
        VALUES ('d', 'send', 'email', 'dead', 'boom', now(), 3, 3)").await;
    post(
        app(&db),
        "/queues/pause",
        &[("queue", "email"), ("reason", "x")],
    )
    .await;
    let page = get(app(&db), "/").await;
    assert!(
        page.body.contains("action=\"/queues/resume\""),
        "{}",
        page.body
    );
    assert!(
        page.body
            .contains("href=\"/bulk?action=replay&amp;task_name=send&amp;error=boom\"")
            || page
                .body
                .contains("href=\"/bulk?action=replay&#38;task_name=send&#38;error=boom\""),
        "{}",
        page.body
    );
    db.cleanup().await;
}
