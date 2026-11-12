#[path = "../../tests/common/mod.rs"]
mod common;
use axum::body::Body;
use axum::http::{HeaderName, Method, Request, StatusCode, header};
use axum::response::Response;
use axum::{Extension, Router};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use common::TestDb;
use pg_task_scheduler_admin::auth::{
    BasicUser, hash_password, with_basic_auth, with_trusted_header,
};
use pg_task_scheduler_admin::{AdminConfig, Operator, router};
use tower::ServiceExt; // for `oneshot`

struct AdminTask;

impl pg_task_scheduler::Task for AdminTask {
    const NAME: &'static str = "admin-task";
    type Args = serde_json::Value;
}

async fn enqueue_one(db: &TestDb) -> i64 {
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
    .0
}

async fn kill(db: &TestDb, id: i64) {
    let mut conn = db.pool.get().await.unwrap();
    diesel_async::RunQueryDsl::execute(
        diesel::sql_query(format!(
            "UPDATE queue_tasks SET status = 'dead', attempts = max_attempts, \
             last_error = 'boom', completed_at = now() WHERE id = {id}"
        )),
        &mut conn,
    )
    .await
    .unwrap();
}

/// `method uri` with the given headers; POSTs carry a JSON replay body.
async fn send(app: &Router, method: Method, uri: &str, headers: &[(&str, &str)]) -> Response {
    let mut req = Request::builder().method(method.clone()).uri(uri);
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    let body = if method == Method::POST {
        req = req.header(header::CONTENT_TYPE, "application/json");
        Body::from(r#"{"reason":"test"}"#)
    } else {
        Body::empty()
    };
    app.clone().oneshot(req.body(body).unwrap()).await.unwrap()
}

#[tokio::test]
async fn every_route_401_without_operator() {
    let db = TestDb::new().await;
    let id = enqueue_one(&db).await;
    let app = router(db.pool.clone(), AdminConfig::default());
    let get = |uri: &str| (Method::GET, uri.to_owned());
    let post = |uri: &str| (Method::POST, uri.to_owned());
    // Every route, so one added outside the operator check fails here.
    #[cfg_attr(not(feature = "cron"), allow(unused_mut))]
    let mut routes = vec![
        get("/"),
        get("/overview"),
        get("/workers"),
        get("/queued?queue=x"),
        get("/running"),
        get("/completed"),
        get("/failed"),
        get("/cancelled"),
        get(&format!("/tasks/{id}")),
        get("/assets/admin.css"),
        get("/assets/admin.js"),
        get("/bulk?action=cancel&task_name=admin-task"),
        post(&format!("/tasks/{id}/cancel")),
        post(&format!("/tasks/{id}/replay")),
        post("/queues/pause"),
        post("/queues/resume"),
        post("/bulk"),
        get(&format!("/api/tasks/{id}")),
        post(&format!("/api/tasks/{id}/replay")),
    ];
    #[cfg(feature = "cron")]
    {
        let job = db.insert_job("j", "*/5 * * * *", chrono::Utc::now()).await;
        routes.extend([
            (Method::GET, "/api/jobs".to_owned()),
            (Method::POST, format!("/api/jobs/{job}/pause")),
            (Method::POST, format!("/api/jobs/{job}/resume")),
            (Method::DELETE, format!("/api/jobs/{job}")),
        ]);
    }
    for (method, uri) in routes {
        let resp = send(&app, method.clone(), &uri, &[]).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "{method} {uri}");
    }
    // The operator check runs before the CSRF check.
    let resp = send(
        &app,
        Method::POST,
        &format!("/api/tasks/{id}/replay"),
        &[("sec-fetch-site", "cross-site")],
    )
    .await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    db.cleanup().await;
}

/// Every response forbids framing (clickjacking): pages, and the 401s of the operator check
/// and of the shipped auth layers.
#[tokio::test]
async fn responses_forbid_framing() {
    let db = TestDb::new().await;
    let bare = || router(db.pool.clone(), AdminConfig::default());
    let page = send(
        &bare().layer(Extension(Operator("ops".into()))),
        Method::GET,
        "/",
        &[],
    )
    .await;
    assert_eq!(page.status(), StatusCode::OK);
    let no_operator = send(&bare(), Method::GET, "/", &[]).await;
    let basic = send(
        &with_basic_auth(bare(), Vec::new()).unwrap(),
        Method::GET,
        "/",
        &[],
    )
    .await;
    let trusted = send(
        &with_trusted_header(bare(), HeaderName::from_static("x-forwarded-user")),
        Method::GET,
        "/",
        &[],
    )
    .await;
    for resp in [page, no_operator, basic, trusted] {
        let status = resp.status();
        assert_eq!(resp.headers()["x-frame-options"], "DENY", "{status}");
        assert_eq!(
            resp.headers()["content-security-policy"],
            "frame-ancestors 'none'",
            "{status}"
        );
    }
    db.cleanup().await;
}

const CHALLENGE: &str = r#"Basic realm="pg_task_scheduler admin", charset="UTF-8""#;

fn basic(user: &str, password: &str) -> String {
    format!("Basic {}", BASE64.encode(format!("{user}:{password}")))
}

#[tokio::test]
async fn basic_auth_accepts_valid_rejects_invalid() {
    let db = TestDb::new().await;
    let id = enqueue_one(&db).await;
    let users = vec![BasicUser {
        name: "alice".into(),
        password_hash: hash_password("s3cret"),
    }];
    let app = with_basic_auth(router(db.pool.clone(), AdminConfig::default()), users).unwrap();
    let uri = format!("/api/tasks/{id}");

    let ok = send(
        &app,
        Method::GET,
        &uri,
        &[("authorization", &basic("alice", "s3cret"))],
    )
    .await;
    assert_eq!(ok.status(), StatusCode::OK);

    for bad in [
        vec![("authorization", basic("alice", "wrong"))],
        vec![("authorization", basic("mallory", "s3cret"))],
        vec![("authorization", "Basic not-base64!".to_owned())],
        vec![("authorization", "Bearer abc".to_owned())],
        vec![],
    ] {
        let headers: Vec<(&str, &str)> = bad.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let resp = send(&app, Method::GET, &uri, &headers).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "{bad:?}");
        assert_eq!(
            resp.headers()[header::WWW_AUTHENTICATE],
            CHALLENGE,
            "{bad:?}"
        );
    }

    // Config errors: a hash that does not parse, or is not argon2id.
    let argon2i = hash_password("s3cret").replacen("$argon2id$", "$argon2i$", 1);
    for hash in ["not a hash".to_owned(), argon2i] {
        let users = vec![BasicUser {
            name: "bob".into(),
            password_hash: hash.clone(),
        }];
        let err = with_basic_auth(router(db.pool.clone(), AdminConfig::default()), users)
            .unwrap_err()
            .to_string();
        assert!(err.contains("bob") && !err.contains(&hash), "{err}");
    }
    db.cleanup().await;
}

#[tokio::test]
async fn trusted_header_sets_operator() {
    let db = TestDb::new().await;
    let id = enqueue_one(&db).await;
    kill(&db, id).await;
    let app = with_trusted_header(
        router(db.pool.clone(), AdminConfig::default()),
        HeaderName::from_static("x-forwarded-user"),
    );
    let uri = format!("/api/tasks/{id}/replay");

    for headers in [
        vec![],
        vec![("x-forwarded-user", "")],
        // Two values: a proxy appended rather than overwrote the client's.
        vec![
            ("x-forwarded-user", "mallory"),
            ("x-forwarded-user", "carol"),
        ],
    ] {
        let resp = send(&app, Method::POST, &uri, &headers).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "{headers:?}");
    }

    let resp = send(&app, Method::POST, &uri, &[("x-forwarded-user", "carol")]).await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let n = db
        .scalar(&format!(
            "SELECT count(*) AS n FROM queue_admin_actions \
             WHERE action = 'replay' AND task_id = {id} AND operator = 'carol'"
        ))
        .await;
    assert_eq!(n, 1);
    db.cleanup().await;
}

#[tokio::test]
async fn csrf_rejects_cross_site_post() {
    let db = TestDb::new().await;
    let authed = |config: AdminConfig| {
        router(db.pool.clone(), config).layer(Extension(Operator("ops@example".into())))
    };
    let app = authed(AdminConfig::default());
    // A missing task: a request past the CSRF guard gets 409, a rejected one 403.
    let uri = "/api/tasks/999999/replay";
    let cases: [(&[(&str, &str)], StatusCode); 7] = [
        (&[("sec-fetch-site", "cross-site")], StatusCode::FORBIDDEN),
        (&[("sec-fetch-site", "same-site")], StatusCode::FORBIDDEN),
        (
            &[("origin", "https://evil.example"), ("host", "admin.local")],
            StatusCode::FORBIDDEN,
        ),
        (
            &[("origin", "null"), ("host", "admin.local")],
            StatusCode::FORBIDDEN,
        ),
        (
            &[("origin", "http://admin.local"), ("host", "admin.local")],
            StatusCode::CONFLICT,
        ),
        (&[("sec-fetch-site", "same-origin")], StatusCode::CONFLICT),
        (&[], StatusCode::CONFLICT),
    ];
    for (headers, want) in cases {
        let resp = send(&app, Method::POST, uri, headers).await;
        assert_eq!(resp.status(), want, "{headers:?}");
    }
    // Sec-Fetch-Site wins over a matching Origin.
    let resp = send(
        &app,
        Method::POST,
        uri,
        &[
            ("sec-fetch-site", "cross-site"),
            ("origin", "http://admin.local"),
            ("host", "admin.local"),
        ],
    )
    .await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    // GETs are not checked.
    let resp = send(
        &app,
        Method::GET,
        "/api/tasks/999999",
        &[("sec-fetch-site", "cross-site")],
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // Behind a proxy that rewrites Host, public_origin is what matches.
    let behind_proxy = authed(AdminConfig {
        public_origin: Some("https://admin.example.com".into()),
        ..AdminConfig::default()
    });
    let resp = send(
        &behind_proxy,
        Method::POST,
        uri,
        &[
            ("origin", "https://admin.example.com"),
            ("host", "internal:8080"),
        ],
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    db.cleanup().await;
}
