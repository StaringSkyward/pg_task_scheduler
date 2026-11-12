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
use pg_task_scheduler_admin::AdminConfig;
use pg_task_scheduler_admin::Operator;
use pg_task_scheduler_admin::router;
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

/// The overview's queue-table cells for `queue`, keyed by column header, tags stripped.
fn queue_row(body: &str, queue: &str) -> std::collections::HashMap<String, String> {
    let strip = |html: &str| {
        let mut out = String::new();
        let mut in_tag = false;
        for c in html.chars() {
            match c {
                '<' => in_tag = true,
                '>' => in_tag = false,
                c if !in_tag => out.push(c),
                _ => {}
            }
        }
        out.trim().to_owned()
    };
    let table = body
        .split("<h2>Queues</h2>")
        .nth(1)
        .and_then(|t| t.split("</table>").next())
        .expect("queue table");
    let headers: Vec<String> = table
        .split("<th>")
        .skip(1)
        .map(|h| strip(h.split("</th>").next().unwrap()))
        .collect();
    let link = format!("href=\"/queued?queue={queue}\"");
    let row = table
        .split("<tr>")
        .find(|r| r.contains(&link))
        .expect("queue row");
    let cells = row
        .split("<td>")
        .skip(1)
        .map(|c| strip(c.split("</td>").next().unwrap()));
    headers.into_iter().zip(cells).collect()
}

#[tokio::test]
async fn overview_shows_queued_total_beside_due() {
    let db = TestDb::new().await;
    sql(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name, queue, due_at) \
         SELECT 'k' || g, 'send', 'email', \
                CASE WHEN g <= 2 THEN now() - interval '1 minute' \
                     ELSE now() + interval '1 hour' END \
         FROM generate_series(1, 5) g",
    )
    .await;
    let (status, _, body) = get(app(&db), "/").await;
    assert_eq!(status, StatusCode::OK);
    let row = queue_row(&body, "email");
    assert_eq!(row.get("Queued").map(String::as_str), Some("5"), "{row:?}");
    assert_eq!(row.get("Due").map(String::as_str), Some("2"), "{row:?}");
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

/// The `href` values on a page, with HTML entities of `&` undone.
fn hrefs(body: &str) -> Vec<String> {
    body.split("href=\"")
        .skip(1)
        .map(|p| p.split('"').next().unwrap().replace("&amp;", "&"))
        .collect()
}

/// Every `<td>` cell text of the first `<td>` column, in row order (ids here).
fn first_cells(body: &str, nth: usize) -> Vec<String> {
    body.split("<tr")
        .skip(2)
        .map(|r| {
            r.split("<td")
                .nth(nth + 1)
                .unwrap_or("")
                .trim_start_matches(|c| c != '>')[1..]
                .split("</td>")
                .next()
                .unwrap()
                .trim()
                .to_owned()
        })
        .collect()
}

#[tokio::test]
async fn queued_page_lists_in_dequeue_order_with_next_link() {
    let db = TestDb::new().await;
    sql(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name, queue, priority, due_at) \
         SELECT 'k' || g, 'n', 'q', CASE WHEN g = 60 THEN 9 ELSE 0 END, \
                now() - interval '1 hour' + g * interval '1 second' \
         FROM generate_series(1, 60) g",
    )
    .await;
    let (status, _, body) = get(app(&db), "/queued?queue=q").await;
    assert_eq!(status, StatusCode::OK);
    let ids = first_cells(&body, 1);
    assert_eq!(ids.len(), 50, "{body}");
    // The highest priority task (last inserted) comes first.
    assert!(ids[0].contains(">60<"), "{ids:?}");
    let next = hrefs(&body)
        .into_iter()
        .find(|h| h.contains("position="))
        .expect("next link");
    let (status, _, body2) = get(app(&db), &next).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first_cells(&body2, 1).len(), 10, "{body2}");
    assert!(!hrefs(&body2).iter().any(|h| h.contains("position=")));
    // A malformed cursor is a 400.
    let (status, _, _) = get(app(&db), "/queued?queue=q&id=x&position=1").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    db.cleanup().await;
}

#[tokio::test]
async fn queued_without_queue_offers_known_queues() {
    let db = TestDb::new().await;
    sql(&db, DUE_EMAIL).await;
    let (status, _, body) = get(app(&db), "/queued").await;
    assert_eq!(status, StatusCode::OK);
    assert!(hrefs(&body).contains(&"/queued?queue=email".to_owned()));
    db.cleanup().await;
}

#[tokio::test]
async fn all_list_routes_resolve() {
    let db = TestDb::new().await;
    for p in [
        "/workers",
        "/running",
        "/completed",
        "/failed",
        "/cancelled",
    ] {
        let (status, _, _) = get(app(&db), p).await;
        assert_eq!(status, StatusCode::OK, "{p}");
    }
    db.cleanup().await;
}

const DEAD: &str = "INSERT INTO queue_tasks (task_key, task_name, queue, status, last_error, completed_at) \
    VALUES ('d1', 'a', 'q', 'dead', 'boom', now()), ('d2', 'b', 'q', 'dead', 'boom', now()), \
           ('d3', 'a', 'q', 'dead', 'other', now()), \
           ('c1', 'a', 'q', 'cancelled', NULL, now())";

#[tokio::test]
async fn failed_page_filters_by_group() {
    let db = TestDb::new().await;
    sql(&db, DEAD).await;
    let (_, _, body) = get(app(&db), "/failed").await;
    assert_eq!(first_cells(&body, 0).len(), 3, "{body}");
    let (_, _, body) = get(app(&db), "/failed?task_name=a&error=boom").await;
    assert_eq!(first_cells(&body, 0).len(), 1, "{body}");
    let (_, _, body) = get(app(&db), "/cancelled").await;
    assert_eq!(first_cells(&body, 0).len(), 1, "{body}");
    db.cleanup().await;
}

#[tokio::test]
async fn task_page_shows_attempts_and_404s() {
    let db = TestDb::new().await;
    sql(&db, DEAD).await;
    sql(
        &db,
        "INSERT INTO queue_task_attempts (task_id, attempt_no, lock_token, worker_id, finished_at, outcome, error) \
         SELECT id, 1, 1, 'worker-7', now(), 'dead', 'boom' FROM queue_tasks WHERE task_key = 'd1'",
    )
    .await;
    let (status, _, body) = get(app(&db), "/tasks/1").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("<td>worker-7</td>"), "{body}");
    assert!(body.contains("<pre>"), "{body}");
    let (status, _, _) = get(app(&db), "/tasks/99999").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    db.cleanup().await;
}

#[tokio::test]
async fn names_with_url_characters_round_trip() {
    let db = TestDb::new().await;
    sql(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name, queue, due_at) \
         VALUES ('u1', 'a&b=c', 'eu/bulk & co #1', now() - interval '1 minute'), \
                ('u2', 'other', 'plain', now() - interval '1 minute')",
    )
    .await;
    sql(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name, queue, status, last_error, completed_at) \
         VALUES ('u3', 'a&b=c', 'q', 'dead', 'x&y', now()), \
                ('u4', 'a&b=c', 'q', 'dead', 'x', now())",
    )
    .await;
    let (_, _, body) = get(app(&db), "/").await;
    let links = hrefs(&body);
    let q = links
        .iter()
        .find(|h| h.contains("queued?queue=eu"))
        .expect("queue link");
    let (status, _, page) = get(app(&db), q).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first_cells(&page, 1).len(), 1, "{page}");
    assert!(page.contains("a&amp;b=c"), "{page}");
    // The failure group for error `x&y` lists exactly that task.
    let g = links
        .iter()
        .find(|h| h.contains("failed?task_name=") && h.contains("x%26y"))
        .expect("group link");
    let (status, _, page) = get(app(&db), g).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first_cells(&page, 0).len(), 1, "{page}");
    db.cleanup().await;
}

#[tokio::test]
async fn task_data_is_escaped() {
    let db = TestDb::new().await;
    sql(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name, queue, status, payload, last_error, completed_at) \
         VALUES ('x1', 'n', 'q', 'dead', '{\"x\":\"<script>alert(1)</script>\"}', '<b>boom</b>', now())",
    )
    .await;
    sql(
        &db,
        "INSERT INTO queue_admin_actions (action, task_id, affected, operator, reason) \
         SELECT 'cancel', id, 1, 'op', '<i>r</i>' FROM queue_tasks",
    )
    .await;
    sql(
        &db,
        "INSERT INTO queue_workers (worker_id, label, lost_after, status, queues, task_names) \
         VALUES ('w1', '<u>w</u>', now() + interval '1 minute', 'healthy', '[]', '{}')",
    )
    .await;
    for (path, want, raw) in [
        (
            "/tasks/1",
            ["&lt;script&gt;", "&lt;b&gt;boom", "&lt;i&gt;r"],
            ["<script>", "<b>boom", "<i>r"],
        ),
        (
            "/workers",
            ["&lt;u&gt;w", "&lt;u&gt;w", "&lt;u&gt;w"],
            ["<u>w", "<u>w", "<u>w"],
        ),
        (
            "/failed",
            ["&lt;b&gt;boom", "&lt;b&gt;boom", "&lt;b&gt;boom"],
            ["<b>boom", "<b>boom", "<b>boom"],
        ),
    ] {
        let (status, _, body) = get(app(&db), path).await;
        assert_eq!(status, StatusCode::OK, "{path}");
        for (w, r) in want.iter().zip(raw) {
            assert!(body.contains(w), "{path} {w}\n{body}");
            assert!(!body.contains(r), "{path} {r}\n{body}");
        }
    }
    db.cleanup().await;
}

/// The failure groups' "Failed attempts" header names the span its capped scan covered.
#[tokio::test]
async fn failed_attempts_header_shows_covered_span() {
    let db = TestDb::new().await;
    sql(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name, status, completed_at) \
           VALUES ('k1', 'send', 'done', now()); \
         INSERT INTO queue_task_attempts \
           (task_id, attempt_no, lock_token, worker_id, started_at, finished_at, outcome, error) \
           SELECT (SELECT id FROM queue_tasks), g, g, 'w', now() - g * interval '1 minute', \
                  now(), 'retry', 'boom' \
           FROM generate_series(1, 3) g",
    )
    .await;
    let (_, _, body) = get(app(&db), "/").await;
    assert!(body.contains("<th>Failed attempts (1h)</th>"), "{body}");
    let config = AdminConfig {
        overview: pg_task_scheduler::store::OverviewParams {
            count_cap: 2,
            ..Default::default()
        },
        ..Default::default()
    };
    let capped = router(db.pool.clone(), config).layer(Extension(Operator("ops@example".into())));
    let (_, _, body) = get(capped, "/").await;
    assert!(body.contains("<th>Failed attempts (last 2m"), "{body}");
    db.cleanup().await;
}
