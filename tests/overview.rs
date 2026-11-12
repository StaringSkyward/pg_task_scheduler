mod common;

use std::time::Duration;

use common::TestDb;
use diesel_async::SimpleAsyncConnection;
use pg_task_scheduler::store::{
    self, Count, Overview, OverviewParams, QueueSlots, WorkerRegistration, WorkerStatus,
};
use pg_task_scheduler::{QueueName, WorkerId};

async fn sql(db: &TestDb, stmt: &str) {
    let mut conn = db.pool.get().await.unwrap();
    conn.batch_execute(stmt).await.unwrap();
}

async fn overview(db: &TestDb, p: &OverviewParams) -> Overview {
    let mut conn = db.pool.get().await.unwrap();
    store::overview(&mut conn, p).await.unwrap()
}

fn c(value: i64, capped: bool) -> Count {
    Count { value, capped }
}

#[tokio::test]
async fn overview_of_empty_install() {
    let db = TestDb::new().await;
    let p = OverviewParams::default();
    let o = overview(&db, &p).await;
    assert!(o.queues.is_empty() && o.workers.is_empty() && o.paused.is_empty());
    assert!(o.failure_groups.is_empty() && o.no_handler.is_empty() && o.recent_actions.is_empty());
    assert_eq!(
        (o.done_covered, o.dead_covered, o.attempts_covered, o.window),
        (p.window, p.window, p.window, p.window)
    );
    db.cleanup().await;
}

#[tokio::test]
async fn overview_counts_per_queue() {
    let db = TestDb::new().await;
    sql(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name, queue, due_at) VALUES \
           ('d1', 'a', 'default', now() - interval '10 minutes'), \
           ('d2', 'a', 'default', now() - interval '1 minute'), \
           ('d3', 'a', 'default', now()), \
           ('f1', 'a', 'default', now() + interval '1 hour'), \
           ('b1', 'a', 'bulk', now()); \
         INSERT INTO queue_tasks (task_key, task_name, queue, status, locked_by, locked_until) \
           VALUES ('r1', 'a', 'default', 'running', 'w', now() + interval '1 minute'); \
         INSERT INTO queue_tasks (task_key, task_name, queue, status, completed_at) VALUES \
           ('x1', 'a', 'default', 'done', now()), ('x2', 'a', 'default', 'done', now()), \
           ('y1', 'a', 'default', 'dead', now())",
    )
    .await;
    let o = overview(&db, &OverviewParams::default()).await;
    let names: Vec<_> = o.queues.iter().map(|q| q.queue.as_str()).collect();
    assert_eq!(names, ["bulk", "default"]);
    let (bulk, def) = (&o.queues[0], &o.queues[1]);
    assert_eq!(
        (bulk.queued, bulk.due, bulk.running),
        (c(1, false), c(1, false), 0)
    );
    assert_eq!(
        (def.queued, def.due, def.running),
        (c(4, false), c(3, false), 1)
    );
    assert_eq!((def.done, def.dead), (c(2, false), c(1, false)));
    let age = def.oldest_due_age.unwrap();
    assert!(
        age >= Duration::from_secs(600) && age < Duration::from_secs(660),
        "{age:?}"
    );
    db.cleanup().await;
}

#[tokio::test]
async fn overview_caps_counts() {
    let db = TestDb::new().await;
    sql(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name, due_at) \
           SELECT 'd' || g, 'a', now() FROM generate_series(1, 8) g; \
         INSERT INTO queue_tasks (task_key, task_name, status, completed_at) \
           SELECT 'x' || g, 'a', 'done', now() - g * interval '1 minute' \
           FROM generate_series(1, 8) g",
    )
    .await;
    let p = OverviewParams {
        count_cap: 5,
        ..Default::default()
    };
    let o = overview(&db, &p).await;
    assert_eq!(o.queues[0].due, c(5, true));
    assert!(o.queues[0].done.capped && o.queues[0].done.value == 5);
    assert!(o.done_covered < p.window && o.done_covered >= Duration::from_secs(240));
    assert_eq!(o.dead_covered, p.window);
    db.cleanup().await;
}

#[tokio::test]
async fn overview_groups_failures() {
    let db = TestDb::new().await;
    sql(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name, status, completed_at, last_error) \
           VALUES ('k1', 'send', 'dead', now(), 'smtp refused'); \
         INSERT INTO queue_task_attempts \
           (task_id, attempt_no, lock_token, worker_id, finished_at, outcome, error) \
           SELECT id, 1, 1, 'w', now(), 'retry', 'smtp refused' FROM queue_tasks; \
         INSERT INTO queue_task_attempts \
           (task_id, attempt_no, lock_token, worker_id, finished_at, outcome, error) \
           SELECT id, 2, 2, 'w', now(), 'retry', 'smtp refused' FROM queue_tasks",
    )
    .await;
    let o = overview(&db, &OverviewParams::default()).await;
    assert_eq!(o.failure_groups.len(), 1);
    let g = &o.failure_groups[0];
    assert_eq!(
        (g.task_name.as_str(), g.error.as_str()),
        ("send", "smtp refused")
    );
    assert_eq!((g.dead, g.failed_attempts), (c(1, false), c(2, false)));
    db.cleanup().await;
}

/// The failed-attempt groups read the newest `count_cap` attempts that started in the window,
/// failed or not. When that scan is full, each group's count is a lower bound and
/// `attempts_covered` is the span the scan reached.
#[tokio::test]
async fn overview_caps_failed_attempts() {
    let db = TestDb::new().await;
    sql(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name, status, completed_at) \
           VALUES ('k1', 'send', 'done', now()); \
         INSERT INTO queue_task_attempts \
           (task_id, attempt_no, lock_token, worker_id, started_at, finished_at, outcome, error) \
           SELECT (SELECT id FROM queue_tasks), g, g, 'w', now() - g * interval '1 minute', \
                  now() - g * interval '1 minute', \
                  CASE WHEN g = 1 THEN 'done' ELSE 'retry' END, \
                  CASE WHEN g = 1 THEN NULL ELSE 'smtp refused' END \
           FROM generate_series(1, 8) g",
    )
    .await;
    let p = OverviewParams {
        count_cap: 5,
        ..Default::default()
    };
    let o = overview(&db, &p).await;
    // The newest five attempts: one done, four retries.
    assert_eq!(o.failure_groups.len(), 1);
    assert_eq!(o.failure_groups[0].failed_attempts, c(4, true));
    assert!(
        o.attempts_covered < p.window && o.attempts_covered >= Duration::from_secs(300),
        "{:?}",
        o.attempts_covered
    );
    // Below the cap the count is exact and the whole window is covered.
    let o = overview(&db, &OverviewParams::default()).await;
    assert_eq!(o.failure_groups[0].failed_attempts, c(7, false));
    assert_eq!(o.attempts_covered, o.window);
    db.cleanup().await;
}

fn registration(queue: &str, names: &[&str]) -> WorkerRegistration {
    WorkerRegistration {
        worker_id: WorkerId::generate(),
        label: None,
        status: WorkerStatus::Healthy,
        last_error: None,
        queues: vec![QueueSlots {
            name: QueueName::try_from(queue).unwrap(),
            limit: 1,
            in_use: 0,
            finished_ago: None,
        }],
        task_names: names.iter().map(|s| (*s).to_owned()).collect(),
        lost_after: Duration::from_secs(60),
    }
}

#[tokio::test]
async fn overview_finds_unhandled_due_names() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    store::upsert_worker(&mut conn, &registration("default", &["a"]))
        .await
        .unwrap();
    drop(conn);
    sql(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name) VALUES ('1', 'a'), ('2', 'b'); \
         INSERT INTO queue_tasks (task_key, task_name, queue) VALUES ('3', 'z', 'orphan')",
    )
    .await;
    let o = overview(&db, &OverviewParams::default()).await;
    // `orphan` has no live worker: the health rule reports it, not the probe.
    assert_eq!(o.no_handler.len(), 1);
    assert_eq!(o.no_handler[0].queue.as_str(), "default");
    let names: Vec<_> = o.no_handler[0]
        .task_names
        .iter()
        .map(|n| n.as_str())
        .collect();
    assert_eq!(names, ["b"]);
    db.cleanup().await;
}

#[tokio::test]
async fn overview_lists_paused_and_registered_queues_without_tasks() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    store::upsert_worker(&mut conn, &registration("served", &["a"]))
        .await
        .unwrap();
    drop(conn);
    sql(
        &db,
        "INSERT INTO queue_pauses (queue, operator, reason) VALUES ('held', 'op', 'why')",
    )
    .await;
    let o = overview(&db, &OverviewParams::default()).await;
    let names: Vec<_> = o.queues.iter().map(|q| q.queue.as_str()).collect();
    assert_eq!(names, ["held", "served"]);
    assert_eq!(o.queues[0].queued, c(0, false));
    assert_eq!((o.paused.len(), o.workers.len()), (1, 1));
    db.cleanup().await;
}

#[tokio::test]
async fn overview_queue_set_covers_every_queued_queue() {
    let db = TestDb::new().await;
    sql(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name, queue, priority) \
         SELECT q || g, 'a', q, g FROM unnest(array['b-q', 'a-q', 'd-q', 'c-q']) q, \
                generate_series(1, 4) g",
    )
    .await;
    let o = overview(&db, &OverviewParams::default()).await;
    let names: Vec<_> = o.queues.iter().map(|q| q.queue.as_str()).collect();
    assert_eq!(names, ["a-q", "b-q", "c-q", "d-q"]);
    assert!(o.queues.iter().all(|q| q.queued == c(4, false)));
    db.cleanup().await;
}
