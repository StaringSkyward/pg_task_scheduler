mod common;

use std::num::NonZeroUsize;
use std::time::Duration;

use common::TestDb;
use diesel_async::SimpleAsyncConnection;
use pg_task_scheduler::EnqueueOptions;
use pg_task_scheduler::QueueName;
use pg_task_scheduler::Task;
use pg_task_scheduler::TaskKey;
use pg_task_scheduler::TaskName;
use pg_task_scheduler::WorkerId;
use pg_task_scheduler::enqueue;
use pg_task_scheduler::store::BulkFilter;
use pg_task_scheduler::store::{self};
use serde_json::json;

struct A;
impl Task for A {
    const NAME: &'static str = "a";
    type Args = serde_json::Value;
}

fn filter(name: &str) -> BulkFilter {
    BulkFilter {
        task_name: TaskName::try_from(name).unwrap(),
        queue: None,
    }
}

/// Insert `n` rows named `name` in `status` (terminal statuses need `completed_at`).
async fn insert_many(db: &TestDb, name: &str, status: &str, err: &str, n: i64) {
    let mut conn = db.pool.get().await.unwrap();
    conn.batch_execute(&format!(
        "INSERT INTO queue_tasks (task_key, task_name, status, last_error, completed_at) \
         SELECT '{name}-{status}-{err}-' || g, '{name}', '{status}', '{err}', \
                CASE WHEN '{status}' IN ('queued','running') THEN NULL ELSE now() END \
         FROM generate_series(1, {n}) g"
    ))
    .await
    .unwrap();
}

async fn count(db: &TestDb, name: &str, status: &str) -> i64 {
    db.scalar(&format!(
        "SELECT count(*) AS n FROM queue_tasks WHERE task_name = '{name}' AND status = '{status}'"
    ))
    .await
}

#[tokio::test]
async fn cancel_matching_cancels_queued_and_running_of_that_name_only() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    insert_many(&db, "a", "queued", "", 3).await;
    insert_many(&db, "b", "queued", "", 2).await;
    let claimed = store::claim(
        &mut conn,
        &WorkerId::generate(),
        &QueueName::default(),
        &["a".to_owned()],
        NonZeroUsize::new(1).unwrap(),
        Duration::from_secs(60),
    )
    .await
    .unwrap();
    assert_eq!(claimed.len(), 1);

    let out = store::cancel_matching(&mut conn, &filter("a"), "ops", "why", store::BULK_BUDGET)
        .await
        .unwrap();
    assert_eq!((out.affected, out.more_remain), (3, false));
    assert_eq!(count(&db, "a", "cancelled").await, 3);
    assert_eq!(count(&db, "b", "queued").await, 2);
    assert_eq!(
        db.scalar("SELECT count(*) AS n FROM queue_admin_actions WHERE affected = 3")
            .await,
        1
    );
    assert_eq!(
        db.scalar(&format!(
            "SELECT count(*) AS n FROM queue_tasks WHERE task_name = 'a' AND admin_action_id = {}",
            out.action_id
        ))
        .await,
        3
    );
    // The running one's attempt was closed.
    assert_eq!(
        db.scalar(
            "SELECT count(*) AS n FROM queue_task_attempts WHERE outcome = 'cancelled' \
             AND finished_at IS NOT NULL"
        )
        .await,
        1
    );
    drop(conn);
    db.cleanup().await;
}

#[tokio::test]
async fn cancel_matching_ignores_tasks_enqueued_after_it_started() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    insert_many(&db, "a", "queued", "", 5).await;
    let pool = db.pool.clone();
    let producer = tokio::spawn(async move {
        let mut c = pool.get().await.unwrap();
        for i in 0..40 {
            enqueue::<A>(
                &mut c,
                TaskKey::try_from(format!("late-{i}")).unwrap(),
                json!({}),
                EnqueueOptions::immediate(),
            )
            .await
            .unwrap();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let out = store::cancel_matching(&mut conn, &filter("a"), "ops", "why", store::BULK_BUDGET)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    producer.await.unwrap();

    let bound = db
        .scalar(&format!(
            "SELECT max_task_id AS n FROM queue_admin_actions WHERE id = {}",
            out.action_id
        ))
        .await;
    assert_eq!(
        db.scalar(&format!(
            "SELECT count(*) AS n FROM queue_tasks WHERE admin_action_id = {} AND id > {bound}",
            out.action_id
        ))
        .await,
        0
    );
    let after = format!("task_name = 'a' AND id > {bound}");
    assert!(
        db.scalar(&format!(
            "SELECT count(*) AS n FROM queue_tasks WHERE {after}"
        ))
        .await
            > 0
    );
    assert_eq!(
        db.scalar(&format!(
            "SELECT count(*) AS n FROM queue_tasks WHERE {after} AND status <> 'queued'"
        ))
        .await,
        0
    );
    drop(conn);
    db.cleanup().await;
}

#[tokio::test]
async fn bulk_stops_at_budget_and_continues() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    insert_many(&db, "a", "queued", "", 1200).await;
    let first = store::cancel_matching(&mut conn, &filter("a"), "ops", "why", Duration::ZERO)
        .await
        .unwrap();
    assert_eq!((first.affected, first.more_remain), (500, true));
    let mut total = first.affected;
    loop {
        let o = store::cancel_matching(&mut conn, &filter("a"), "ops", "why", Duration::ZERO)
            .await
            .unwrap();
        total += o.affected;
        if !o.more_remain {
            break;
        }
    }
    assert_eq!(total, 1200);
    assert_eq!(count(&db, "a", "cancelled").await, 1200);
    drop(conn);
    db.cleanup().await;
}

#[tokio::test]
async fn replay_matching_filters_by_last_error() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    insert_many(&db, "a", "dead", "x", 2).await;
    insert_many(&db, "a", "dead", "y", 1).await;
    insert_many(&db, "a", "cancelled", "x", 1).await;
    let out = store::replay_matching(
        &mut conn,
        &filter("a"),
        Some("x"),
        "ops",
        "why",
        store::BULK_BUDGET,
    )
    .await
    .unwrap();
    assert_eq!((out.affected, out.more_remain), (3, false));
    assert_eq!(count(&db, "a", "queued").await, 3);
    assert_eq!(count(&db, "a", "dead").await, 1);
    assert_eq!(
        db.scalar("SELECT count(*) AS n FROM queue_tasks WHERE last_error IS NOT NULL")
            .await,
        1
    );
    drop(conn);
    db.cleanup().await;
}

#[tokio::test]
async fn counts_are_capped() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    insert_many(&db, "a", "queued", "", 30).await;
    insert_many(&db, "a", "dead", "x", 30).await;
    assert_eq!(
        store::count_cancellable(&mut conn, &filter("a"), 10)
            .await
            .unwrap(),
        10
    );
    assert_eq!(
        store::count_cancellable(&mut conn, &filter("a"), 100)
            .await
            .unwrap(),
        30
    );
    assert_eq!(
        store::count_replayable(&mut conn, &filter("a"), Some("x"), 10)
            .await
            .unwrap(),
        10
    );
    drop(conn);
    db.cleanup().await;
}

#[tokio::test]
async fn concurrent_bulk_cancels_split_the_work() {
    let db = TestDb::new().await;
    insert_many(&db, "a", "queued", "", 1200).await;
    let mut c1 = db.pool.get().await.unwrap();
    let mut c2 = db.pool.get().await.unwrap();
    let f = filter("a");
    let (r1, r2) = tokio::join!(
        store::cancel_matching(&mut c1, &f, "ops", "one", store::BULK_BUDGET),
        store::cancel_matching(&mut c2, &f, "ops", "two", store::BULK_BUDGET),
    );
    let (o1, o2) = (r1.unwrap(), r2.unwrap());
    assert_eq!(o1.affected + o2.affected, 1200);
    assert_eq!(count(&db, "a", "cancelled").await, 1200);
    assert_eq!(
        db.scalar("SELECT count(*) AS n FROM queue_admin_actions")
            .await,
        2
    );
    drop((c1, c2));
    db.cleanup().await;
}
