mod common;

use std::time::Duration;

use chrono::{SubsecRound, Utc};
use common::TestDb;
use diesel_async::{AsyncConnection, AsyncPgConnection, SimpleAsyncConnection};
use futures::StreamExt;
use pg_task_scheduler::{EnqueueOptions, SchedulerError, Task, TaskKey, TaskStatus, enqueue};
use serde_json::json;

struct Email;
impl Task for Email {
    const NAME: &'static str = "email";
    type Args = serde_json::Value;
}

struct Sms;
impl Task for Sms {
    const NAME: &'static str = "sms";
    type Args = serde_json::Value;
}

fn key(s: &str) -> TaskKey {
    TaskKey::try_from(s).unwrap()
}

#[tokio::test]
async fn enqueue_inserts_queued_task() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let e = enqueue::<Email>(
        &mut conn,
        key("k1"),
        json!({"to": "a"}),
        EnqueueOptions::immediate(),
    )
    .await
    .unwrap();
    assert!(e.inserted);
    assert_eq!(e.status, TaskStatus::Queued);
    assert_eq!(
        db.scalar(
            "SELECT count(*) AS n FROM queue_tasks \
             WHERE task_key = 'k1' AND lock_token = 0 AND attempts = 0"
        )
        .await,
        1
    );
    db.cleanup().await;
}

#[tokio::test]
async fn duplicate_enqueue_returns_existing_task() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let a = enqueue::<Email>(
        &mut conn,
        key("k"),
        json!({"x": 1}),
        EnqueueOptions::immediate(),
    )
    .await
    .unwrap();
    let b = enqueue::<Email>(
        &mut conn,
        key("k"),
        json!({"x": 1}),
        EnqueueOptions::immediate(),
    )
    .await
    .unwrap();
    assert_eq!(a.id, b.id);
    assert!(a.inserted);
    assert!(!b.inserted);
    db.cleanup().await;
}

#[tokio::test]
async fn concurrent_duplicate_enqueue_creates_one_task() {
    let db = TestDb::new().await;
    let mut handles = Vec::new();
    for _ in 0..16 {
        let pool = db.pool.clone();
        handles.push(tokio::spawn(async move {
            let mut conn = pool.get().await.unwrap();
            enqueue::<Email>(
                &mut conn,
                key("same"),
                json!({"x": 1}),
                EnqueueOptions::immediate(),
            )
            .await
            .unwrap()
        }));
    }
    let mut results = Vec::new();
    for h in handles {
        results.push(h.await.unwrap());
    }
    assert_eq!(db.scalar("SELECT count(*) AS n FROM queue_tasks").await, 1);
    assert!(results.iter().all(|r| r.id == results[0].id));
    assert_eq!(results.iter().filter(|r| r.inserted).count(), 1);
    db.cleanup().await;
}

#[tokio::test]
async fn conflicting_duplicate_is_rejected() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    enqueue::<Email>(
        &mut conn,
        key("k"),
        json!({"x": 1}),
        EnqueueOptions::immediate(),
    )
    .await
    .unwrap();
    let different_payload = enqueue::<Email>(
        &mut conn,
        key("k"),
        json!({"x": 2}),
        EnqueueOptions::immediate(),
    )
    .await;
    assert!(matches!(
        different_payload,
        Err(SchedulerError::IdempotencyConflict(k)) if k.as_str() == "k"
    ));
    let different_name = enqueue::<Sms>(
        &mut conn,
        key("k"),
        json!({"x": 1}),
        EnqueueOptions::immediate(),
    )
    .await;
    assert!(matches!(
        different_name,
        Err(SchedulerError::IdempotencyConflict(_))
    ));
    db.cleanup().await;
}

#[tokio::test]
async fn duplicate_with_reordered_payload_keys_is_not_a_conflict() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let first = enqueue::<Email>(
        &mut conn,
        key("k"),
        json!({"a": 1, "b": 2}),
        EnqueueOptions::immediate(),
    )
    .await
    .unwrap();
    let reordered: serde_json::Value = serde_json::from_str(r#"{"b":2,"a":1}"#).unwrap();
    let second = enqueue::<Email>(&mut conn, key("k"), reordered, EnqueueOptions::immediate())
        .await
        .unwrap();
    assert_eq!(first.id, second.id);
    assert!(!second.inserted);
    db.cleanup().await;
}

#[tokio::test]
async fn enqueue_rolls_back_with_callers_transaction() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    conn.batch_execute("CREATE TABLE domain_rows (v INT)")
        .await
        .unwrap();
    conn.batch_execute("BEGIN").await.unwrap();
    conn.batch_execute("INSERT INTO domain_rows VALUES (1)")
        .await
        .unwrap();
    enqueue::<Email>(&mut conn, key("rb"), json!({}), EnqueueOptions::immediate())
        .await
        .unwrap();
    conn.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(db.scalar("SELECT count(*) AS n FROM domain_rows").await, 0);
    assert_eq!(db.scalar("SELECT count(*) AS n FROM queue_tasks").await, 0);
    db.cleanup().await;
}

#[tokio::test]
async fn scheduled_enqueue_sets_due_at() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let t = (Utc::now() + chrono::Duration::hours(1)).trunc_subsecs(6);
    let e = enqueue::<Email>(&mut conn, key("later"), json!({}), EnqueueOptions::at(t))
        .await
        .unwrap();
    assert_eq!(e.due_at, t);
    db.cleanup().await;
}

#[tokio::test]
async fn enqueue_notifies_queue_available() {
    let db = TestDb::new().await;
    let mut listener = AsyncPgConnection::establish(&common::database_url())
        .await
        .unwrap();
    // Notifications are database-wide, so listen on the shared database while the
    // enqueue runs in the per-test schema.
    listener
        .batch_execute("LISTEN queue_available")
        .await
        .unwrap();
    let mut conn = db.pool.get().await.unwrap();
    enqueue::<Email>(&mut conn, key("n"), json!({}), EnqueueOptions::immediate())
        .await
        .unwrap();
    let mut stream = std::pin::pin!(listener.notifications_stream());
    let n = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("notification within 1s")
        .expect("stream open")
        .unwrap();
    assert_eq!(n.channel, "queue_available");
    assert_eq!(n.payload, "");
    db.cleanup().await;
}
