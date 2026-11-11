mod common;

use std::num::NonZeroUsize;
use std::time::Duration;

use chrono::Utc;
use common::TestDb;
use pg_task_scheduler::store;
use pg_task_scheduler::{
    EnqueueOptions, LockToken, Priority, QueueName, Task, TaskId, TaskKey, TaskStatus, WorkerId,
    enqueue,
};
use serde_json::json;

struct Email;
impl Task for Email {
    const NAME: &'static str = "email";
    type Args = serde_json::Value;
}

struct Other;
impl Task for Other {
    const NAME: &'static str = "other";
    type Args = serde_json::Value;
}

const LEASE: Duration = Duration::from_secs(30);

fn key(s: &str) -> TaskKey {
    TaskKey::try_from(s).unwrap()
}

fn names() -> Vec<String> {
    vec!["email".to_owned()]
}

fn default_queue() -> QueueName {
    QueueName::default()
}

fn bulk() -> QueueName {
    QueueName::try_from("bulk").unwrap()
}

fn one() -> NonZeroUsize {
    NonZeroUsize::new(1).unwrap()
}

#[tokio::test]
async fn claim_marks_running_and_records_attempt() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let e = enqueue::<Email>(&mut conn, key("k"), json!({}), EnqueueOptions::immediate())
        .await
        .unwrap();
    let worker = WorkerId::generate();
    let claimed = store::claim(&mut conn, &worker, &default_queue(), &names(), one(), LEASE)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    let c = &claimed[0];
    assert_eq!(c.lease.task_id, e.id);
    assert_eq!(c.lease.attempt_no, 1);
    assert_eq!(c.lease.lock_token, LockToken(1));

    let t = store::task(&mut conn, e.id).await.unwrap().unwrap();
    assert_eq!(t.status, TaskStatus::Running);
    assert_eq!(t.attempts, 1);
    assert_eq!(t.lock_token, LockToken(1));
    assert_eq!(t.locked_by, Some(worker.clone()));
    assert!(t.locked_until.unwrap() > c.claimed_at);

    let attempts = store::attempts(&mut conn, e.id).await.unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].attempt_no, 1);
    assert_eq!(attempts[0].lock_token, LockToken(1));
    assert_eq!(attempts[0].worker_id, worker);
    assert_eq!(attempts[0].outcome, None);
    db.cleanup().await;
}

#[tokio::test]
async fn concurrent_claims_produce_one_owner() {
    let db = TestDb::new().await;
    {
        let mut conn = db.pool.get().await.unwrap();
        enqueue::<Email>(&mut conn, key("k"), json!({}), EnqueueOptions::immediate())
            .await
            .unwrap();
    }
    let mut handles = Vec::new();
    for _ in 0..16 {
        let pool = db.pool.clone();
        handles.push(tokio::spawn(async move {
            let mut conn = pool.get().await.unwrap();
            store::claim(
                &mut conn,
                &WorkerId::generate(),
                &default_queue(),
                &names(),
                one(),
                LEASE,
            )
            .await
            .unwrap()
        }));
    }
    let mut owners = 0;
    for h in handles {
        if !h.await.unwrap().is_empty() {
            owners += 1;
        }
    }
    assert_eq!(owners, 1);
    db.cleanup().await;
}

#[tokio::test]
async fn future_task_is_not_claimed_until_due() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    enqueue::<Email>(
        &mut conn,
        key("k"),
        json!({}),
        EnqueueOptions::at(Utc::now() + chrono::Duration::seconds(2)),
    )
    .await
    .unwrap();
    let w = WorkerId::generate();
    assert!(
        store::claim(&mut conn, &w, &default_queue(), &names(), one(), LEASE)
            .await
            .unwrap()
            .is_empty()
    );
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(
        store::claim(&mut conn, &w, &default_queue(), &names(), one(), LEASE)
            .await
            .unwrap()
            .len(),
        1
    );
    db.cleanup().await;
}

#[tokio::test]
async fn higher_priority_due_task_is_claimed_first() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let delayed = enqueue::<Email>(
        &mut conn,
        key("delayed"),
        json!({}),
        EnqueueOptions::at(Utc::now() + chrono::Duration::hours(1)),
    )
    .await
    .unwrap();
    let low = enqueue::<Email>(
        &mut conn,
        key("low"),
        json!({}),
        EnqueueOptions::immediate().priority(Priority::new(-1)),
    )
    .await
    .unwrap();
    let high = enqueue::<Email>(
        &mut conn,
        key("high"),
        json!({}),
        EnqueueOptions::immediate().priority(Priority::new(10)),
    )
    .await
    .unwrap();

    let claimed = store::claim(
        &mut conn,
        &WorkerId::generate(),
        &default_queue(),
        &names(),
        one(),
        LEASE,
    )
    .await
    .unwrap();
    assert_eq!(claimed[0].lease.task_id, high.id);
    for id in [low.id, delayed.id] {
        let t = store::task(&mut conn, id).await.unwrap().unwrap();
        assert_eq!(t.status, TaskStatus::Queued);
    }
    db.cleanup().await;
}

#[tokio::test]
async fn unregistered_task_name_is_never_claimed() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let e = enqueue::<Other>(&mut conn, key("k"), json!({}), EnqueueOptions::immediate())
        .await
        .unwrap();
    let claimed = store::claim(
        &mut conn,
        &WorkerId::generate(),
        &default_queue(),
        &names(),
        one(),
        LEASE,
    )
    .await
    .unwrap();
    assert!(claimed.is_empty());
    let t = store::task(&mut conn, e.id).await.unwrap().unwrap();
    assert_eq!(t.status, TaskStatus::Queued);
    assert_eq!(t.attempts, 0);
    db.cleanup().await;
}

#[tokio::test]
async fn batch_claim_respects_limit() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    for i in 0..5 {
        enqueue::<Email>(
            &mut conn,
            key(&format!("k{i}")),
            json!({}),
            EnqueueOptions::immediate(),
        )
        .await
        .unwrap();
    }
    let claimed = store::claim(
        &mut conn,
        &WorkerId::generate(),
        &default_queue(),
        &names(),
        NonZeroUsize::new(3).unwrap(),
        LEASE,
    )
    .await
    .unwrap();
    assert_eq!(claimed.len(), 3);
    for c in &claimed {
        assert_eq!(c.lease.lock_token, LockToken(1));
        assert_eq!(
            store::attempts(&mut conn, c.lease.task_id)
                .await
                .unwrap()
                .len(),
            1
        );
    }
    db.cleanup().await;
}

#[tokio::test]
async fn next_due_in_reports_wait_until_earliest_due() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    assert_eq!(
        store::next_due_in(&mut conn, &[default_queue()], &names())
            .await
            .unwrap(),
        None
    );
    enqueue::<Email>(
        &mut conn,
        key("later"),
        json!({}),
        EnqueueOptions::at(Utc::now() + chrono::Duration::seconds(10)),
    )
    .await
    .unwrap();
    let d = store::next_due_in(&mut conn, &[default_queue()], &names())
        .await
        .unwrap()
        .unwrap();
    assert!(
        d > Duration::from_secs(9) && d <= Duration::from_secs(10),
        "{d:?}"
    );
    enqueue::<Email>(
        &mut conn,
        key("now"),
        json!({}),
        EnqueueOptions::immediate(),
    )
    .await
    .unwrap();
    assert_eq!(
        store::next_due_in(&mut conn, &[default_queue()], &names())
            .await
            .unwrap(),
        Some(Duration::ZERO)
    );
    db.cleanup().await;
}

#[tokio::test]
async fn missing_task_is_none() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    assert!(
        store::task(&mut conn, TaskId(i64::MAX))
            .await
            .unwrap()
            .is_none()
    );
    db.cleanup().await;
}

#[tokio::test]
async fn claim_takes_only_the_requested_queue() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    enqueue::<Email>(&mut conn, key("d"), json!({}), EnqueueOptions::immediate())
        .await
        .unwrap();
    enqueue::<Email>(
        &mut conn,
        key("b"),
        json!({}),
        EnqueueOptions::immediate().queue(bulk()),
    )
    .await
    .unwrap();
    let two = NonZeroUsize::new(2).unwrap();
    let w = WorkerId::generate();
    let got = store::claim(&mut conn, &w, &bulk(), &names(), two, LEASE)
        .await
        .unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].task_key, key("b"));
    assert_eq!(got[0].queue, bulk());
    let got = store::claim(&mut conn, &w, &default_queue(), &names(), two, LEASE)
        .await
        .unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].task_key, key("d"));
    db.cleanup().await;
}

#[tokio::test]
async fn next_due_in_covers_only_the_given_queues() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    enqueue::<Email>(&mut conn, key("d"), json!({}), EnqueueOptions::immediate())
        .await
        .unwrap();
    assert_eq!(
        store::next_due_in(&mut conn, &[bulk()], &names())
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store::next_due_in(&mut conn, &[bulk(), default_queue()], &names())
            .await
            .unwrap(),
        Some(Duration::ZERO)
    );
    enqueue::<Email>(
        &mut conn,
        key("b"),
        json!({}),
        EnqueueOptions::at(Utc::now() + chrono::Duration::seconds(10)).queue(bulk()),
    )
    .await
    .unwrap();
    let d = store::next_due_in(&mut conn, &[bulk()], &names())
        .await
        .unwrap()
        .unwrap();
    assert!(
        d > Duration::from_secs(9) && d <= Duration::from_secs(10),
        "{d:?}"
    );
    db.cleanup().await;
}
