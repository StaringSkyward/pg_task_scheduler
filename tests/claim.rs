mod common;

use std::num::NonZeroU32;
use std::time::Duration;

use chrono::Utc;
use common::TestDb;
use pg_task_scheduler::AttemptNo;
use pg_task_scheduler::AttemptOutcome;
use pg_task_scheduler::AttemptState;
use pg_task_scheduler::EnqueueOptions;
use pg_task_scheduler::LockToken;
use pg_task_scheduler::Priority;
use pg_task_scheduler::QueueName;
use pg_task_scheduler::Task;
use pg_task_scheduler::TaskId;
use pg_task_scheduler::TaskKey;
use pg_task_scheduler::TaskName;
use pg_task_scheduler::TaskPayload;
use pg_task_scheduler::TaskState;
use pg_task_scheduler::TaskStatus;
use pg_task_scheduler::WorkerId;
use pg_task_scheduler::enqueue;
use pg_task_scheduler::store;
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

fn lease() -> pg_task_scheduler::LeaseDuration {
    common::lease(Duration::from_secs(30))
}

fn key(s: &str) -> TaskKey {
    TaskKey::try_from(s).unwrap()
}

fn names() -> Vec<TaskName> {
    vec![TaskName::try_from("email").unwrap()]
}

fn default_queue() -> QueueName {
    QueueName::default()
}

fn bulk() -> QueueName {
    QueueName::try_from("bulk").unwrap()
}

fn one() -> NonZeroU32 {
    NonZeroU32::new(1).unwrap()
}

#[tokio::test]
async fn claim_marks_running_and_records_attempt() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let e = enqueue::<Email>(&mut conn, key("k"), json!({}), EnqueueOptions::immediate())
        .await
        .unwrap();
    let worker = WorkerId::generate();
    let claimed = store::claim(
        &mut conn,
        &worker,
        &default_queue(),
        &names(),
        one(),
        lease(),
    )
    .await
    .unwrap();
    assert_eq!(claimed.len(), 1);
    let c = &claimed[0];
    assert_eq!(c.lease.task_id, e.id);
    assert_eq!(c.lease.attempt_no, AttemptNo::FIRST);
    assert_eq!(c.lease.lock_token, LockToken(1));

    let t = store::task(&mut conn, e.id).await.unwrap().unwrap();
    assert_eq!(t.attempts, 1);
    assert_eq!(t.lock_token, LockToken(1));
    let TaskState::Running {
        locked_by,
        locked_until,
    } = &t.state
    else {
        panic!("not running: {:?}", t.state);
    };
    assert_eq!(locked_by, &worker);
    assert!(*locked_until > c.claimed_at);

    let attempts = store::attempts(&mut conn, e.id).await.unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].attempt_no, AttemptNo::FIRST);
    assert_eq!(attempts[0].lock_token, LockToken(1));
    assert_eq!(attempts[0].worker_id, worker);
    assert_eq!(attempts[0].state, AttemptState::Open);
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
                lease(),
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
        store::claim(&mut conn, &w, &default_queue(), &names(), one(), lease())
            .await
            .unwrap()
            .is_empty()
    );
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(
        store::claim(&mut conn, &w, &default_queue(), &names(), one(), lease())
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
        lease(),
    )
    .await
    .unwrap();
    assert_eq!(claimed[0].lease.task_id, high.id);
    for id in [low.id, delayed.id] {
        let t = store::task(&mut conn, id).await.unwrap().unwrap();
        assert_eq!(t.state.status(), TaskStatus::Queued);
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
        lease(),
    )
    .await
    .unwrap();
    assert!(claimed.is_empty());
    let t = store::task(&mut conn, e.id).await.unwrap().unwrap();
    assert_eq!(t.state.status(), TaskStatus::Queued);
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
        NonZeroU32::new(3).unwrap(),
        lease(),
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
    let two = NonZeroU32::new(2).unwrap();
    let w = WorkerId::generate();
    let got = store::claim(&mut conn, &w, &bulk(), &names(), two, lease())
        .await
        .unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].task_key, key("b"));
    assert_eq!(got[0].queue, bulk());
    let got = store::claim(&mut conn, &w, &default_queue(), &names(), two, lease())
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

#[tokio::test]
async fn claim_dead_letters_a_row_with_an_invalid_task_key_and_returns_the_rest() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let good = enqueue::<Email>(
        &mut conn,
        key("good"),
        json!({}),
        EnqueueOptions::immediate(),
    )
    .await
    .unwrap();
    // A raw-SQL producer: queue_tasks has no identifier CHECK, so a 300-char key (TaskKey allows
    // 255) can be stored.
    let bad = TaskId(
        db.scalar(
            "WITH t AS (INSERT INTO queue_tasks (task_key, task_name) \
             VALUES (repeat('x', 300), 'email') RETURNING id) SELECT id AS n FROM t",
        )
        .await,
    );
    let worker = WorkerId::generate();
    let claimed = store::claim(
        &mut conn,
        &worker,
        &default_queue(),
        &names(),
        NonZeroU32::new(10).unwrap(),
        lease(),
    )
    .await
    .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].lease.task_id, good.id);
    let t = store::task(&mut conn, good.id).await.unwrap().unwrap();
    assert_eq!(t.state.status(), TaskStatus::Running);

    let dead = db
        .scalar(&format!(
            "SELECT count(*) AS n FROM queue_tasks WHERE id = {} AND status = 'dead' \
             AND last_error = 'invalid task_key: identifier must be at most 255 characters'",
            bad.0
        ))
        .await;
    assert_eq!(dead, 1);
    // The dead letter can still be opened, with the raw key.
    let view = store::task(&mut conn, bad).await.unwrap().unwrap();
    match &view.task_key {
        pg_task_scheduler::StoredTaskKey::Invalid { text, error } => {
            assert_eq!(text.len(), 300);
            assert_eq!(*error, pg_task_scheduler::IdentifierError::TooLong);
        }
        other => panic!("expected Invalid, got {other:?}"),
    }
    assert_eq!(view.task_key.as_str().len(), 300);
    let attempts = store::attempts(&mut conn, bad).await.unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(common::outcome(&attempts[0]), Some(AttemptOutcome::Dead));
    assert_eq!(
        attempts[0].error.as_deref(),
        Some("invalid task_key: identifier must be at most 255 characters")
    );
    db.cleanup().await;
}

#[tokio::test]
async fn claim_dead_letters_a_row_whose_payload_does_not_parse_and_returns_the_rest() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    // PostgreSQL accepts this JSONB; serde_json rejects the out-of-range number.
    let bad = TaskId(
        db.scalar(
            "WITH t AS (INSERT INTO queue_tasks (task_key, task_name, payload) \
             VALUES ('bad', 'email', '{\"x\":1e500}'::jsonb) RETURNING id) SELECT id AS n FROM t",
        )
        .await,
    );
    let good = TaskId(
        db.scalar(
            "WITH t AS (INSERT INTO queue_tasks (task_key, task_name, payload) \
             VALUES ('good', 'email', '{}') RETURNING id) SELECT id AS n FROM t",
        )
        .await,
    );
    let claimed = store::claim(
        &mut conn,
        &WorkerId::generate(),
        &default_queue(),
        &names(),
        NonZeroU32::new(2).unwrap(),
        lease(),
    )
    .await
    .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].lease.task_id, good);
    let t = store::task(&mut conn, good).await.unwrap().unwrap();
    assert_eq!(t.state.status(), TaskStatus::Running);
    // Check the dead letter in SQL.
    let dead = db
        .scalar(&format!(
            "SELECT count(*) AS n FROM queue_tasks WHERE id = {} AND status = 'dead' \
             AND last_error LIKE '%payload%'",
            bad.0
        ))
        .await;
    assert_eq!(dead, 1);
    let attempts = store::attempts(&mut conn, bad).await.unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(common::outcome(&attempts[0]), Some(AttemptOutcome::Dead));
    db.cleanup().await;
}

#[tokio::test]
async fn task_with_an_unparseable_payload_is_still_readable() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let bad = TaskId(
        db.scalar(
            "WITH t AS (INSERT INTO queue_tasks (task_key, task_name, payload) \
             VALUES ('bad', 'email', '{\"x\":1e500}'::jsonb) RETURNING id) SELECT id AS n FROM t",
        )
        .await,
    );
    store::claim(
        &mut conn,
        &WorkerId::generate(),
        &default_queue(),
        &names(),
        one(),
        lease(),
    )
    .await
    .unwrap();
    let view = store::task(&mut conn, bad).await.unwrap().unwrap();
    assert_eq!(view.state.status(), TaskStatus::Dead);
    let TaskPayload::Unreadable { text, error } = view.payload else {
        panic!("expected unreadable");
    };
    // PostgreSQL prints the numeric expanded, not as `1e500`.
    assert!(text.starts_with(r#"{"x": 1000"#), "{text}");
    assert!(!error.is_empty());
    db.cleanup().await;
}
