mod common;

use std::time::Duration;

use chrono::SubsecRound;
use chrono::Utc;
use common::TestDb;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::SimpleAsyncConnection;
use futures::StreamExt;
use pg_task_scheduler::EnqueueOptions;
use pg_task_scheduler::IdentifierError;
use pg_task_scheduler::QueueName;
use pg_task_scheduler::SchedulerError;
use pg_task_scheduler::Task;
use pg_task_scheduler::TaskKey;
use pg_task_scheduler::TaskStatus;
use pg_task_scheduler::enqueue;
use pg_task_scheduler::store;
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

struct Bulk;
impl Task for Bulk {
    const NAME: &'static str = "bulk-email";
    const QUEUE: &'static str = "bulk";
    type Args = serde_json::Value;
}

struct BadQueue;
impl Task for BadQueue {
    const NAME: &'static str = "bad";
    const QUEUE: &'static str = "";
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

#[tokio::test]
async fn queue_comes_from_task_type_unless_overridden() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let a = enqueue::<Email>(&mut conn, key("a"), json!({}), EnqueueOptions::immediate())
        .await
        .unwrap();
    let b = enqueue::<Bulk>(&mut conn, key("b"), json!({}), EnqueueOptions::immediate())
        .await
        .unwrap();
    let c = enqueue::<Email>(
        &mut conn,
        key("c"),
        json!({}),
        EnqueueOptions::immediate().queue(QueueName::try_from("urgent").unwrap()),
    )
    .await
    .unwrap();
    for (e, want) in [(a, "default"), (b, "bulk"), (c, "urgent")] {
        let t = store::task(&mut conn, e.id).await.unwrap().unwrap();
        assert_eq!(t.queue.as_str(), want);
    }
    db.cleanup().await;
}

#[tokio::test]
async fn same_key_in_another_queue_returns_the_existing_task() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let first = enqueue::<Email>(&mut conn, key("k"), json!({}), EnqueueOptions::immediate())
        .await
        .unwrap();
    let second = enqueue::<Email>(
        &mut conn,
        key("k"),
        json!({}),
        EnqueueOptions::immediate().queue(QueueName::try_from("bulk").unwrap()),
    )
    .await
    .unwrap();
    assert!(!second.inserted);
    assert_eq!(second.id, first.id);
    let t = store::task(&mut conn, first.id).await.unwrap().unwrap();
    assert_eq!(t.queue.as_str(), "default");
    db.cleanup().await;
}

#[tokio::test]
async fn invalid_task_queue_is_rejected() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let err = enqueue::<BadQueue>(&mut conn, key("k"), json!({}), EnqueueOptions::immediate())
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        SchedulerError::Identifier(IdentifierError::Empty)
    ));
    assert_eq!(db.scalar("SELECT count(*) AS n FROM queue_tasks").await, 0);
    db.cleanup().await;
}

#[tokio::test]
async fn enqueue_rejects_the_schedule_prefix() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let r = enqueue::<Email>(
        &mut conn,
        key("schedule:imported-1"),
        json!({}),
        EnqueueOptions::immediate(),
    )
    .await;
    assert!(
        matches!(r, Err(SchedulerError::ReservedTaskKey(_))),
        "{r:?}"
    );
    assert_eq!(db.scalar("SELECT count(*) AS n FROM queue_tasks").await, 0);
    db.cleanup().await;
}

fn too_deep() -> serde_json::Value {
    (0..140).fold(json!([]), |v, _| json!([v]))
}

#[tokio::test]
async fn enqueue_rejects_a_payload_the_worker_cannot_parse() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let r = enqueue::<Email>(
        &mut conn,
        key("deep"),
        too_deep(),
        EnqueueOptions::default(),
    )
    .await;
    assert!(matches!(r, Err(SchedulerError::Serde(_))), "{r:?}");
    assert_eq!(db.scalar("SELECT count(*) AS n FROM queue_tasks").await, 0);
    db.cleanup().await;
}

/// NaN serializes as `null`, which an `f64` cannot decode: the worker would dead-letter it.
#[tokio::test]
async fn enqueue_rejects_args_that_do_not_round_trip() {
    #[derive(serde::Serialize, serde::Deserialize)]
    struct Reading {
        value: f64,
    }
    struct Measure;
    impl Task for Measure {
        const NAME: &'static str = "measure";
        type Args = Reading;
    }
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let r = enqueue::<Measure>(
        &mut conn,
        key("nan"),
        Reading { value: f64::NAN },
        EnqueueOptions::default(),
    )
    .await;
    assert!(matches!(r, Err(SchedulerError::Serde(_))), "{r:?}");
    assert_eq!(db.scalar("SELECT count(*) AS n FROM queue_tasks").await, 0);
    db.cleanup().await;
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
#[serde(untagged)]
enum Amount {
    Count(u64),
    Ratio(f64),
}
struct Measure;
impl Task for Measure {
    const NAME: &'static str = "measure";
    type Args = Amount;
}

/// JSONB stores `1e18` as `1000000000000000000`, which the untagged enum decodes as `Count`.
#[tokio::test]
async fn enqueue_rejects_args_whose_meaning_changes_in_jsonb() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let r = enqueue::<Measure>(
        &mut conn,
        key("ratio"),
        Amount::Ratio(1e18),
        EnqueueOptions::default(),
    )
    .await;
    assert!(
        matches!(r, Err(SchedulerError::PayloadNotPreserved)),
        "{r:?}"
    );
    assert_eq!(db.scalar("SELECT count(*) AS n FROM queue_tasks").await, 0);
    db.cleanup().await;
}

#[tokio::test]
async fn accepted_args_decode_equal_after_storage() {
    use std::num::NonZeroU32;

    use pg_task_scheduler::TaskName;
    use pg_task_scheduler::WorkerId;

    #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
    struct Big {
        x: f64,
    }
    struct Gauge;
    impl Task for Gauge {
        const NAME: &'static str = "gauge";
        type Args = Big;
    }
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    enqueue::<Gauge>(
        &mut conn,
        key("big"),
        Big { x: 1e18 },
        EnqueueOptions::default(),
    )
    .await
    .unwrap();
    enqueue::<Measure>(
        &mut conn,
        key("count"),
        Amount::Count(5),
        EnqueueOptions::default(),
    )
    .await
    .unwrap();
    let mut claimed = store::claim(
        &mut conn,
        &WorkerId::generate(),
        &QueueName::default(),
        &[
            TaskName::try_from("gauge").unwrap(),
            TaskName::try_from("measure").unwrap(),
        ],
        NonZeroU32::new(2).unwrap(),
        common::lease(Duration::from_secs(30)),
    )
    .await
    .unwrap()
    .ready;
    claimed.sort_by(|a, b| a.task_name.as_str().cmp(b.task_name.as_str()));
    assert_eq!(claimed.len(), 2);
    assert_eq!(
        serde_json::from_value::<Big>(claimed[0].payload.clone()).unwrap(),
        Big { x: 1e18 }
    );
    assert_eq!(
        serde_json::from_value::<Amount>(claimed[1].payload.clone()).unwrap(),
        Amount::Count(5)
    );
    db.cleanup().await;
}

/// A fresh `HashSet` serializes in another order, so the check must not re-serialize JSON that
/// JSONB left unchanged.
#[tokio::test]
async fn enqueue_accepts_args_that_reserialize_in_another_order() {
    use std::collections::HashSet;

    struct Tags;
    impl Task for Tags {
        const NAME: &'static str = "tags";
        type Args = HashSet<String>;
    }
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let tags: HashSet<String> = (0..20).map(|i| format!("t{i}")).collect();
    let r = enqueue::<Tags>(&mut conn, key("tags"), tags, EnqueueOptions::default()).await;
    assert!(r.is_ok(), "{r:?}");
    db.cleanup().await;
}

#[tokio::test]
async fn enqueue_survives_cleanup_deleting_the_incumbent() {
    use std::num::NonZeroU32;

    use pg_task_scheduler::Retention;
    use pg_task_scheduler::TaskName;
    use pg_task_scheduler::WorkerId;

    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    enqueue::<Email>(
        &mut conn,
        key("k"),
        json!({"a": 1}),
        EnqueueOptions::immediate(),
    )
    .await
    .unwrap();
    let claimed = store::claim(
        &mut conn,
        &WorkerId::generate(),
        &QueueName::default(),
        &[TaskName::try_from("email").unwrap()],
        NonZeroU32::new(1).unwrap(),
        common::lease(Duration::from_secs(30)),
    )
    .await
    .unwrap()
    .ready;
    store::complete(&mut conn, &claimed[0].lease).await.unwrap();

    // A statement-level trigger fires even when ON CONFLICT DO NOTHING inserts nothing, so
    // the second enqueue parks between its conflict and its read-back.
    db.install(
        "CREATE FUNCTION barrier() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN \
         PERFORM pg_advisory_lock(4242); PERFORM pg_advisory_unlock(4242); RETURN NULL; END $$; \
         CREATE TRIGGER barrier AFTER INSERT ON queue_tasks \
         FOR EACH STATEMENT EXECUTE FUNCTION barrier();",
    )
    .await;
    let mut holder = db.pool.get().await.unwrap();
    holder
        .batch_execute("SELECT pg_advisory_lock(4242)")
        .await
        .unwrap();

    let mut enq_conn = db.pool.get().await.unwrap();
    let pid = common::backend_pid(&mut enq_conn).await;
    let handle = tokio::spawn(async move {
        enqueue::<Email>(
            &mut enq_conn,
            key("k"),
            json!({"a": 1}),
            EnqueueOptions::immediate(),
        )
        .await
    });
    db.wait_until_lock_blocked(pid).await;

    let summary = store::cleanup(
        &mut conn,
        &Retention {
            done: Duration::ZERO,
            ..Retention::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(summary.tasks, 1);
    holder
        .batch_execute("SELECT pg_advisory_unlock(4242)")
        .await
        .unwrap();

    let e = handle.await.unwrap().unwrap();
    assert!(e.inserted);
    assert_eq!(
        db.scalar(
            "SELECT count(*) AS n FROM queue_tasks WHERE task_key = 'k' AND status = 'queued'"
        )
        .await,
        1
    );
    db.cleanup().await;
}
