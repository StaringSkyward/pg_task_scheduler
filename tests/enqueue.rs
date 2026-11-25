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
async fn a_due_time_rounds_up_to_the_microsecond() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let given: chrono::DateTime<Utc> = "2030-01-01T00:00:00.000000999Z".parse().unwrap();
    let want: chrono::DateTime<Utc> = "2030-01-01T00:00:00.000001Z".parse().unwrap();
    let e = enqueue::<Email>(
        &mut conn,
        key("round"),
        json!({}),
        EnqueueOptions::at(given),
    )
    .await
    .unwrap();
    assert_eq!(e.due_at, want);
    let stored = db
        .scalar(
            "SELECT count(*) AS n FROM queue_tasks WHERE due_at = '2030-01-01T00:00:00.000001Z'",
        )
        .await;
    assert_eq!(stored, 1);
    db.cleanup().await;
}

/// A leap-second instant (`:60.f`) is stored as the same fraction of the following second.
#[tokio::test]
async fn a_leap_second_due_time_is_stored_as_the_next_second() {
    use chrono::NaiveDate;
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let leap = NaiveDate::from_ymd_opt(2016, 12, 31)
        .unwrap()
        .and_hms_milli_opt(23, 59, 59, 1250)
        .unwrap()
        .and_utc();
    let want: chrono::DateTime<Utc> = "2017-01-01T00:00:00.250Z".parse().unwrap();
    let e = enqueue::<Email>(&mut conn, key("leap"), json!({}), EnqueueOptions::at(leap))
        .await
        .unwrap();
    assert_eq!(e.due_at, want);
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
async fn a_failed_wakeup_inserts_nothing() {
    let db = TestDb::new().await;
    let mut conn = db.conn_with_failing_notify().await;
    let r = enqueue::<Email>(&mut conn, key("k"), json!({}), EnqueueOptions::immediate()).await;
    assert!(matches!(r, Err(SchedulerError::Database(_))), "{r:?}");
    assert_eq!(db.scalar("SELECT count(*) AS n FROM queue_tasks").await, 0);
    db.cleanup().await;
}

#[tokio::test]
async fn a_rolled_back_enqueue_sends_no_wakeup() {
    let db = TestDb::new().await;
    let schema = &db.schema;
    let mut listener = AsyncPgConnection::establish(&common::database_url())
        .await
        .unwrap();
    listener
        .batch_execute("LISTEN queue_available")
        .await
        .unwrap();
    // Notifications are database-wide and parallel tests send them too, so this schema's
    // `pg_notify` tags its wakeups with the schema name.
    let mut conn = AsyncPgConnection::establish(&common::database_url())
        .await
        .unwrap();
    conn.batch_execute(&format!(
        "CREATE FUNCTION \"{schema}\".pg_notify(text, text) RETURNS void LANGUAGE sql \
         AS $$ SELECT pg_catalog.pg_notify($1, '{schema}') $$; \
         SET search_path TO \"{schema}\", pg_catalog"
    ))
    .await
    .unwrap();
    let r: Result<(), SchedulerError> = conn
        .transaction(async |conn| {
            enqueue::<Email>(conn, key("k"), json!({}), EnqueueOptions::immediate()).await?;
            Err(SchedulerError::Contention("roll back"))
        })
        .await;
    assert!(r.is_err());
    let mut stream = std::pin::pin!(listener.notifications_stream());
    let mine = tokio::time::timeout(Duration::from_millis(300), async {
        while let Some(n) = stream.next().await {
            if n.unwrap().payload == *schema {
                return true;
            }
        }
        false
    })
    .await;
    assert!(mine.is_err(), "a rolled-back enqueue notified");
    assert_eq!(db.scalar("SELECT count(*) AS n FROM queue_tasks").await, 0);
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
    #[derive(PartialEq, serde::Serialize, serde::Deserialize)]
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

/// JSONB equality says `1 = 1.0`, but the handler decodes `Count(1)` and `Ratio(1.0)` apart.
#[tokio::test]
async fn idempotency_distinguishes_payloads_that_decode_differently() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let k = || key("same");
    enqueue::<Measure>(&mut conn, k(), Amount::Count(1), EnqueueOptions::default())
        .await
        .unwrap();
    let r = enqueue::<Measure>(
        &mut conn,
        k(),
        Amount::Ratio(1.0),
        EnqueueOptions::default(),
    )
    .await;
    assert!(
        matches!(r, Err(SchedulerError::IdempotencyConflict(_))),
        "{r:?}"
    );
    db.cleanup().await;
}

#[tokio::test]
async fn idempotency_ignores_payload_key_order() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let first = enqueue::<Email>(
        &mut conn,
        key("order"),
        json!({"a": 1, "b": 2}),
        EnqueueOptions::default(),
    )
    .await
    .unwrap();
    assert!(first.inserted);
    let second = enqueue::<Email>(
        &mut conn,
        key("order"),
        json!({"b": 2, "a": 1}),
        EnqueueOptions::default(),
    )
    .await
    .unwrap();
    assert!(!second.inserted);
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

/// serde_json's default parser reads `0.10000000000000005` as `0.10000000000000003`, one ULP off,
/// so the claimed argument would differ from the enqueued one and idempotency would treat the
/// adjacent value as equal.
#[tokio::test]
async fn enqueue_preserves_f64_bits_and_rejects_the_adjacent_value() {
    use std::num::NonZeroU32;

    use pg_task_scheduler::TaskName;
    use pg_task_scheduler::WorkerId;

    struct Level;
    impl Task for Level {
        const NAME: &'static str = "level";
        type Args = f64;
    }
    let a = f64::from_bits(0x3fb9_9999_9999_999d);
    let b = f64::from_bits(0x3fb9_9999_9999_999c);
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    enqueue::<Level>(&mut conn, key("float"), a, EnqueueOptions::default())
        .await
        .unwrap();
    let claimed = store::claim(
        &mut conn,
        &WorkerId::generate(),
        &QueueName::default(),
        &[TaskName::try_from("level").unwrap()],
        NonZeroU32::new(1).unwrap(),
        common::lease(Duration::from_secs(30)),
    )
    .await
    .unwrap()
    .ready;
    assert_eq!(
        serde_json::from_value::<f64>(claimed[0].payload.clone())
            .unwrap()
            .to_bits(),
        a.to_bits()
    );
    let err = enqueue::<Level>(&mut conn, key("float"), b, EnqueueOptions::default())
        .await
        .unwrap_err();
    assert!(
        matches!(err, SchedulerError::IdempotencyConflict(_)),
        "{err:?}"
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
    assert_eq!(summary.tasks(), 1);
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

/// serde_json writes a non-finite `f64` as `null`, which decodes as `None`.
#[tokio::test]
async fn enqueue_rejects_non_finite_floats_inside_an_option() {
    struct Reading;
    impl Task for Reading {
        const NAME: &'static str = "reading";
        type Args = Option<f64>;
    }
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    for (k, v) in [("inf", f64::INFINITY), ("nan", f64::NAN)] {
        let r = enqueue::<Reading>(&mut conn, key(k), Some(v), EnqueueOptions::default()).await;
        assert!(
            matches!(r, Err(SchedulerError::PayloadNotPreserved)),
            "{r:?}"
        );
    }
    assert_eq!(db.scalar("SELECT count(*) AS n FROM queue_tasks").await, 0);
    db.cleanup().await;
}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Mixed {
    tags: std::collections::HashSet<String>,
    x: f64,
}
struct MixedTask;
impl Task for MixedTask {
    const NAME: &'static str = "mixed";
    type Args = Mixed;
}

/// A `HashSet` re-serializes in another order and `1e18` changes form in JSONB; comparing
/// decoded values accepts both together.
#[tokio::test]
async fn enqueue_accepts_a_set_beside_a_large_float() {
    use std::num::NonZeroU32;

    use pg_task_scheduler::TaskName;
    use pg_task_scheduler::WorkerId;

    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let make = || Mixed {
        tags: (0..8).map(|i| format!("t{i}")).collect(),
        x: 1e18,
    };
    enqueue::<MixedTask>(&mut conn, key("mixed"), make(), EnqueueOptions::default())
        .await
        .unwrap();
    let claimed = store::claim(
        &mut conn,
        &WorkerId::generate(),
        &QueueName::default(),
        &[TaskName::try_from("mixed").unwrap()],
        NonZeroU32::new(1).unwrap(),
        common::lease(Duration::from_secs(30)),
    )
    .await
    .unwrap()
    .ready;
    let got: Mixed = serde_json::from_value(claimed[0].payload.clone()).unwrap();
    assert_eq!(got, make());
    db.cleanup().await;
}

/// Equal sets match whatever order they serialize in.
#[tokio::test]
async fn idempotency_matches_equal_sets_in_another_order() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let tags: Vec<String> = (0..20).map(|i| format!("t{i}")).collect();
    let build = || Mixed {
        tags: tags.iter().cloned().collect(),
        x: 1.0,
    };
    let a = build();
    let a_json = serde_json::to_string(&a).unwrap();
    let mut b = build();
    let mut differed = false;
    for _ in 0..100 {
        b = build();
        if serde_json::to_string(&b).unwrap() != a_json {
            differed = true;
            break;
        }
    }
    assert!(differed, "no rebuilt set serialized in another order");
    let first = enqueue::<MixedTask>(&mut conn, key("set"), a, EnqueueOptions::default())
        .await
        .unwrap();
    let second = enqueue::<MixedTask>(&mut conn, key("set"), b, EnqueueOptions::default())
        .await
        .unwrap();
    assert!(!second.inserted);
    assert_eq!(second.id, first.id);
    db.cleanup().await;
}

/// A stored payload the retry's type cannot decode is a conflict, not an error or a match.
#[tokio::test]
async fn idempotency_conflicts_when_the_stored_payload_does_not_decode() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    conn.batch_execute(
        "INSERT INTO queue_tasks (task_key, task_name, payload) \
         VALUES ('bad', 'mixed', '\"not an object\"')",
    )
    .await
    .unwrap();
    let r = enqueue::<MixedTask>(
        &mut conn,
        key("bad"),
        Mixed {
            tags: Default::default(),
            x: 1.0,
        },
        EnqueueOptions::default(),
    )
    .await;
    assert!(
        matches!(r, Err(SchedulerError::IdempotencyConflict(_))),
        "{r:?}"
    );
    db.cleanup().await;
}

/// `Value` equality tells an integer from a float, so `1` then `1.0` conflict.
#[tokio::test]
async fn idempotency_with_value_args_keeps_numeric_scale() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    enqueue::<Email>(
        &mut conn,
        key("n"),
        json!({"n": 1}),
        EnqueueOptions::default(),
    )
    .await
    .unwrap();
    let r = enqueue::<Email>(
        &mut conn,
        key("n"),
        json!({"n": 1.0}),
        EnqueueOptions::default(),
    )
    .await;
    assert!(
        matches!(r, Err(SchedulerError::IdempotencyConflict(_))),
        "{r:?}"
    );
    db.cleanup().await;
}
