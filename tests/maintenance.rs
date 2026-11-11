mod common;

use std::num::NonZeroUsize;
use std::time::Duration;

use common::TestDb;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use pg_task_scheduler::store;
use pg_task_scheduler::{
    AttemptOutcome, ClaimedTask, EnqueueOptions, LockToken, MaxAttempts, ReapSummary, Retention,
    SchedulerError, Task, TaskKey, TaskStatus, WorkerId, enqueue, replay,
};
use serde_json::json;

struct Email;
impl Task for Email {
    const NAME: &'static str = "email";
    type Args = serde_json::Value;
}

fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).unwrap()
}

async fn exec(conn: &mut AsyncPgConnection, sql: &str) {
    diesel::sql_query(sql).execute(conn).await.unwrap();
}

async fn enqueue_key(conn: &mut AsyncPgConnection, key: &str, max_attempts: u32) {
    enqueue::<Email>(
        conn,
        TaskKey::try_from(key).unwrap(),
        json!({}),
        EnqueueOptions::immediate().max_attempts(MaxAttempts::try_from(max_attempts).unwrap()),
    )
    .await
    .unwrap();
}

async fn claim_lease(conn: &mut AsyncPgConnection, lease: Duration) -> Option<ClaimedTask> {
    store::claim(
        conn,
        &WorkerId::generate(),
        &pg_task_scheduler::QueueName::default(),
        &["email".to_owned()],
        nz(1),
        lease,
    )
    .await
    .unwrap()
    .pop()
}

async fn claim_one(conn: &mut AsyncPgConnection) -> Option<ClaimedTask> {
    claim_lease(conn, Duration::from_secs(60)).await
}

/// Enqueue and claim one task with the given `max_attempts`.
async fn claimed(db: &TestDb, max_attempts: u32) -> ClaimedTask {
    let mut conn = db.pool.get().await.unwrap();
    enqueue_key(&mut conn, "k", max_attempts).await;
    claim_one(&mut conn).await.unwrap()
}

#[tokio::test]
async fn reaper_requeues_with_crash_retry_delay() {
    let db = TestDb::new().await;
    let c = claimed(&db, 3).await;
    db.expire_lease(c.lease.task_id).await;
    let mut conn = db.pool.get().await.unwrap();
    let s = store::reap_expired(&mut conn, nz(100), Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        s,
        ReapSummary {
            requeued: 1,
            dead: 0
        }
    );
    let t = store::task(&mut conn, c.lease.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(t.status, TaskStatus::Queued);
    assert_eq!(t.last_error.as_deref(), Some("worker lease expired"));
    assert_eq!(t.lock_token, c.lease.lock_token);
    assert_eq!(t.locked_by, None);
    let ok = db
        .scalar(&format!(
            "SELECT count(*) AS n FROM queue_tasks WHERE id = {} \
             AND due_at >= now() + interval '4 seconds'",
            c.lease.task_id.0
        ))
        .await;
    assert_eq!(ok, 1);
    let a = store::attempts(&mut conn, c.lease.task_id).await.unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].outcome, Some(AttemptOutcome::LeaseExpired));
    assert!(a[0].finished_at.is_some());
    db.cleanup().await;
}

#[tokio::test]
async fn reaper_kills_exhausted_task() {
    let db = TestDb::new().await;
    let c = claimed(&db, 1).await;
    db.expire_lease(c.lease.task_id).await;
    let mut conn = db.pool.get().await.unwrap();
    let s = store::reap_expired(&mut conn, nz(100), Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        s,
        ReapSummary {
            requeued: 0,
            dead: 1
        }
    );
    let t = store::task(&mut conn, c.lease.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(t.status, TaskStatus::Dead);
    assert!(t.completed_at.is_some());
    db.cleanup().await;
}

#[tokio::test]
async fn reaper_ignores_live_leases() {
    let db = TestDb::new().await;
    claimed(&db, 3).await;
    let mut conn = db.pool.get().await.unwrap();
    let s = store::reap_expired(&mut conn, nz(100), Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(s, ReapSummary::default());
    db.cleanup().await;
}

#[tokio::test]
async fn concurrent_reapers_reap_each_task_once() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    for i in 0..10 {
        enqueue_key(&mut conn, &format!("k{i}"), 3).await;
        let c = claim_one(&mut conn).await.unwrap();
        db.expire_lease(c.lease.task_id).await;
    }
    let mut a = db.pool.get().await.unwrap();
    let mut b = db.pool.get().await.unwrap();
    let delay = Duration::from_secs(5);
    let (ra, rb) = tokio::join!(
        store::reap_expired(&mut a, nz(10), delay),
        store::reap_expired(&mut b, nz(10), delay)
    );
    assert_eq!(ra.unwrap().requeued + rb.unwrap().requeued, 10);
    let n = db
        .scalar("SELECT count(*) AS n FROM queue_task_attempts WHERE outcome = 'lease_expired'")
        .await;
    assert_eq!(n, 10);
    db.cleanup().await;
}

#[tokio::test]
async fn reaper_skips_row_locked_by_db_contained_transaction() {
    let db = TestDb::new().await;
    let mut conn_a = db.pool.get().await.unwrap();
    let mut conn_b = db.pool.get().await.unwrap();
    enqueue_key(&mut conn_a, "k", 3).await;
    let c = claim_lease(&mut conn_a, Duration::from_secs(1))
        .await
        .unwrap();
    let delay = Duration::from_secs(5);

    let mut while_locked = None;
    let outcome = tokio::time::timeout(
        Duration::from_secs(20),
        conn_a.transaction::<(), SchedulerError, _>(async |a| {
            store::lock_owned(a, &c.lease).await?;
            // The lease expires naturally; an UPDATE from B would block on our row lock.
            tokio::time::sleep(Duration::from_millis(1200)).await;
            while_locked = Some(store::reap_expired(&mut conn_b, nz(100), delay).await);
            // Ownership is gone; the error rolls the transaction back.
            store::complete(a, &c.lease).await
        }),
    )
    .await
    .expect("test must not hang");

    assert_eq!(while_locked.unwrap().unwrap(), ReapSummary::default());
    match outcome {
        Err(SchedulerError::LostOwnership(id)) => assert_eq!(id, c.lease.task_id),
        other => panic!("expected LostOwnership, got {other:?}"),
    }
    let s = store::reap_expired(&mut conn_b, nz(100), delay)
        .await
        .unwrap();
    assert_eq!(
        s,
        ReapSummary {
            requeued: 1,
            dead: 0
        }
    );
    db.cleanup().await;
}

/// Insert a terminal row `age` old.
async fn insert_terminal(conn: &mut AsyncPgConnection, key: &str, status: &str, age: &str) {
    exec(
        conn,
        &format!(
            "INSERT INTO queue_tasks (task_key, task_name, status, completed_at) \
             VALUES ('{key}', 'email', '{status}', now() - interval '{age}')"
        ),
    )
    .await;
}

#[tokio::test]
async fn cleanup_deletes_only_eligible_terminal_rows_in_batches() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    for i in 0..3 {
        insert_terminal(&mut conn, &format!("done-old-{i}"), "done", "31 days").await;
    }
    insert_terminal(&mut conn, "done-new", "done", "1 day").await;
    insert_terminal(&mut conn, "dead-8", "dead", "8 days").await;
    insert_terminal(&mut conn, "dead-31", "dead", "31 days").await;
    exec(
        &mut conn,
        "INSERT INTO queue_tasks (task_key, task_name) VALUES ('queued', 'email')",
    )
    .await;
    exec(
        &mut conn,
        "INSERT INTO queue_tasks (task_key, task_name, status, locked_by, locked_until) \
         VALUES ('running', 'email', 'running', 'w', now() + interval '1 hour')",
    )
    .await;
    // Attempt rows of a doomed task must cascade away.
    exec(
        &mut conn,
        "INSERT INTO queue_task_attempts (task_id, attempt_no, lock_token, worker_id) \
         SELECT id, 1, 1, 'w' FROM queue_tasks WHERE task_key IN ('done-old-0', 'done-new')",
    )
    .await;

    let r = Retention {
        batch_size: nz(2),
        ..Default::default()
    };
    for expected in [2, 2, 0] {
        let s = store::cleanup(&mut conn, &r).await.unwrap();
        assert_eq!(s.tasks, expected);
    }
    let left = db
        .scalar(
            "SELECT count(*) AS n FROM queue_tasks \
             WHERE task_key IN ('done-new', 'dead-8', 'queued', 'running')",
        )
        .await;
    assert_eq!(left, 4);
    assert_eq!(db.scalar("SELECT count(*) AS n FROM queue_tasks").await, 4);
    assert_eq!(
        db.scalar("SELECT count(*) AS n FROM queue_task_attempts")
            .await,
        1
    );
    db.cleanup().await;
}

#[tokio::test]
async fn cleanup_deletes_published_outbox_rows_only() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    exec(
        &mut conn,
        "INSERT INTO queue_outbox (task_key, effect_name, command_key, topic, payload, \
         created_at, published_at) VALUES \
         ('a', 'e', 'a', 't', '{}', now() - interval '9 days', now() - interval '8 days'), \
         ('b', 'e', 'b', 't', '{}', now() - interval '9 days', NULL)",
    )
    .await;
    let s = store::cleanup(&mut conn, &Retention::default())
        .await
        .unwrap();
    assert_eq!((s.tasks, s.outbox), (0, 1));
    assert_eq!(db.scalar("SELECT count(*) AS n FROM queue_outbox").await, 1);
    db.cleanup().await;
}

#[tokio::test]
async fn replay_requeues_dead_task_and_audits() {
    let db = TestDb::new().await;
    let c = claimed(&db, 3).await;
    let mut conn = db.pool.get().await.unwrap();
    store::fail_permanently(&mut conn, &c.lease, "boom")
        .await
        .unwrap();
    assert!(
        replay(&mut conn, c.lease.task_id, "ops@example", "fixed config")
            .await
            .unwrap()
    );
    let t = store::task(&mut conn, c.lease.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(t.status, TaskStatus::Queued);
    assert_eq!(t.attempts, 0);
    assert_eq!(t.lock_token, LockToken(c.lease.lock_token.0 + 1));
    assert_eq!(t.last_error, None);
    assert_eq!(t.completed_at, None);
    let n = db
        .scalar(&format!(
            "SELECT count(*) AS n FROM queue_task_replays WHERE task_id = {} \
             AND operator = 'ops@example' AND reason = 'fixed config' AND lock_token = {} \
             AND task_key = 'k'",
            c.lease.task_id.0, t.lock_token.0
        ))
        .await;
    assert_eq!(n, 1);
    assert_eq!(
        db.scalar("SELECT count(*) AS n FROM queue_task_replays")
            .await,
        1
    );
    db.cleanup().await;
}

#[tokio::test]
async fn replay_refuses_non_dead_task() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    enqueue_key(&mut conn, "k", 3).await;
    let id = claim_one(&mut conn).await.unwrap().lease.task_id; // running
    assert!(!replay(&mut conn, id, "op", "r").await.unwrap());
    assert!(
        !replay(&mut conn, pg_task_scheduler::TaskId(999_999), "op", "r")
            .await
            .unwrap()
    );
    assert_eq!(
        db.scalar("SELECT count(*) AS n FROM queue_task_replays")
            .await,
        0
    );
    db.cleanup().await;
}

#[tokio::test]
async fn replayed_task_is_claimable_and_records_new_attempt() {
    let db = TestDb::new().await;
    let c = claimed(&db, 1).await;
    assert_eq!(c.lease.lock_token, LockToken(1));
    let mut conn = db.pool.get().await.unwrap();
    store::fail_permanently(&mut conn, &c.lease, "boom")
        .await
        .unwrap();
    assert!(replay(&mut conn, c.lease.task_id, "op", "r").await.unwrap());
    let again = claim_one(&mut conn).await.unwrap();
    assert_eq!(again.lease.attempt_no, 1);
    assert_eq!(again.lease.lock_token, LockToken(3));
    let a = store::attempts(&mut conn, c.lease.task_id).await.unwrap();
    assert_eq!(a.len(), 2);
    assert!(a.iter().all(|x| x.attempt_no == 1));
    db.cleanup().await;
}

#[tokio::test]
async fn queue_stats_reports_counts_depth_and_outbox() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    exec(
        &mut conn,
        "INSERT INTO queue_tasks (task_key, task_name, due_at) VALUES \
         ('q1', 'email', now() - interval '1 minute'), \
         ('q2', 'email', now() + interval '1 hour')",
    )
    .await;
    exec(
        &mut conn,
        "INSERT INTO queue_tasks (task_key, task_name, status, locked_by, locked_until) \
         VALUES ('r', 'email', 'running', 'w', now() + interval '1 hour')",
    )
    .await;
    insert_terminal(&mut conn, "d", "done", "1 day").await;
    insert_terminal(&mut conn, "x", "dead", "1 day").await;
    exec(
        &mut conn,
        "INSERT INTO queue_outbox (task_key, effect_name, command_key, topic, payload, created_at) \
         VALUES ('o', 'e', 'o', 't', '{}', now() - interval '1 minute')",
    )
    .await;

    let s = store::queue_stats(&mut conn).await.unwrap();
    assert_eq!(
        (s.queued, s.running, s.done, s.dead, s.due),
        (2, 1, 1, 1, 1)
    );
    assert_eq!(s.depth.len(), 1);
    let d = &s.depth[0];
    assert_eq!(d.queue, pg_task_scheduler::QueueName::default());
    assert_eq!(d.task_name.as_str(), "email");
    assert_eq!((d.priority, d.queued, d.due), (0, 2, 1));
    assert!(d.oldest_due_age.is_some());
    assert_eq!(s.outbox_unpublished, 1);
    assert!(s.oldest_due_age.is_some());
    assert!(s.oldest_unpublished_age.is_some());
    db.cleanup().await;
}
