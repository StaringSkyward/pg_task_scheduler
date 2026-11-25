mod common;

use std::num::NonZeroU32;
use std::time::Duration;

use common::TestDb;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use pg_task_scheduler::AttemptNo;
use pg_task_scheduler::AttemptOutcome;
use pg_task_scheduler::ClaimedTask;
use pg_task_scheduler::EnqueueOptions;
use pg_task_scheduler::LockToken;
use pg_task_scheduler::MaxAttempts;
use pg_task_scheduler::ReapSummary;
use pg_task_scheduler::Retention;
use pg_task_scheduler::SchedulerError;
use pg_task_scheduler::Task;
use pg_task_scheduler::TaskKey;
use pg_task_scheduler::TaskStatus;
use pg_task_scheduler::WorkerId;
use pg_task_scheduler::enqueue;
use pg_task_scheduler::replay;
use pg_task_scheduler::store;
use serde_json::json;

struct Email;
impl Task for Email {
    const NAME: &'static str = "email";
    type Args = serde_json::Value;
}

fn nz(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).unwrap()
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
        &[pg_task_scheduler::TaskName::try_from("email").unwrap()],
        nz(1),
        common::lease(lease),
    )
    .await
    .unwrap()
    .ready
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
    assert_eq!(t.state.status(), TaskStatus::Queued);
    assert_eq!(t.last_error.as_deref(), Some("worker lease expired"));
    assert_eq!(t.lock_token, c.lease.lock_token);
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
    assert_eq!(common::outcome(&a[0]), Some(AttemptOutcome::LeaseExpired));
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
    assert_eq!(t.state.status(), TaskStatus::Dead); // Finished carries completed_at
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
            // The lease expires naturally; an UPDATE from B would block on our row lock. Two
            // statements, each within the lease left that `lock_owned` set as their timeout,
            // and no idle gap, so the bounds do not end the transaction.
            let b = &mut conn_b;
            let (slept, reaped) = tokio::join!(
                async {
                    diesel::sql_query("SELECT pg_sleep(0.6)").execute(a).await?;
                    diesel::sql_query("SELECT pg_sleep(0.6)").execute(a).await
                },
                async {
                    tokio::time::sleep(Duration::from_millis(1100)).await;
                    store::reap_expired(b, nz(100), delay).await
                }
            );
            slept?;
            while_locked = Some(reaped);
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

/// Reaper A's statement takes its snapshot, then a gate holds it before it locks a row. While
/// it waits, B's transaction reaps the task, reclaims it (token 2, 1 µs lease) and commits. A
/// then locks token 2's version, whose lease ended before A's statement began, and reaps it.
/// Token 2's attempt was inserted after A's snapshot: only an upsert can find and close it.
/// B reaps and reclaims before A starts so that token 2's lease is already expired at A's
/// statement timestamp; reclaiming after A started, A's cutoff would skip the row instead.
#[tokio::test]
async fn a_reaper_resumed_after_a_reclaim_closes_the_new_attempt() {
    let db = TestDb::new().await;
    let mut h = db.pool.get().await.unwrap();
    let mut a = db.pool.get().await.unwrap();
    let mut b = db.pool.get().await.unwrap();
    // A statement-level BEFORE trigger fires when REAP's `reaped` starts: after the statement's
    // snapshot, before `expired` locks any row.
    h.batch_execute(
        "CREATE FUNCTION reap_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN \
           IF current_setting('pgts.reap_gate', true) = 'on' THEN \
             PERFORM pg_advisory_lock(5757); PERFORM pg_advisory_unlock(5757); \
           END IF; \
           RETURN NULL; \
         END $$; \
         CREATE TRIGGER reap_gate BEFORE UPDATE ON queue_tasks \
           FOR EACH STATEMENT EXECUTE FUNCTION reap_gate(); \
         SELECT pg_advisory_lock(5757)",
    )
    .await
    .unwrap();
    enqueue_key(&mut b, "k", 3).await;
    let first = claim_lease(&mut b, Duration::from_micros(1)).await.unwrap();
    b.batch_execute("BEGIN").await.unwrap();
    let s = store::reap_expired(&mut b, nz(100), Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(s.requeued, 1);
    let second = claim_lease(&mut b, Duration::from_micros(1)).await.unwrap();
    assert_eq!(
        second.lease.lock_token,
        LockToken(first.lease.lock_token.0 + 1)
    );

    let pid = common::backend_pid(&mut a).await;
    a.batch_execute("SET pgts.reap_gate = 'on'").await.unwrap();
    let gated = async {
        let waiting = format!(
            "SELECT count(*) AS n FROM pg_locks \
             WHERE pid = {pid} AND locktype = 'advisory' AND NOT granted"
        );
        let mut polls = 0;
        while db.scalar(&waiting).await == 0 {
            polls += 1;
            assert!(polls < 250, "reaper A never waited at the gate");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        b.batch_execute("COMMIT").await.unwrap();
        h.batch_execute("SELECT pg_advisory_unlock(5757)")
            .await
            .unwrap();
    };
    let (reaped, ()) = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(store::reap_expired(&mut a, nz(100), Duration::ZERO), gated)
    })
    .await
    .expect("test must not hang");

    let open = db
        .scalar(
            "SELECT count(*) AS n FROM queue_task_attempts a \
             JOIN queue_tasks t ON t.id = a.task_id \
             WHERE a.finished_at IS NULL AND t.status <> 'running'",
        )
        .await;
    assert_eq!(
        open, 0,
        "an attempt is open under a task that is not running"
    );
    // A reaped token 2: the interleaving happened.
    assert_eq!(
        reaped.unwrap(),
        ReapSummary {
            requeued: 1,
            dead: 0
        }
    );
    h.batch_execute("DROP TRIGGER reap_gate ON queue_tasks")
        .await
        .unwrap();
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
        "INSERT INTO queue_tasks (task_key, task_name, status, attempts, locked_by, locked_until) \
         VALUES ('running', 'email', 'running', 1, 'w', now() + interval '1 hour')",
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
    for expected in [3, 1, 0] {
        let s = store::cleanup(&mut conn, &r).await.unwrap();
        assert_eq!(s.tasks(), expected);
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
async fn cleanup_serves_every_terminal_status_each_call() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    for i in 0..25 {
        insert_terminal(&mut conn, &format!("done-{i}"), "done", "1 day").await;
    }
    for i in 0..3 {
        insert_terminal(&mut conn, &format!("dead-{i}"), "dead", "1 day").await;
        insert_terminal(&mut conn, &format!("cancelled-{i}"), "cancelled", "1 day").await;
    }
    let r = Retention {
        done: Duration::from_secs(1),
        dead: Duration::from_secs(1),
        outbox_published: Duration::from_secs(1),
        batch_size: nz(10),
    };
    let s = store::cleanup(&mut conn, &r).await.unwrap();
    assert_eq!((s.done, s.dead, s.cancelled), (10, 3, 3));
    db.cleanup().await;
}

#[tokio::test]
async fn cleanup_rejects_retention_it_cannot_apply_before_deleting() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    insert_terminal(&mut conn, "done-old", "done", "31 days").await;
    let r = Retention {
        done: Duration::ZERO,
        dead: Duration::from_micros(u64::try_from(i64::MAX).unwrap()),
        ..Default::default()
    };
    let err = store::cleanup(&mut conn, &r).await.unwrap_err();
    assert!(matches!(err, SchedulerError::Config(_)), "{err:?}");
    assert_eq!(db.scalar("SELECT count(*) AS n FROM queue_tasks").await, 1);
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
    assert_eq!((s.tasks(), s.outbox), (0, 1));
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
    assert_eq!(t.state.status(), TaskStatus::Queued);
    assert_eq!(t.attempts, 0);
    assert_eq!(t.lock_token, LockToken(c.lease.lock_token.0 + 1));
    assert_eq!(t.last_error, None);
    let n = db
        .scalar(&format!(
            "SELECT count(*) AS n FROM queue_admin_actions a \
             JOIN queue_admin_action_tasks m ON m.action_id = a.id \
             WHERE a.action = 'replay' AND m.task_id = {} AND a.max_task_id IS NULL \
             AND operator = 'ops@example' AND reason = 'fixed config' \
             AND task_name = 'email' AND queue = 'default'",
            c.lease.task_id.0,
        ))
        .await;
    assert_eq!(n, 1);
    assert_eq!(
        db.scalar("SELECT count(*) AS n FROM queue_admin_actions WHERE action = 'replay'")
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
        db.scalar("SELECT count(*) AS n FROM queue_admin_actions WHERE action = 'replay'")
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
    assert_eq!(again.lease.attempt_no, AttemptNo::FIRST);
    assert_eq!(again.lease.lock_token, LockToken(3));
    let a = store::attempts(&mut conn, c.lease.task_id).await.unwrap();
    assert_eq!(a.len(), 2);
    assert!(a.iter().all(|x| x.attempt_no == AttemptNo::FIRST));
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
        "INSERT INTO queue_tasks (task_key, task_name, status, attempts, locked_by, locked_until) \
         VALUES ('r', 'email', 'running', 1, 'w', now() + interval '1 hour')",
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

#[tokio::test]
async fn cleanup_deletes_expired_cancelled_tasks() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    insert_terminal(&mut conn, "cancelled-31", "cancelled", "31 days").await;
    insert_terminal(&mut conn, "cancelled-1", "cancelled", "1 day").await;
    let s = store::cleanup(&mut conn, &Retention::default())
        .await
        .unwrap();
    assert_eq!(s.tasks(), 1);
    assert_eq!(
        db.scalar("SELECT count(*) AS n FROM queue_tasks WHERE task_key = 'cancelled-1'")
            .await,
        1
    );
    assert_eq!(db.scalar("SELECT count(*) AS n FROM queue_tasks").await, 1);
    db.cleanup().await;
}

#[tokio::test]
async fn cancelled_rows_obey_terminal_check() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let err = diesel::sql_query(
        "INSERT INTO queue_tasks (task_key, task_name, status) VALUES ('c', 'email', 'cancelled')",
    )
    .execute(&mut conn)
    .await
    .unwrap_err();
    assert!(
        matches!(
            err,
            diesel::result::Error::DatabaseError(
                diesel::result::DatabaseErrorKind::CheckViolation,
                _
            )
        ),
        "{err:?}"
    );
    db.cleanup().await;
}
