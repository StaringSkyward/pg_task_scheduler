mod common;

use std::num::NonZeroU32;
use std::time::Duration;

use common::TestDb;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use diesel_async::pooled_connection::deadpool::Object;
use pg_task_scheduler::AttemptNo;
use pg_task_scheduler::AttemptOutcome;
use pg_task_scheduler::CancellationToken;
use pg_task_scheduler::ClaimedTask;
use pg_task_scheduler::EnqueueOptions;
use pg_task_scheduler::LockToken;
use pg_task_scheduler::MaxAttempts;
use pg_task_scheduler::OutboxCommand;
use pg_task_scheduler::OutboxWrite;
use pg_task_scheduler::RetryOutcome;
use pg_task_scheduler::SchedulerError;
use pg_task_scheduler::Task;
use pg_task_scheduler::TaskKey;
use pg_task_scheduler::TaskLease;
use pg_task_scheduler::TaskState;
use pg_task_scheduler::TaskStatus;
use pg_task_scheduler::TerminalStatus;
use pg_task_scheduler::WorkerId;
use pg_task_scheduler::enqueue;
use pg_task_scheduler::store;
use serde_json::json;

struct Email;
impl Task for Email {
    const NAME: &'static str = "email";
    type Args = serde_json::Value;
}

fn names() -> Vec<pg_task_scheduler::TaskName> {
    vec![pg_task_scheduler::TaskName::try_from("email").unwrap()]
}

async fn claim_one(conn: &mut AsyncPgConnection) -> Option<ClaimedTask> {
    claim_one_with_lease(conn, Duration::from_secs(60)).await
}

async fn claim_one_with_lease(
    conn: &mut AsyncPgConnection,
    lease: Duration,
) -> Option<ClaimedTask> {
    store::claim(
        conn,
        &WorkerId::generate(),
        &pg_task_scheduler::QueueName::default(),
        &names(),
        NonZeroU32::new(1).unwrap(),
        common::lease(lease),
    )
    .await
    .unwrap()
    .ready
    .pop()
}

async fn enqueue_key(conn: &mut AsyncPgConnection, key: &str) {
    enqueue::<Email>(
        conn,
        TaskKey::try_from(key).unwrap(),
        json!({}),
        EnqueueOptions::immediate().max_attempts(MaxAttempts::try_from(2).unwrap()),
    )
    .await
    .unwrap();
}

async fn claimed(db: &TestDb) -> ClaimedTask {
    let mut conn = db.pool.get().await.unwrap();
    enqueue_key(&mut conn, "k").await;
    claim_one(&mut conn).await.unwrap()
}

async fn claimed_with_lease(db: &TestDb, lease: Duration) -> ClaimedTask {
    let mut conn = db.pool.get().await.unwrap();
    enqueue_key(&mut conn, "k").await;
    claim_one_with_lease(&mut conn, lease).await.unwrap()
}

/// Holds the task row with a lock-only transaction until the lease has expired, then releases it
/// without updating the row; the waiting transition must re-check expiry after the wait.
async fn lost_after_lock_wait<F, Fut, T>(op: F)
where
    F: FnOnce(Object<AsyncPgConnection>, TaskLease) -> Fut,
    Fut: std::future::Future<Output = Result<T, SchedulerError>> + Send + 'static,
    T: std::fmt::Debug + Send + 'static,
{
    let db = TestDb::new().await;
    let c = claimed_with_lease(&db, Duration::from_secs(1)).await;
    let mut holder = db.pool.get().await.unwrap();
    holder
        .batch_execute(&format!(
            "BEGIN; SELECT 1 FROM queue_tasks WHERE id = {} FOR UPDATE",
            c.lease.task_id.0
        ))
        .await
        .unwrap();
    let mut conn = db.pool.get().await.unwrap();
    let pid = common::backend_pid(&mut conn).await;
    let lease = c.lease.clone();
    let waiting = tokio::spawn(op(conn, lease.clone()));
    db.wait_until_lock_blocked(pid).await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    holder.batch_execute("ROLLBACK").await.unwrap();
    lost(waiting.await.unwrap(), &lease);
    db.cleanup().await;
}

#[tokio::test]
async fn lock_owned_waiting_on_a_lock_past_expiry_loses_ownership() {
    lost_after_lock_wait(|mut conn, lease| async move {
        conn.transaction(async |t| store::lock_owned(t, &lease).await)
            .await
    })
    .await;
}

#[tokio::test]
async fn complete_waiting_on_a_lock_past_expiry_loses_ownership() {
    lost_after_lock_wait(|mut conn, lease| async move { store::complete(&mut conn, &lease).await })
        .await;
}

#[tokio::test]
async fn heartbeat_waiting_on_a_lock_past_expiry_loses_ownership() {
    lost_after_lock_wait(|mut conn, lease| async move {
        store::heartbeat(&mut conn, &lease, common::lease(Duration::from_secs(60))).await
    })
    .await;
}

fn cmd(effect: &str, key: &str) -> OutboxCommand {
    OutboxCommand {
        effect_name: effect.try_into().unwrap(),
        command_key: key.try_into().unwrap(),
        topic: "t".try_into().unwrap(),
        payload: json!({}),
    }
}

fn lost(r: Result<impl std::fmt::Debug, SchedulerError>, lease: &TaskLease) {
    match r {
        Err(SchedulerError::LostOwnership(id)) => assert_eq!(id, lease.task_id),
        other => panic!("expected LostOwnership, got {other:?}"),
    }
}

#[tokio::test]
async fn complete_marks_done_and_closes_attempt() {
    let db = TestDb::new().await;
    let c = claimed(&db).await;
    let mut conn = db.pool.get().await.unwrap();
    store::complete(&mut conn, &c.lease).await.unwrap();
    let t = store::task(&mut conn, c.lease.task_id)
        .await
        .unwrap()
        .unwrap();
    // Finished carries completed_at and no lock fields.
    assert!(matches!(
        t.state,
        TaskState::Finished {
            status: TerminalStatus::Done,
            ..
        }
    ));
    let a = store::attempts(&mut conn, c.lease.task_id).await.unwrap();
    assert_eq!(common::outcome(&a[0]), Some(AttemptOutcome::Done));
    db.cleanup().await;
}

/// The worker's heartbeat waits at most `lock_wait` for the row lock, so a heartbeat left
/// behind by a dropped keeper cannot outlast the lease waiting.
#[tokio::test]
async fn bounded_heartbeat_gives_up_when_its_lock_wait_exceeds_the_bound() {
    let db = TestDb::new().await;
    let c = claimed(&db).await;
    let mut holder = db.pool.get().await.unwrap();
    holder
        .batch_execute(&format!(
            "BEGIN; SELECT 1 FROM queue_tasks WHERE id = {} FOR UPDATE",
            c.lease.task_id.0
        ))
        .await
        .unwrap();
    let mut conn = db.pool.get().await.unwrap();
    let stop = CancellationToken::new();
    let beat = store::heartbeat_bounded(
        &mut conn,
        &c.lease,
        common::lease(Duration::from_secs(60)),
        Duration::from_millis(100),
        &stop,
    );
    let r = tokio::time::timeout(Duration::from_secs(1), beat)
        .await
        .expect("the heartbeat waited past its lock bound");
    assert!(
        // Both timeouts are `lock_wait`; the statement's starts first, so it usually fires.
        matches!(&r, Err(SchedulerError::Database(e))
            if ["statement timeout", "lock timeout"].iter().any(|t| e.to_string().contains(t))),
        "{r:?}"
    );
    holder.batch_execute("ROLLBACK").await.unwrap();
    db.cleanup().await;
}

/// The heartbeat's budget never raises a stricter timeout the host set on the session.
#[tokio::test]
async fn bounded_heartbeat_respects_a_stricter_session_timeout() {
    let db = TestDb::new().await;
    let c = claimed(&db).await;
    let mut holder = db.pool.get().await.unwrap();
    holder
        .batch_execute(&format!(
            "BEGIN; SELECT 1 FROM queue_tasks WHERE id = {} FOR UPDATE",
            c.lease.task_id.0
        ))
        .await
        .unwrap();
    let mut conn = db.pool.get().await.unwrap();
    conn.batch_execute("SET lock_timeout = '30ms'; SET statement_timeout = '30ms'")
        .await
        .unwrap();
    let stop = CancellationToken::new();
    let beat = store::heartbeat_bounded(
        &mut conn,
        &c.lease,
        common::lease(Duration::from_secs(60)),
        Duration::from_secs(2),
        &stop,
    );
    let r = tokio::time::timeout(Duration::from_millis(500), beat)
        .await
        .expect("the heartbeat waited past the host's stricter timeout");
    assert!(matches!(r, Err(SchedulerError::Database(_))), "{r:?}");
    // Errors if either session value changed.
    conn.batch_execute(
        "DO $$ BEGIN IF current_setting('lock_timeout') <> '30ms' \
         OR current_setting('statement_timeout') <> '30ms' \
         THEN RAISE EXCEPTION 'session timeouts changed'; END IF; END $$",
    )
    .await
    .unwrap();
    holder.batch_execute("ROLLBACK").await.unwrap();
    db.cleanup().await;
}

/// A heartbeat whose dispatch has ended renews nothing.
#[tokio::test]
async fn cancelled_heartbeat_does_not_renew() {
    let db = TestDb::new().await;
    let c = claimed(&db).await;
    let mut conn = db.pool.get().await.unwrap();
    let stop = CancellationToken::new();
    stop.cancel();
    let r = store::heartbeat_bounded(
        &mut conn,
        &c.lease,
        common::lease(Duration::from_secs(120)),
        Duration::from_secs(1),
        &stop,
    )
    .await;
    lost(r, &c.lease);
    let t = store::task(&mut conn, c.lease.task_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(t.state, TaskState::Running { locked_until, .. } if locked_until == c.locked_until),
        "{:?} vs claimed {}",
        t.state,
        c.locked_until
    );
    db.cleanup().await;
}

#[tokio::test]
async fn heartbeat_extends_lease() {
    let db = TestDb::new().await;
    let c = claimed(&db).await;
    let mut conn = db.pool.get().await.unwrap();
    let until = store::heartbeat(&mut conn, &c.lease, common::lease(Duration::from_secs(120)))
        .await
        .unwrap();
    assert!(until > c.locked_until);
    db.cleanup().await;
}

#[tokio::test]
async fn retry_requeues_with_db_time_delay_then_dies() {
    let db = TestDb::new().await;
    let c = claimed(&db).await;
    let mut conn = db.pool.get().await.unwrap();
    let out = store::retry(&mut conn, &c.lease, "boom", Duration::from_secs(2))
        .await
        .unwrap();
    let RetryOutcome::Queued { due_at } = out else {
        panic!("expected Queued, got {out:?}");
    };
    assert!(due_at - c.claimed_at >= chrono::Duration::seconds(2));
    let t = store::task(&mut conn, c.lease.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(t.state.status(), TaskStatus::Queued);
    assert_eq!(t.last_error.as_deref(), Some("boom"));
    let a = store::attempts(&mut conn, c.lease.task_id).await.unwrap();
    assert_eq!(common::outcome(&a[0]), Some(AttemptOutcome::Retry));

    diesel::sql_query("UPDATE queue_tasks SET due_at = now()")
        .execute(&mut conn)
        .await
        .unwrap();
    let c2 = claim_one(&mut conn).await.unwrap();
    assert_eq!(c2.lease.attempt_no, AttemptNo::try_from(2u32).unwrap());
    let out = store::retry(&mut conn, &c2.lease, "boom2", Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(out, RetryOutcome::Dead);
    let t = store::task(&mut conn, c.lease.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(t.state.status(), TaskStatus::Dead); // Finished carries completed_at
    let a = store::attempts(&mut conn, c.lease.task_id).await.unwrap();
    assert_eq!(common::outcome(&a[1]), Some(AttemptOutcome::Dead));
    db.cleanup().await;
}

#[tokio::test]
async fn fail_permanently_marks_dead() {
    let db = TestDb::new().await;
    let c = claimed(&db).await;
    let mut conn = db.pool.get().await.unwrap();
    store::fail_permanently(&mut conn, &c.lease, "bad input")
        .await
        .unwrap();
    let t = store::task(&mut conn, c.lease.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(t.state.status(), TaskStatus::Dead);
    let a = store::attempts(&mut conn, c.lease.task_id).await.unwrap();
    assert_eq!(common::outcome(&a[0]), Some(AttemptOutcome::Dead));
    assert_eq!(a[0].error.as_deref(), Some("bad input"));
    db.cleanup().await;
}

#[tokio::test]
async fn fail_permanently_with_a_nul_is_recorded() {
    let db = TestDb::new().await;
    let c = claimed(&db).await;
    let mut conn = db.pool.get().await.unwrap();
    store::fail_permanently(&mut conn, &c.lease, "bad\0news")
        .await
        .unwrap();
    let t = store::task(&mut conn, c.lease.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(t.state.status(), TaskStatus::Dead);
    assert_eq!(t.last_error.as_deref(), Some("bad\\0news"));
    let a = store::attempts(&mut conn, c.lease.task_id).await.unwrap();
    assert_eq!(common::outcome(&a[0]), Some(AttemptOutcome::Dead));
    db.cleanup().await;
}

#[tokio::test]
async fn retry_with_a_nul_is_recorded() {
    let db = TestDb::new().await;
    let c = claimed(&db).await;
    let mut conn = db.pool.get().await.unwrap();
    store::retry(&mut conn, &c.lease, "bad\0news", Duration::from_secs(2))
        .await
        .unwrap();
    let t = store::task(&mut conn, c.lease.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(t.last_error.as_deref(), Some("bad\\0news"));
    let a = store::attempts(&mut conn, c.lease.task_id).await.unwrap();
    assert_eq!(common::outcome(&a[0]), Some(AttemptOutcome::Retry));
    assert_eq!(a[0].error.as_deref(), Some("bad\\0news"));
    db.cleanup().await;
}

#[tokio::test]
async fn every_transition_fails_after_hard_lease_expiry() {
    let db = TestDb::new().await;
    let c = claimed(&db).await;
    db.expire_lease(c.lease.task_id).await;
    let mut conn = db.pool.get().await.unwrap();
    let l = &c.lease;
    lost(
        store::heartbeat(&mut conn, l, common::lease(Duration::from_secs(60))).await,
        l,
    );
    lost(store::complete(&mut conn, l).await, l);
    lost(
        store::retry(&mut conn, l, "e", Duration::from_secs(1)).await,
        l,
    );
    lost(store::fail_permanently(&mut conn, l, "e").await, l);
    lost(store::lock_owned(&mut conn, l).await, l);
    let t = store::task(&mut conn, l.task_id).await.unwrap().unwrap();
    assert_eq!(t.state.status(), TaskStatus::Running);
    db.cleanup().await;
}

#[tokio::test]
async fn stale_token_cannot_complete_after_reclaim() {
    let db = TestDb::new().await;
    let old = claimed(&db).await;
    db.expire_lease(old.lease.task_id).await;
    let mut conn = db.pool.get().await.unwrap();
    // Reaper stand-in until the reaper exists.
    diesel::sql_query(
        "UPDATE queue_tasks SET status='queued', locked_by=NULL, locked_until=NULL, due_at=now()",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    // Same worker reclaims, so the old lease differs from the new one only by token.
    let new = store::claim(
        &mut conn,
        &old.lease.worker_id,
        &pg_task_scheduler::QueueName::default(),
        &names(),
        NonZeroU32::new(1).unwrap(),
        common::lease(Duration::from_secs(60)),
    )
    .await
    .unwrap()
    .ready
    .pop()
    .unwrap();
    assert_eq!(new.lease.worker_id, old.lease.worker_id);
    assert_eq!(new.lease.lock_token, LockToken(2));
    lost(store::complete(&mut conn, &old.lease).await, &old.lease);
    store::complete(&mut conn, &new.lease).await.unwrap();
    db.cleanup().await;
}

#[tokio::test]
async fn wrong_worker_cannot_complete() {
    let db = TestDb::new().await;
    let c = claimed(&db).await;
    let mut conn = db.pool.get().await.unwrap();
    let imposter = TaskLease {
        worker_id: WorkerId::generate(),
        ..c.lease.clone()
    };
    lost(store::complete(&mut conn, &imposter).await, &imposter);
    db.cleanup().await;
}

#[tokio::test]
async fn db_contained_effect_commits_atomically() {
    let db = TestDb::new().await;
    let c = claimed(&db).await;
    let mut conn = db.pool.get().await.unwrap();
    diesel::sql_query("CREATE TABLE effects (n INT)")
        .execute(&mut conn)
        .await
        .unwrap();
    conn.transaction::<_, SchedulerError, _>(async |tx| {
        store::lock_owned(tx, &c.lease).await?;
        diesel::sql_query("INSERT INTO effects VALUES (1)")
            .execute(tx)
            .await?;
        store::complete(tx, &c.lease).await
    })
    .await
    .unwrap();
    assert_eq!(db.scalar("SELECT count(*) AS n FROM effects").await, 1);
    let t = store::task(&mut conn, c.lease.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(t.state.status(), TaskStatus::Done);
    db.cleanup().await;
}

#[tokio::test]
async fn db_contained_failure_rolls_back_effect_and_completion() {
    let db = TestDb::new().await;
    let c = claimed(&db).await;
    let mut conn = db.pool.get().await.unwrap();
    diesel::sql_query("CREATE TABLE effects (n INT)")
        .execute(&mut conn)
        .await
        .unwrap();
    let r = conn
        .transaction::<(), SchedulerError, _>(async |tx| {
            store::lock_owned(tx, &c.lease).await?;
            diesel::sql_query("INSERT INTO effects VALUES (1)")
                .execute(tx)
                .await?;
            Err(SchedulerError::Invariant("handler failed"))
        })
        .await;
    assert!(r.is_err());
    assert_eq!(db.scalar("SELECT count(*) AS n FROM effects").await, 0);
    let t = store::task(&mut conn, c.lease.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(t.state.status(), TaskStatus::Running);
    assert_eq!(t.lock_token, c.lease.lock_token);
    db.cleanup().await;
}

#[tokio::test]
async fn stale_worker_cannot_emit_outbox_command() {
    let db = TestDb::new().await;
    let c = claimed(&db).await;
    db.expire_lease(c.lease.task_id).await;
    let mut conn = db.pool.get().await.unwrap();
    lost(
        store::complete_with_outbox(&mut conn, &c.lease, &cmd("e", "ck")).await,
        &c.lease,
    );
    assert_eq!(db.scalar("SELECT count(*) AS n FROM queue_outbox").await, 0);
    db.cleanup().await;
}

#[tokio::test]
async fn largest_command_key_is_stored() {
    let db = TestDb::new().await;
    let c = claimed(&db).await;
    // Fixed-seed xorshift64 so the key is high-entropy (incompressible) yet deterministic.
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut key = String::new();
    while key.len() < 2048 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let ch = match x % 4 {
            0 => char::from_u32(0x21 + u32::try_from((x >> 8) % 94).unwrap()).unwrap(),
            1 => char::from_u32(0xA1 + u32::try_from((x >> 8) % 0x500).unwrap()).unwrap(),
            2 => char::from_u32(0x4E00 + u32::try_from((x >> 8) % 0x2000).unwrap()).unwrap(),
            _ => char::from_u32(0x1F300 + u32::try_from((x >> 8) % 0x300).unwrap()).unwrap(),
        };
        if key.len() + ch.len_utf8() <= 2048 {
            key.push(ch);
        } else {
            key.push('a');
        }
    }
    assert_eq!(key.len(), 2048);
    let mut conn = db.pool.get().await.unwrap();
    let w = store::complete_with_outbox(&mut conn, &c.lease, &cmd("e", &key))
        .await
        .unwrap();
    assert_eq!(w, OutboxWrite::Inserted);
    db.cleanup().await;
}

async fn preinsert(conn: &mut AsyncPgConnection, command_key: &str, payload: &str) {
    diesel::sql_query(
        "INSERT INTO queue_outbox (task_key, effect_name, command_key, topic, payload) \
         VALUES ('k', 'e', $1, 't', $2::jsonb)",
    )
    .bind::<diesel::sql_types::Text, _>(command_key)
    .bind::<diesel::sql_types::Text, _>(payload)
    .execute(conn)
    .await
    .unwrap();
}

#[tokio::test]
async fn same_effect_and_content_is_already_present() {
    let db = TestDb::new().await;
    let c = claimed(&db).await;
    let mut conn = db.pool.get().await.unwrap();
    preinsert(&mut conn, "ck", "{}").await;
    let w = store::complete_with_outbox(&mut conn, &c.lease, &cmd("e", "ck"))
        .await
        .unwrap();
    assert_eq!(w, OutboxWrite::AlreadyPresent);
    let t = store::task(&mut conn, c.lease.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(t.state.status(), TaskStatus::Done);
    db.cleanup().await;
}

#[tokio::test]
async fn same_effect_with_different_content_conflicts_and_rolls_back() {
    let db = TestDb::new().await;
    let c = claimed(&db).await;
    let mut conn = db.pool.get().await.unwrap();
    preinsert(&mut conn, "older", "{}").await;
    let r = store::complete_with_outbox(&mut conn, &c.lease, &cmd("e", "ck")).await;
    assert!(
        matches!(r, Err(SchedulerError::OutboxConflict { .. })),
        "{r:?}"
    );
    let t = store::task(&mut conn, c.lease.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(t.state.status(), TaskStatus::Running);
    assert_eq!(
        db.scalar("SELECT count(*) AS n FROM queue_task_attempts WHERE finished_at IS NULL")
            .await,
        1
    );
    db.cleanup().await;
}

#[tokio::test]
async fn outbox_payload_key_order_is_not_a_conflict() {
    let db = TestDb::new().await;
    let c = claimed(&db).await;
    let mut conn = db.pool.get().await.unwrap();
    preinsert(&mut conn, "ck", r#"{"b": 2,   "a": 1}"#).await;
    let mut command = cmd("e", "ck");
    command.payload = serde_json::json!({"a": 1, "b": 2});
    let w = store::complete_with_outbox(&mut conn, &c.lease, &command)
        .await
        .unwrap();
    assert_eq!(w, OutboxWrite::AlreadyPresent);
    db.cleanup().await;
}

/// A receiver decodes `1.0` and `1` differently, so a scale difference is a different command.
#[tokio::test]
async fn outbox_payload_scale_difference_is_a_conflict() {
    let db = TestDb::new().await;
    let c = claimed(&db).await;
    let mut conn = db.pool.get().await.unwrap();
    preinsert(&mut conn, "ck", r#"{"n":1}"#).await;
    let mut command = cmd("e", "ck");
    command.payload = serde_json::json!({"n": 1.0});
    let r = store::complete_with_outbox(&mut conn, &c.lease, &command).await;
    assert!(
        matches!(r, Err(SchedulerError::OutboxConflict { .. })),
        "{r:?}"
    );
    db.cleanup().await;
}

#[tokio::test]
async fn command_key_collision_across_tasks_fails() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    preinsert(&mut conn, "pre", "{}").await;
    // A fresh task whose command_key collides with another task's row fails loudly.
    enqueue_key(&mut conn, "k2").await;
    let c2 = claim_one(&mut conn).await.unwrap();
    let r = store::complete_with_outbox(&mut conn, &c2.lease, &cmd("e", "pre")).await;
    assert!(matches!(r, Err(SchedulerError::Database(_))), "{r:?}");
    let t = store::task(&mut conn, c2.lease.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(t.state.status(), TaskStatus::Running);
    db.cleanup().await;
}

#[tokio::test]
async fn new_outbox_row_is_inserted() {
    let db = TestDb::new().await;
    let c = claimed(&db).await;
    let mut conn = db.pool.get().await.unwrap();
    assert_eq!(
        store::complete_with_outbox(&mut conn, &c.lease, &cmd("e", "ck"))
            .await
            .unwrap(),
        OutboxWrite::Inserted
    );
    assert_eq!(db.scalar("SELECT count(*) AS n FROM queue_outbox").await, 1);
    db.cleanup().await;
}

#[tokio::test]
async fn completed_by_detects_own_completion() {
    let db = TestDb::new().await;
    let c = claimed(&db).await;
    let mut conn = db.pool.get().await.unwrap();
    assert!(!store::completed_by(&mut conn, &c.lease).await.unwrap());
    store::complete(&mut conn, &c.lease).await.unwrap();
    assert!(store::completed_by(&mut conn, &c.lease).await.unwrap());
    let other = TaskLease {
        lock_token: LockToken(c.lease.lock_token.0 + 1),
        ..c.lease.clone()
    };
    assert!(!store::completed_by(&mut conn, &other).await.unwrap());
    db.cleanup().await;
}

#[tokio::test]
async fn release_requeues_and_refunds_the_attempt() {
    let db = TestDb::new().await;
    let c = claimed(&db).await;
    let mut conn = db.pool.get().await.unwrap();
    store::release(&mut conn, &c.lease).await.unwrap();
    let t = store::task(&mut conn, c.lease.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(t.state.status(), TaskStatus::Queued);
    assert_eq!(t.attempts, 0);
    assert_eq!(t.due_at, c.due_at); // keeps its place in line
    assert_eq!(t.lock_token, c.lease.lock_token);
    assert!(t.last_error.is_none()); // Queued carries no lock fields
    let a = store::attempts(&mut conn, c.lease.task_id).await.unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(common::outcome(&a[0]), Some(AttemptOutcome::Released));
    assert!(a[0].error.is_none());
    // The next claim reuses attempt_no 1 under a new token.
    let again = claim_one(&mut conn).await.unwrap();
    assert_eq!(
        (again.lease.attempt_no, again.lease.lock_token),
        (AttemptNo::FIRST, LockToken(2))
    );
    db.cleanup().await;
}

#[tokio::test]
async fn release_with_stale_or_expired_lease_is_lost_ownership() {
    let db = TestDb::new().await;
    let c = claimed(&db).await;
    let mut conn = db.pool.get().await.unwrap();
    let stale = TaskLease {
        lock_token: LockToken(c.lease.lock_token.0 + 1),
        ..c.lease.clone()
    };
    lost(store::release(&mut conn, &stale).await, &stale);
    db.expire_lease(c.lease.task_id).await;
    lost(store::release(&mut conn, &c.lease).await, &c.lease);
    let t = store::task(&mut conn, c.lease.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((t.state.status(), t.attempts), (TaskStatus::Running, 1));
    db.cleanup().await;
}
