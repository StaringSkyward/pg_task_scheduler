mod common;

use std::num::NonZeroUsize;
use std::time::Duration;

use common::TestDb;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use pg_task_scheduler::store;
use pg_task_scheduler::{
    AttemptOutcome, ClaimedTask, EnqueueOptions, LockToken, MaxAttempts, OutboxCommand,
    RetryOutcome, SchedulerError, Task, TaskKey, TaskLease, TaskStatus, WorkerId, enqueue,
};
use serde_json::json;

struct Email;
impl Task for Email {
    const NAME: &'static str = "email";
    type Args = serde_json::Value;
}

fn names() -> Vec<String> {
    vec!["email".to_owned()]
}

async fn claim_one(conn: &mut AsyncPgConnection) -> Option<ClaimedTask> {
    store::claim(
        conn,
        &WorkerId::generate(),
        &names(),
        NonZeroUsize::new(1).unwrap(),
        Duration::from_secs(60),
    )
    .await
    .unwrap()
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

fn cmd(effect: &str, key: &str) -> OutboxCommand {
    OutboxCommand {
        effect_name: effect.to_owned(),
        command_key: key.to_owned(),
        topic: "t".to_owned(),
        payload: json!({"a": 1}),
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
    assert_eq!(t.status, TaskStatus::Done);
    assert_eq!(t.locked_by, None);
    assert_eq!(t.locked_until, None);
    assert!(t.completed_at.is_some());
    let a = store::attempts(&mut conn, c.lease.task_id).await.unwrap();
    assert_eq!(a[0].outcome, Some(AttemptOutcome::Done));
    db.cleanup().await;
}

#[tokio::test]
async fn heartbeat_extends_lease() {
    let db = TestDb::new().await;
    let c = claimed(&db).await;
    let mut conn = db.pool.get().await.unwrap();
    let until = store::heartbeat(&mut conn, &c.lease, Duration::from_secs(120))
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
    assert_eq!(t.status, TaskStatus::Queued);
    assert_eq!(t.last_error.as_deref(), Some("boom"));
    let a = store::attempts(&mut conn, c.lease.task_id).await.unwrap();
    assert_eq!(a[0].outcome, Some(AttemptOutcome::Retry));

    diesel::sql_query("UPDATE queue_tasks SET due_at = now()")
        .execute(&mut conn)
        .await
        .unwrap();
    let c2 = claim_one(&mut conn).await.unwrap();
    assert_eq!(c2.lease.attempt_no, 2);
    let out = store::retry(&mut conn, &c2.lease, "boom2", Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(out, RetryOutcome::Dead);
    let t = store::task(&mut conn, c.lease.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(t.status, TaskStatus::Dead);
    assert!(t.completed_at.is_some());
    let a = store::attempts(&mut conn, c.lease.task_id).await.unwrap();
    assert_eq!(a[1].outcome, Some(AttemptOutcome::Dead));
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
    assert_eq!(t.status, TaskStatus::Dead);
    let a = store::attempts(&mut conn, c.lease.task_id).await.unwrap();
    assert_eq!(a[0].outcome, Some(AttemptOutcome::Dead));
    assert_eq!(a[0].error.as_deref(), Some("bad input"));
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
        store::heartbeat(&mut conn, l, Duration::from_secs(60)).await,
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
    assert_eq!(t.status, TaskStatus::Running);
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
    let new = claim_one(&mut conn).await.unwrap();
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
    assert_eq!(t.status, TaskStatus::Done);
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
    assert_eq!(t.status, TaskStatus::Running);
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
async fn outbox_insert_is_idempotent_per_effect() {
    let db = TestDb::new().await;
    let c = claimed(&db).await;
    let mut conn = db.pool.get().await.unwrap();
    diesel::sql_query(
        "INSERT INTO queue_outbox (task_key, effect_name, command_key, topic, payload) \
         VALUES ('k', 'e', 'pre', 't', '{}')",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    let inserted = store::complete_with_outbox(&mut conn, &c.lease, &cmd("e", "ck"))
        .await
        .unwrap();
    assert!(!inserted);
    let t = store::task(&mut conn, c.lease.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(t.status, TaskStatus::Done);

    // A fresh task whose command_key collides with another task's row fails loudly.
    enqueue_key(&mut conn, "k2").await;
    let c2 = claim_one(&mut conn).await.unwrap();
    let r = store::complete_with_outbox(&mut conn, &c2.lease, &cmd("e", "pre")).await;
    assert!(matches!(r, Err(SchedulerError::Database(_))), "{r:?}");
    let t = store::task(&mut conn, c2.lease.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(t.status, TaskStatus::Running);
    db.cleanup().await;
}

#[tokio::test]
async fn new_outbox_row_reports_true() {
    let db = TestDb::new().await;
    let c = claimed(&db).await;
    let mut conn = db.pool.get().await.unwrap();
    assert!(
        store::complete_with_outbox(&mut conn, &c.lease, &cmd("e", "ck"))
            .await
            .unwrap()
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
