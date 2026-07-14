mod common;

use chrono::{Duration, Utc};
use common::TestDb;
use pg_task_scheduler::store;
use pg_task_scheduler::{Outcome, RunId, RunState, WorkerId};
use uuid::Uuid;

async fn materialized(db: &TestDb, name: &str) -> RunId {
    let job_id = db
        .insert_job(name, "*/1 * * * *", Utc::now() - Duration::minutes(1))
        .await;
    let mut conn = db.pool.get().await.unwrap();
    store::materialize_due_jobs(&mut conn).await.unwrap();
    RunId(db.run_ids(job_id).await[0])
}

async fn claimed(db: &TestDb, name: &str) -> pg_task_scheduler::ClaimedRun {
    let run_id = materialized(db, name).await;
    let mut conn = db.pool.get().await.unwrap();
    let claimed = store::claim_one(
        &mut conn,
        &WorkerId::try_from("test-worker").unwrap(),
        &[name.to_owned()],
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(claimed.run_id, run_id);
    claimed
}

#[tokio::test]
async fn missing_run_returns_none() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    assert!(
        store::run_state(&mut conn, RunId(Uuid::new_v4()))
            .await
            .unwrap()
            .is_none()
    );
    db.cleanup().await;
}

#[tokio::test]
async fn run_with_no_lease_or_outcome_is_pending() {
    let db = TestDb::new().await;
    let run_id = materialized(&db, "j-pending").await;
    let mut conn = db.pool.get().await.unwrap();
    assert!(matches!(
        store::run_state(&mut conn, run_id).await.unwrap(),
        Some(RunState::Pending)
    ));
    db.cleanup().await;
}

#[tokio::test]
async fn run_with_lease_is_running() {
    let db = TestDb::new().await;
    let claimed = claimed(&db, "j-running").await;
    let mut conn = db.pool.get().await.unwrap();
    match store::run_state(&mut conn, claimed.run_id).await.unwrap() {
        Some(RunState::Running(lease)) => {
            assert_eq!(lease.worker_id, WorkerId::try_from("test-worker").unwrap());
            assert_eq!(lease.lease_token, claimed.lease_token);
        }
        other => panic!("expected running, got {other:?}"),
    }
    db.cleanup().await;
}

#[tokio::test]
async fn run_with_completed_outcome_is_completed() {
    let db = TestDb::new().await;
    let claimed = claimed(&db, "j-completed").await;
    let mut conn = db.pool.get().await.unwrap();
    let outcome = store::finalize_run(
        &mut conn,
        claimed.run_id,
        claimed.lease_token,
        Outcome::Completed,
    )
    .await
    .unwrap();
    assert_eq!(outcome, pg_task_scheduler::FinalizeOutcome::Applied);
    assert!(matches!(
        store::run_state(&mut conn, claimed.run_id).await.unwrap(),
        Some(RunState::Completed { .. })
    ));
    db.cleanup().await;
}

#[tokio::test]
async fn run_with_failed_outcome_is_failed() {
    let db = TestDb::new().await;
    let claimed = claimed(&db, "j-failed").await;
    let mut conn = db.pool.get().await.unwrap();
    let outcome = store::finalize_run(
        &mut conn,
        claimed.run_id,
        claimed.lease_token,
        Outcome::Failed("something went boom".into()),
    )
    .await
    .unwrap();
    assert_eq!(outcome, pg_task_scheduler::FinalizeOutcome::Applied);
    match store::run_state(&mut conn, claimed.run_id).await.unwrap() {
        Some(RunState::Failed { error, .. }) => assert_eq!(error, "something went boom"),
        other => panic!("expected failed, got {other:?}"),
    }
    db.cleanup().await;
}
