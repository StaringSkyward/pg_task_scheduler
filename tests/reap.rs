mod common;
use chrono::{Duration, Utc};
use common::TestDb;
use pg_task_scheduler::store;
use pg_task_scheduler::{RunState, WorkerId};
use std::num::NonZeroUsize;

#[tokio::test]
async fn dead_letters_exhausted_runs() {
    let db = TestDb::new().await;
    let job = db
        .insert_job_full(
            "j",
            "*/1 * * * *",
            Utc::now() - Duration::minutes(1),
            "1 second",
            1,
            false,
        )
        .await;
    let mut conn = db.pool.get().await.unwrap();
    store::materialize_due_jobs(&mut conn).await.unwrap();
    let c = store::claim_one(&mut conn, &WorkerId::try_from("w").unwrap(), &["j".into()])
        .await
        .unwrap()
        .unwrap();
    db.force_lease_expired(job).await;

    assert_eq!(store::reap_expired(&mut conn).await.unwrap(), 1);
    assert!(matches!(
        store::run_state(&mut conn, c.run_id).await.unwrap(),
        Some(RunState::Failed { .. })
    ));
    db.cleanup().await;
}

#[tokio::test]
async fn requeues_reclaimable_runs() {
    let db = TestDb::new().await;
    let job = db
        .insert_job_full(
            "j",
            "*/1 * * * *",
            Utc::now() - Duration::minutes(1),
            "1 second",
            3,
            false,
        )
        .await;
    let mut conn = db.pool.get().await.unwrap();
    store::materialize_due_jobs(&mut conn).await.unwrap();
    let c = store::claim_one(&mut conn, &WorkerId::try_from("w").unwrap(), &["j".into()])
        .await
        .unwrap()
        .unwrap();
    db.force_lease_expired(job).await;

    assert_eq!(store::reap_expired(&mut conn).await.unwrap(), 0);
    assert!(matches!(
        store::run_state(&mut conn, c.run_id).await.unwrap(),
        Some(RunState::Pending)
    ));
    db.cleanup().await;
}

#[tokio::test]
async fn concurrent_recovery_processes_task_once() {
    let db = TestDb::new().await;
    let job = db
        .insert_job_full(
            "j",
            "*/1 * * * *",
            Utc::now() - Duration::minutes(1),
            "1 second",
            1,
            false,
        )
        .await;
    let mut setup = db.pool.get().await.unwrap();
    store::materialize_due_jobs(&mut setup).await.unwrap();
    let c = store::claim_one(&mut setup, &WorkerId::try_from("w").unwrap(), &["j".into()])
        .await
        .unwrap()
        .unwrap();
    db.force_lease_expired(job).await;
    drop(setup);

    let mut a = db.pool.get().await.unwrap();
    let mut b = db.pool.get().await.unwrap();
    let limit = NonZeroUsize::new(10).unwrap();
    let (left, right) = tokio::join!(
        store::recover_expired(&mut a, limit),
        store::recover_expired(&mut b, limit),
    );
    let total = left.unwrap().failed + right.unwrap().failed;
    assert_eq!(total, 1, "exactly one recovery owns the expired row");

    let mut chk = db.pool.get().await.unwrap();
    assert!(matches!(
        store::run_state(&mut chk, c.run_id).await.unwrap(),
        Some(RunState::Failed { .. })
    ));
    db.cleanup().await;
}
