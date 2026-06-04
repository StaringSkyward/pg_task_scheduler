mod common;
use chrono::{Duration, Utc};
use common::TestDb;
use pg_task_scheduler::store;
use pg_task_scheduler::{RunState, WorkerId};

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
    let c = store::claim_one(&mut conn, &WorkerId::new("w"), &["j".into()])
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
async fn leaves_reclaimable_runs() {
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
    let c = store::claim_one(&mut conn, &WorkerId::new("w"), &["j".into()])
        .await
        .unwrap()
        .unwrap();
    db.force_lease_expired(job).await;

    assert_eq!(store::reap_expired(&mut conn).await.unwrap(), 0);
    assert!(matches!(
        store::run_state(&mut conn, c.run_id).await.unwrap(),
        Some(RunState::Running(_))
    ));
    db.cleanup().await;
}
