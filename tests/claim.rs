mod common;
use chrono::{Duration, Utc};
use common::TestDb;
use pg_task_scheduler::store;
use pg_task_scheduler::{RunId, RunState, WorkerId};

fn names(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

#[tokio::test]
async fn claims_pending_and_marks_running() {
    let db = TestDb::new().await;
    let job = db
        .insert_job("j", "*/1 * * * *", Utc::now() - Duration::minutes(1))
        .await;
    let mut conn = db.pool.get().await.unwrap();
    store::materialize_due_jobs(&mut conn).await.unwrap();

    let w = WorkerId::new("w1");
    let claimed = store::claim_one(&mut conn, &w, &names(&["j"]))
        .await
        .unwrap()
        .expect("claim");
    assert_eq!(claimed.attempt.get(), 1);
    assert!(claimed.lease_expires_at > Utc::now());

    // The claimed run id must belong to the job we materialized.
    assert_eq!(claimed.run_id, RunId(db.run_ids(job).await[0]));

    match store::run_state(&mut conn, claimed.run_id).await.unwrap() {
        Some(RunState::Running(l)) => assert_eq!(l.lease_token, claimed.lease_token),
        other => panic!("expected Running, got {other:?}"),
    }
    db.cleanup().await;
}

#[tokio::test]
async fn does_not_claim_unregistered_job() {
    let db = TestDb::new().await;
    db.insert_job("j", "*/1 * * * *", Utc::now() - Duration::minutes(1))
        .await;
    let mut conn = db.pool.get().await.unwrap();
    store::materialize_due_jobs(&mut conn).await.unwrap();
    let w = WorkerId::new("w1");
    assert!(
        store::claim_one(&mut conn, &w, &names(&["other"]))
            .await
            .unwrap()
            .is_none()
    );
    db.cleanup().await;
}

#[tokio::test]
async fn concurrent_workers_claim_disjoint() {
    let db = TestDb::new().await;
    db.insert_job("j", "*/1 * * * *", Utc::now() - Duration::minutes(1))
        .await;
    {
        let mut c = db.pool.get().await.unwrap();
        store::materialize_due_jobs(&mut c).await.unwrap();
    }
    let (w1, w2) = (WorkerId::new("w1"), WorkerId::new("w2"));
    let names = names(&["j"]);
    let mut c1 = db.pool.get().await.unwrap();
    let mut c2 = db.pool.get().await.unwrap();
    let (a, b) = tokio::join!(
        store::claim_one(&mut c1, &w1, &names),
        store::claim_one(&mut c2, &w2, &names),
    );
    let some = [a.unwrap(), b.unwrap()]
        .iter()
        .filter(|x| x.is_some())
        .count();
    assert_eq!(some, 1);
    db.cleanup().await;
}

#[tokio::test]
async fn reclaims_expired_until_max_attempts() {
    let db = TestDb::new().await;
    let job = db
        .insert_job_full(
            "j",
            "*/1 * * * *",
            Utc::now() - Duration::minutes(1),
            "1 second",
            2,
            false,
        )
        .await;
    let mut conn = db.pool.get().await.unwrap();
    store::materialize_due_jobs(&mut conn).await.unwrap();
    let w = WorkerId::new("w");

    let a = store::claim_one(&mut conn, &w, &names(&["j"]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.attempt.get(), 1);
    db.force_lease_expired(job).await;
    let b = store::claim_one(&mut conn, &w, &names(&["j"]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(b.attempt.get(), 2);
    db.force_lease_expired(job).await;
    assert!(
        store::claim_one(&mut conn, &w, &names(&["j"]))
            .await
            .unwrap()
            .is_none()
    );
    db.cleanup().await;
}
