mod common;
use chrono::{Duration, Utc};
use common::TestDb;
use pg_task_scheduler::store;
use pg_task_scheduler::{LeaseToken, Outcome, RunState, WorkerId};

#[tokio::test]
async fn completes_with_matching_token() {
    let db = TestDb::new().await;
    db.insert_job("j", "*/1 * * * *", Utc::now() - Duration::minutes(1))
        .await;
    let mut conn = db.pool.get().await.unwrap();
    store::materialize_due_jobs(&mut conn).await.unwrap();
    let c = store::claim_one(&mut conn, &WorkerId::new("w"), &["j".to_string()])
        .await
        .unwrap()
        .unwrap();

    assert!(
        store::finalize_run(&mut conn, c.run_id, c.lease_token, Outcome::Completed)
            .await
            .unwrap()
    );
    assert!(matches!(
        store::run_state(&mut conn, c.run_id).await.unwrap(),
        Some(RunState::Completed { .. })
    ));
    db.cleanup().await;
}

#[tokio::test]
async fn fails_with_error_and_clears_lease() {
    let db = TestDb::new().await;
    db.insert_job("j", "*/1 * * * *", Utc::now() - Duration::minutes(1))
        .await;
    let mut conn = db.pool.get().await.unwrap();
    store::materialize_due_jobs(&mut conn).await.unwrap();
    let c = store::claim_one(&mut conn, &WorkerId::new("w"), &["j".to_string()])
        .await
        .unwrap()
        .unwrap();

    assert!(
        store::finalize_run(
            &mut conn,
            c.run_id,
            c.lease_token,
            Outcome::Failed("boom".into())
        )
        .await
        .unwrap()
    );
    match store::run_state(&mut conn, c.run_id).await.unwrap() {
        Some(RunState::Failed { error, .. }) => assert_eq!(error, "boom"),
        other => panic!("expected Failed, got {other:?}"),
    }
    db.cleanup().await;
}

#[tokio::test]
async fn stale_token_is_fenced_out() {
    let db = TestDb::new().await;
    db.insert_job("j", "*/1 * * * *", Utc::now() - Duration::minutes(1))
        .await;
    let mut conn = db.pool.get().await.unwrap();
    store::materialize_due_jobs(&mut conn).await.unwrap();
    let c = store::claim_one(&mut conn, &WorkerId::new("w"), &["j".to_string()])
        .await
        .unwrap()
        .unwrap();

    let applied = store::finalize_run(
        &mut conn,
        c.run_id,
        LeaseToken::generate(),
        Outcome::Completed,
    )
    .await
    .unwrap();
    assert!(!applied);
    assert!(matches!(
        store::run_state(&mut conn, c.run_id).await.unwrap(),
        Some(RunState::Running(_))
    ));
    db.cleanup().await;
}
