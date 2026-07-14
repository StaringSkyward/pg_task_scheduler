mod common;
use chrono::{Duration, Utc};
use common::TestDb;
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use pg_task_scheduler::store;
use pg_task_scheduler::{FinalizeOutcome, LeaseToken, Outcome, RunState, WorkerId};

#[tokio::test]
async fn completes_with_matching_token() {
    let db = TestDb::new().await;
    db.insert_job("j", "*/1 * * * *", Utc::now() - Duration::minutes(1))
        .await;
    let mut conn = db.pool.get().await.unwrap();
    store::materialize_due_jobs(&mut conn).await.unwrap();
    let c = store::claim_one(
        &mut conn,
        &WorkerId::try_from("w").unwrap(),
        &["j".to_string()],
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(
        store::finalize_run(&mut conn, c.run_id, c.lease_token, Outcome::Completed)
            .await
            .unwrap(),
        FinalizeOutcome::Applied
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
    let c = store::claim_one(
        &mut conn,
        &WorkerId::try_from("w").unwrap(),
        &["j".to_string()],
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(
        store::finalize_run(
            &mut conn,
            c.run_id,
            c.lease_token,
            Outcome::Failed("boom".into())
        )
        .await
        .unwrap(),
        FinalizeOutcome::Applied
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
    let c = store::claim_one(
        &mut conn,
        &WorkerId::try_from("w").unwrap(),
        &["j".to_string()],
    )
    .await
    .unwrap()
    .unwrap();

    let outcome = store::finalize_run(
        &mut conn,
        c.run_id,
        LeaseToken::generate(),
        Outcome::Completed,
    )
    .await
    .unwrap();
    assert_eq!(outcome, FinalizeOutcome::Fenced);
    assert!(matches!(
        store::run_state(&mut conn, c.run_id).await.unwrap(),
        Some(RunState::Running(_))
    ));
    db.cleanup().await;
}

#[tokio::test]
async fn already_terminal_after_reap() {
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
    // Reaper records the terminal outcome (and the trigger clears the lease).
    assert_eq!(store::reap_expired(&mut conn).await.unwrap(), 1);
    // The original worker finalizes late with its (now-cleared) token: benign no-op.
    let outcome = store::finalize_run(&mut conn, c.run_id, c.lease_token, Outcome::Completed)
        .await
        .unwrap();
    assert_eq!(outcome, FinalizeOutcome::AlreadyTerminal);
    // The late Completed finalize recorded nothing; the reaper's Failed stands.
    assert!(matches!(
        store::run_state(&mut conn, c.run_id).await.unwrap(),
        Some(RunState::Failed { .. })
    ));
    db.cleanup().await;
}

#[tokio::test]
async fn stale_finalize_blocked_by_reclaim_is_fenced() {
    let db = TestDb::new().await;
    db.insert_job("j", "*/1 * * * *", Utc::now() - Duration::minutes(1))
        .await;
    let mut setup = db.pool.get().await.unwrap();
    store::materialize_due_jobs(&mut setup).await.unwrap();
    let c = store::claim_one(&mut setup, &WorkerId::try_from("w").unwrap(), &["j".into()])
        .await
        .unwrap()
        .unwrap();
    drop(setup);

    // Conn A takes the task-row mutex before rotating the fencing token.
    let mut a = db.pool.get().await.unwrap();
    a.batch_execute("BEGIN").await.unwrap();
    #[derive(diesel::QueryableByName)]
    struct Locked {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        #[allow(dead_code)]
        id: uuid::Uuid,
    }
    diesel::sql_query("SELECT id FROM scheduler_runs WHERE id = $1 FOR UPDATE")
        .bind::<diesel::sql_types::Uuid, _>(c.run_id.0)
        .get_result::<Locked>(&mut a)
        .await
        .unwrap();

    // Conn B uses the original token and blocks on the same task row.
    let mut b = db.pool.get().await.unwrap();
    let b_pid = common::backend_pid(&mut b).await;
    let run_id = c.run_id;
    let token = c.lease_token;
    let handle = tokio::spawn(async move {
        store::finalize_run(&mut b, run_id, token, Outcome::Completed).await
    });

    db.wait_until_lock_blocked(b_pid).await;
    let new_token = LeaseToken::generate();
    diesel::sql_query(
        "UPDATE scheduler_run_attempts SET lease_token = $1, worker_id = 'new-worker' \
         WHERE lease_token = $2",
    )
    .bind::<diesel::sql_types::Uuid, _>(new_token)
    .bind::<diesel::sql_types::Uuid, _>(c.lease_token)
    .execute(&mut a)
    .await
    .unwrap();
    diesel::sql_query(
        "UPDATE scheduler_runs SET lease_token = $1, worker_id = 'new-worker' WHERE id = $2",
    )
    .bind::<diesel::sql_types::Uuid, _>(new_token)
    .bind::<diesel::sql_types::Uuid, _>(c.run_id.0)
    .execute(&mut a)
    .await
    .unwrap();
    a.batch_execute("COMMIT").await.unwrap();

    let outcome = handle
        .await
        .unwrap()
        .expect("stale finalize must resolve without a database error");
    assert_eq!(outcome, FinalizeOutcome::Fenced);

    let mut check = db.pool.get().await.unwrap();
    match store::run_state(&mut check, c.run_id).await.unwrap() {
        Some(RunState::Running(lease)) => assert_eq!(lease.lease_token, new_token),
        other => panic!("new claim must remain running, got {other:?}"),
    }
    db.cleanup().await;
}
