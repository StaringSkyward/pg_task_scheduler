mod common;
use chrono::{Duration, Utc};
use common::TestDb;
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
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
    let c = store::claim_one(&mut conn, &WorkerId::try_from("w").unwrap(), &["j".into()])
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

#[tokio::test]
async fn reap_race_is_noop_not_error() {
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

    // Conn A: hold an UNCOMMITTED terminal-outcome insert for the run (contends the PK).
    let mut a = db.pool.get().await.unwrap();
    a.batch_execute("BEGIN").await.unwrap();
    diesel::sql_query(
        "INSERT INTO scheduler_run_outcomes (run_id, outcome, last_error) \
         VALUES ($1, 'failed'::run_outcome, 'race')",
    )
    .bind::<diesel::sql_types::Uuid, _>(c.run_id.0)
    .execute(&mut a)
    .await
    .unwrap();

    // Conn B: capture its pid, then reap in a task; its insert blocks on A's row.
    let mut b = db.pool.get().await.unwrap();
    let b_pid = common::backend_pid(&mut b).await;
    let handle = tokio::spawn(async move { store::reap_expired(&mut b).await });

    // Deterministically wait until B is blocked, THEN release A.
    db.wait_until_lock_blocked(b_pid).await;
    a.batch_execute("COMMIT").await.unwrap();

    // B unblocks: ON CONFLICT DO NOTHING => Ok(0), NOT a unique violation.
    let reaped = handle
        .await
        .unwrap()
        .expect("reap must not error on a concurrent terminal-insert race");
    assert_eq!(reaped, 0, "the race-loser dead-letters nothing");

    let mut chk = db.pool.get().await.unwrap();
    assert!(matches!(
        store::run_state(&mut chk, c.run_id).await.unwrap(),
        Some(RunState::Failed { .. })
    ));
    db.cleanup().await;
}
