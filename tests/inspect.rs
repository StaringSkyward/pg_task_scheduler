mod common;

use chrono::{Duration, Utc};
use common::TestDb;
use diesel_async::RunQueryDsl;
use pg_task_scheduler::store;
use pg_task_scheduler::{RunId, RunState, WorkerId};
use uuid::Uuid;

/// Directly inserts a scheduler_run row (bypassing materialize) and returns the run_id.
async fn insert_run(db: &TestDb, job_id: Uuid) -> Uuid {
    let mut conn = db.pool.get().await.unwrap();
    #[derive(diesel::QueryableByName)]
    struct IdRow {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
    }
    diesel::sql_query(
        "INSERT INTO scheduler_runs (job_id, scheduled_for) \
         VALUES ($1, now()) RETURNING id",
    )
    .bind::<diesel::sql_types::Uuid, _>(job_id)
    .get_result::<IdRow>(&mut conn)
    .await
    .unwrap()
    .id
}

#[tokio::test]
async fn missing_run_returns_none() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();

    let run_id = RunId(Uuid::new_v4());
    let state = store::run_state(&mut conn, run_id).await.unwrap();
    assert!(state.is_none(), "expected None for unknown run_id");

    db.cleanup().await;
}

#[tokio::test]
async fn run_with_no_lease_or_outcome_is_pending() {
    let db = TestDb::new().await;
    let job_id = db
        .insert_job(
            "j-pending",
            "*/1 * * * *",
            Utc::now() - Duration::minutes(1),
        )
        .await;
    let raw_run_id = insert_run(&db, job_id).await;
    let run_id = RunId(raw_run_id);

    let mut conn = db.pool.get().await.unwrap();
    let state = store::run_state(&mut conn, run_id).await.unwrap();
    match state {
        Some(RunState::Pending) => {}
        other => panic!("expected Some(Pending), got {other:?}"),
    }

    db.cleanup().await;
}

#[tokio::test]
async fn run_with_lease_is_running() {
    let db = TestDb::new().await;
    let job_id = db
        .insert_job(
            "j-running",
            "*/1 * * * *",
            Utc::now() - Duration::minutes(1),
        )
        .await;
    let raw_run_id = insert_run(&db, job_id).await;
    let run_id = RunId(raw_run_id);

    // Directly insert a lease row (bypassing claim_one, which doesn't exist yet).
    let worker_id = "test-worker-001";
    let token = Uuid::new_v4();
    {
        let mut conn = db.pool.get().await.unwrap();
        diesel::sql_query(
            "INSERT INTO scheduler_run_leases \
             (run_id, worker_id, lease_token, lease_expires_at) \
             VALUES ($1, $2, $3, now() + interval '5 minutes')",
        )
        .bind::<diesel::sql_types::Uuid, _>(raw_run_id)
        .bind::<diesel::sql_types::Text, _>(worker_id)
        .bind::<diesel::sql_types::Uuid, _>(token)
        .execute(&mut conn)
        .await
        .unwrap();
    }

    let mut conn = db.pool.get().await.unwrap();
    let state = store::run_state(&mut conn, run_id).await.unwrap();
    match state {
        Some(RunState::Running(lease)) => {
            assert_eq!(lease.worker_id, WorkerId::new(worker_id));
            // Verify the token round-trips: LeaseToken's inner is private, so we check
            // via Debug that it contains the same UUID string.
            assert!(format!("{lease:?}").contains(&token.to_string()));
        }
        other => panic!("expected Some(Running(..)), got {other:?}"),
    }

    db.cleanup().await;
}

#[tokio::test]
async fn run_with_completed_outcome_is_completed() {
    let db = TestDb::new().await;
    let job_id = db
        .insert_job(
            "j-completed",
            "*/1 * * * *",
            Utc::now() - Duration::minutes(1),
        )
        .await;
    let raw_run_id = insert_run(&db, job_id).await;
    let run_id = RunId(raw_run_id);

    {
        let mut conn = db.pool.get().await.unwrap();
        diesel::sql_query(
            "INSERT INTO scheduler_run_outcomes (run_id, outcome, last_error) \
             VALUES ($1, 'completed', NULL)",
        )
        .bind::<diesel::sql_types::Uuid, _>(raw_run_id)
        .execute(&mut conn)
        .await
        .unwrap();
    }

    let mut conn = db.pool.get().await.unwrap();
    let state = store::run_state(&mut conn, run_id).await.unwrap();
    match state {
        Some(RunState::Completed { finished_at }) => {
            // finished_at should be recent
            assert!(Utc::now().signed_duration_since(finished_at).num_seconds() < 10);
        }
        other => panic!("expected Some(Completed {{ .. }}), got {other:?}"),
    }

    db.cleanup().await;
}

#[tokio::test]
async fn run_with_failed_outcome_is_failed() {
    let db = TestDb::new().await;
    let job_id = db
        .insert_job("j-failed", "*/1 * * * *", Utc::now() - Duration::minutes(1))
        .await;
    let raw_run_id = insert_run(&db, job_id).await;
    let run_id = RunId(raw_run_id);

    let error_msg = "something went boom";
    {
        let mut conn = db.pool.get().await.unwrap();
        diesel::sql_query(
            "INSERT INTO scheduler_run_outcomes (run_id, outcome, last_error) \
             VALUES ($1, 'failed', $2)",
        )
        .bind::<diesel::sql_types::Uuid, _>(raw_run_id)
        .bind::<diesel::sql_types::Text, _>(error_msg)
        .execute(&mut conn)
        .await
        .unwrap();
    }

    let mut conn = db.pool.get().await.unwrap();
    let state = store::run_state(&mut conn, run_id).await.unwrap();
    match state {
        Some(RunState::Failed { error, finished_at }) => {
            assert_eq!(error, error_msg);
            assert!(Utc::now().signed_duration_since(finished_at).num_seconds() < 10);
        }
        other => panic!("expected Some(Failed {{ .. }}), got {other:?}"),
    }

    db.cleanup().await;
}
