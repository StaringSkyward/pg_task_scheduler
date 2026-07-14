mod common;

use common::TestDb;
use diesel::result::{DatabaseErrorKind, Error as DieselError};
use diesel_async::RunQueryDsl;

#[derive(diesel::QueryableByName)]
struct IdRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: uuid::Uuid,
}

/// Raw insert of a job with the given name, bypassing the Rust JobName
/// constructor so the SQL CHECK is what is under test.
async fn insert_job_with_name(db: &TestDb, name: &str) -> Result<usize, DieselError> {
    let mut conn = db.pool.get().await.unwrap();
    diesel::sql_query(
        "INSERT INTO scheduler_jobs (name, cron_expression, next_run_at) \
         VALUES ($1, '*/5 * * * *', now())",
    )
    .bind::<diesel::sql_types::Text, _>(name)
    .execute(&mut conn)
    .await
}

fn is_check_violation(err: &DieselError) -> bool {
    matches!(
        err,
        DieselError::DatabaseError(DatabaseErrorKind::CheckViolation, _)
    )
}

#[tokio::test]
async fn empty_job_name_is_rejected_by_check() {
    let db = TestDb::new().await;
    let err = insert_job_with_name(&db, "")
        .await
        .expect_err("empty name must violate the CHECK");
    assert!(is_check_violation(&err), "got: {err:?}");
    db.cleanup().await;
}

#[tokio::test]
async fn leading_whitespace_job_name_is_rejected_by_check() {
    let db = TestDb::new().await;
    let err = insert_job_with_name(&db, " x")
        .await
        .expect_err("leading-whitespace name must violate the CHECK");
    assert!(is_check_violation(&err), "got: {err:?}");
    db.cleanup().await;
}

#[tokio::test]
async fn valid_job_name_is_accepted() {
    let db = TestDb::new().await;
    let n = insert_job_with_name(&db, "ok")
        .await
        .expect("valid name should insert");
    assert_eq!(n, 1);
    db.cleanup().await;
}

#[tokio::test]
async fn empty_worker_id_is_rejected_by_check() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();

    let job_id = diesel::sql_query(
        "INSERT INTO scheduler_jobs (name, cron_expression, next_run_at) \
         VALUES ('wjob', '*/5 * * * *', now()) RETURNING id",
    )
    .get_result::<IdRow>(&mut conn)
    .await
    .unwrap()
    .id;

    let run_id = diesel::sql_query(
        "INSERT INTO scheduler_runs (\
             job_id, job_name, job_args, scheduled_for, available_at, max_attempts,\
             lease_duration, retry_backoff\
         ) VALUES ($1, 'wjob', '{}'::jsonb, now(), now(), 3, interval '5 minutes', interval '1 second')\
         RETURNING id",
    )
    .bind::<diesel::sql_types::Uuid, _>(job_id)
    .get_result::<IdRow>(&mut conn)
    .await
    .unwrap()
    .id;

    let err = diesel::sql_query(
        "UPDATE scheduler_runs SET state = 'running'::scheduler_run_state, attempt_count = 1,\
             worker_id = '', lease_token = gen_random_uuid(), started_at = now(),\
             lease_expires_at = now() + interval '5 minutes' WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(run_id)
    .execute(&mut conn)
    .await
    .expect_err("empty worker_id must violate the CHECK");
    assert!(is_check_violation(&err), "got: {err:?}");

    db.cleanup().await;
}
