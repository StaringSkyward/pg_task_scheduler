#![cfg(feature = "cron")]
mod common;

use common::TestDb;
use diesel::result::DatabaseErrorKind;
use diesel::result::Error as DieselError;
use diesel_async::RunQueryDsl;

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
