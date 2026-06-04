mod common;

use common::TestDb;
use diesel::result::{DatabaseErrorKind, Error as DieselError};
use diesel_async::RunQueryDsl;

/// Raw insert of a job row with the given lease interval, deliberately bypassing
/// the Rust `LeaseDuration` constructor so the SQL CHECK is what is under test.
async fn insert_with_lease(db: &TestDb, name: &str, lease: &str) -> Result<usize, DieselError> {
    let mut conn = db.pool.get().await.unwrap();
    diesel::sql_query(
        "INSERT INTO scheduler_jobs \
         (name, cron_expression, next_run_at, lease_duration, max_attempts, is_paused) \
         VALUES ($1, '*/5 * * * *', now(), $2::interval, 3, false)",
    )
    .bind::<diesel::sql_types::Text, _>(name)
    .bind::<diesel::sql_types::Text, _>(lease)
    .execute(&mut conn)
    .await
}

#[tokio::test]
async fn zero_lease_duration_is_rejected_by_check() {
    let db = TestDb::new().await;
    let err = insert_with_lease(&db, "zero-lease", "0 seconds")
        .await
        .expect_err("INSERT with lease_duration = 0 must violate the CHECK constraint");
    assert!(
        matches!(
            err,
            DieselError::DatabaseError(DatabaseErrorKind::CheckViolation, _)
        ),
        "expected a check-constraint violation, got: {err:?}"
    );
    db.cleanup().await;
}

#[tokio::test]
async fn positive_lease_duration_is_accepted() {
    let db = TestDb::new().await;
    let n = insert_with_lease(&db, "ok-lease", "5 minutes")
        .await
        .expect("positive lease_duration should insert");
    assert_eq!(n, 1);
    db.cleanup().await;
}
