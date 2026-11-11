mod common;
use common::TestDb;
use diesel_async::RunQueryDsl;

#[tokio::test]
async fn harness_isolates_schema() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let c = diesel::sql_query("SELECT count(*)::bigint AS n FROM queue_tasks")
        .get_result::<Count>(&mut conn)
        .await
        .unwrap()
        .n;
    assert_eq!(c, 0);
    db.cleanup().await;
}

#[derive(diesel::QueryableByName)]
struct Count {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

/// A pooled connection left idle in a transaction that holds a lock in the test schema (a
/// dropped handler or transaction future) must not make cleanup's DROP SCHEMA wait forever.
#[tokio::test]
async fn cleanup_ends_idle_transactions_left_in_the_pool() {
    use diesel_async::SimpleAsyncConnection;
    let db = TestDb::new().await;
    {
        let mut conn = db.pool.get().await.unwrap();
        conn.batch_execute("BEGIN; LOCK TABLE queue_tasks IN ACCESS SHARE MODE")
            .await
            .unwrap();
        // Back to the pool with the transaction still open.
    }
    tokio::time::timeout(std::time::Duration::from_secs(10), db.cleanup())
        .await
        .expect("cleanup hung behind an idle transaction");
}
