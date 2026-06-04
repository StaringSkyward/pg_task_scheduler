mod common;
use common::TestDb;
use diesel_async::RunQueryDsl;

#[tokio::test]
async fn harness_isolates_schema() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let c = diesel::sql_query("SELECT count(*)::bigint AS n FROM scheduler_runs")
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
