#![cfg(feature = "axum")]
mod common;
use common::TestDb;

#[tokio::test]
async fn router_builds() {
    let db = TestDb::new().await;
    let _ = pg_task_scheduler::admin::router(db.pool.clone());
    db.cleanup().await;
}
