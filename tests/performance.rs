mod common;

use common::TestDb;
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};

#[derive(diesel::QueryableByName)]
struct PlanLine {
    #[diesel(sql_type = diesel::sql_types::Text)]
    #[diesel(column_name = "QUERY PLAN")]
    line: String,
}

#[tokio::test]
async fn pending_claim_uses_hot_partial_index() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    conn.batch_execute("SET enable_seqscan = off")
        .await
        .unwrap();
    let plan = diesel::sql_query(
        r#"EXPLAIN SELECT id FROM scheduler_runs
           WHERE state = 'pending'::scheduler_run_state
             AND available_at <= now() AND job_name = 'example-task'
           ORDER BY priority DESC, available_at, id LIMIT 16"#,
    )
    .get_results::<PlanLine>(&mut conn)
    .await
    .unwrap()
    .into_iter()
    .map(|row| row.line)
    .collect::<Vec<_>>()
    .join("\n");
    assert!(
        plan.contains("scheduler_runs_pending_claim_idx"),
        "unexpected query plan:\n{plan}"
    );
    db.cleanup().await;
}
