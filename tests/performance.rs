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
async fn claim_uses_claim_index() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    conn.batch_execute("SET enable_seqscan = off")
        .await
        .unwrap();
    let plan = diesel::sql_query(
        r#"EXPLAIN SELECT id FROM queue_tasks
           WHERE status = 'queued' AND due_at <= now() AND attempts < max_attempts
             AND task_name = ANY('{email}')
           ORDER BY priority DESC, due_at, id LIMIT 16"#,
    )
    .get_results::<PlanLine>(&mut conn)
    .await
    .unwrap()
    .into_iter()
    .map(|row| row.line)
    .collect::<Vec<_>>()
    .join("\n");
    assert!(
        plan.contains("queue_tasks_claim_idx"),
        "unexpected query plan:\n{plan}"
    );
    db.cleanup().await;
}
