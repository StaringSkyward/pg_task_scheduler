mod common;
use common::TestDb;
use diesel_async::RunQueryDsl;
use pg_task_scheduler::TaskStatus;
use pg_task_scheduler::replay;
use pg_task_scheduler::store::AdminActionKind;
use pg_task_scheduler::store::{self};

async fn exec(db: &TestDb, sql: &str) {
    let mut conn = db.pool.get().await.unwrap();
    diesel::sql_query(sql).execute(&mut conn).await.unwrap();
}

async fn insert_task(db: &TestDb, status: &str) -> i64 {
    exec(
        db,
        &format!(
            "INSERT INTO queue_tasks (task_key, task_name, status, attempts, last_error, \
             completed_at) VALUES ('k', 'email', '{status}', 3, 'boom', now())"
        ),
    )
    .await;
    db.scalar("SELECT max(id) AS n FROM queue_tasks").await
}

#[tokio::test]
async fn replay_of_cancelled_task_requeues_it() {
    let db = TestDb::new().await;
    let id = insert_task(&db, "cancelled").await;
    let mut conn = db.pool.get().await.unwrap();
    assert!(
        replay(&mut conn, pg_task_scheduler::TaskId(id), "ops", "again")
            .await
            .unwrap()
    );
    let t = store::task(&mut conn, pg_task_scheduler::TaskId(id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(t.state.status(), TaskStatus::Queued);
    assert_eq!(t.attempts, 0);
    assert_eq!(t.last_error, None);
    let action_id = db
        .scalar("SELECT id AS n FROM queue_admin_actions WHERE action = 'replay'")
        .await;
    assert_eq!(t.admin_action_id, Some(action_id));
    db.cleanup().await;
}

#[tokio::test]
async fn task_actions_include_linked_bulk_action() {
    let db = TestDb::new().await;
    let id = insert_task(&db, "cancelled").await;
    exec(
        &db,
        "INSERT INTO queue_admin_actions (action, task_name, max_task_id, affected, operator, reason) \
         VALUES ('cancel', 'email', 100, 5, 'ops', 'bulk')",
    )
    .await;
    exec(
        &db,
        &format!("UPDATE queue_tasks SET admin_action_id = (SELECT max(id) FROM queue_admin_actions) WHERE id = {id}"),
    )
    .await;
    let mut conn = db.pool.get().await.unwrap();
    let t = store::task(&mut conn, pg_task_scheduler::TaskId(id))
        .await
        .unwrap()
        .unwrap();
    let a = store::task_actions(&mut conn, &t).await.unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].action, AdminActionKind::Cancel);
    assert_eq!(a[0].task_id, None);
    assert_eq!(a[0].affected, 5);
    assert_eq!(a[0].reason, "bulk");
    // Replay relinks the task to its own row, which also names the task: listed once.
    assert!(replay(&mut conn, t.id, "ops", "again").await.unwrap());
    let t = store::task(&mut conn, t.id).await.unwrap().unwrap();
    let a = store::task_actions(&mut conn, &t).await.unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].action, AdminActionKind::Replay);
    let all = store::admin_actions(&mut conn, None, 10).await.unwrap();
    assert_eq!(all.len(), 2);
    let older = store::admin_actions(&mut conn, Some(all[0].id), 10)
        .await
        .unwrap();
    assert_eq!(older.len(), 1);
    assert_eq!(older[0].action, AdminActionKind::Cancel);
    db.cleanup().await;
}
