mod common;
use std::num::NonZeroU32;

use common::TestDb;
use diesel_async::RunQueryDsl;
use pg_task_scheduler::TaskStatus;
use pg_task_scheduler::replay;
use pg_task_scheduler::store::AdminActionKind;
use pg_task_scheduler::store::AdminActionTarget;
use pg_task_scheduler::store::BulkAction;
use pg_task_scheduler::store::TaskAction;
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
    assert_eq!(a[0].kind(), AdminActionKind::Cancel);
    assert!(matches!(
        a[0].target,
        AdminActionTarget::Bulk {
            action: BulkAction::Cancel,
            queue: None,
            max_task_id: 100,
            ..
        }
    ));
    assert_eq!(a[0].affected, 5);
    assert_eq!(a[0].reason, "bulk");
    // Replay relinks the task to its own row, which also names the task: listed once.
    assert!(replay(&mut conn, t.id, "ops", "again").await.unwrap());
    let t = store::task(&mut conn, t.id).await.unwrap().unwrap();
    let a = store::task_actions(&mut conn, &t).await.unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].kind(), AdminActionKind::Replay);
    assert!(matches!(
        a[0].target,
        AdminActionTarget::Task {
            action: TaskAction::Replay,
            ..
        }
    ));
    let all = store::admin_actions(&mut conn, None, NonZeroU32::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(all.len(), 2);
    let older = store::admin_actions(&mut conn, Some(all[0].id), NonZeroU32::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(older.len(), 1);
    assert_eq!(older[0].kind(), AdminActionKind::Cancel);
    db.cleanup().await;
}

#[tokio::test]
async fn bulk_replay_with_error_filter_records_it() {
    let db = TestDb::new().await;
    insert_task(&db, "dead").await;
    exec(&db, "UPDATE queue_tasks SET last_error = 'x'").await;
    let mut conn = db.pool.get().await.unwrap();
    let filter = store::BulkFilter {
        task_name: pg_task_scheduler::TaskName::try_from("email").unwrap(),
        queue: None,
    };
    let out = store::replay_matching(
        &mut conn,
        &filter,
        Some("x"),
        "ops",
        "why",
        store::BULK_BUDGET,
    )
    .await
    .unwrap();
    assert_eq!(out.affected, 1);
    let a = store::admin_actions(&mut conn, None, NonZeroU32::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(a.len(), 1);
    let AdminActionTarget::Bulk {
        action: BulkAction::Replay { last_error },
        task_name,
        ..
    } = &a[0].target
    else {
        panic!("not a bulk replay: {:?}", a[0].target)
    };
    assert_eq!(last_error.as_deref(), Some("x"));
    assert_eq!(task_name.as_str(), "email");
    db.cleanup().await;
}
