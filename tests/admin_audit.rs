mod common;
use std::num::NonZeroU32;

use common::TestDb;
use diesel_async::RunQueryDsl;
use pg_task_scheduler::TaskId;
use pg_task_scheduler::TaskStatus;
use pg_task_scheduler::replay;
use pg_task_scheduler::store::AdminActionKind;
use pg_task_scheduler::store::AdminActionTarget;
use pg_task_scheduler::store::BulkAction;
use pg_task_scheduler::store::Count;
use pg_task_scheduler::store::ErrorFilter;
use pg_task_scheduler::store::TaskAction;
use pg_task_scheduler::store::{self};

fn nz(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).unwrap()
}

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
    let a = store::task_actions(&mut conn, TaskId(id), nz(10))
        .await
        .unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].id, action_id);
    db.cleanup().await;
}

/// The task page lists every action that changed the task, single or bulk, newest first, each
/// with its capped count of changed tasks.
#[tokio::test]
async fn task_actions_lists_bulk_and_single_actions() {
    let db = TestDb::new().await;
    exec(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name) VALUES ('k1', 'email'), ('k2', 'email')",
    )
    .await;
    let t1 = TaskId(db.scalar("SELECT min(id) AS n FROM queue_tasks").await);
    let t2 = TaskId(db.scalar("SELECT max(id) AS n FROM queue_tasks").await);
    let mut conn = db.pool.get().await.unwrap();
    let filter = store::BulkFilter {
        task_name: pg_task_scheduler::TaskName::try_from("email").unwrap(),
        queue: None,
    };
    let bulk = store::cancel_matching(&mut conn, &filter, "ops", "bulk", store::BULK_BUDGET)
        .await
        .unwrap();
    assert_eq!(bulk.affected, 2);
    assert!(replay(&mut conn, t1, "ops", "again").await.unwrap());

    let a = store::task_actions(&mut conn, t1, nz(10)).await.unwrap();
    assert_eq!(a.len(), 2);
    assert!(matches!(
        a[0].target,
        AdminActionTarget::Task {
            action: TaskAction::Replay,
            task_id,
            ..
        } if task_id == t1
    ));
    assert_eq!(
        a[0].affected,
        Count {
            value: 1,
            capped: false
        }
    );
    assert_eq!(a[1].id, bulk.action_id);
    assert!(matches!(
        a[1].target,
        AdminActionTarget::Bulk {
            action: BulkAction::Cancel,
            queue: None,
            ..
        }
    ));
    assert_eq!(
        a[1].affected,
        Count {
            value: 2,
            capped: false
        }
    );
    assert_eq!(a[1].reason, "bulk");
    // Only the bulk action changed t2; a cap of 1 marks its count capped.
    let a = store::task_actions(&mut conn, t2, nz(1)).await.unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].id, bulk.action_id);
    assert_eq!(
        a[0].affected,
        Count {
            value: 1,
            capped: true
        }
    );

    let all = store::admin_actions(&mut conn, None, nz(10), nz(10))
        .await
        .unwrap();
    assert_eq!(all.len(), 2);
    let older = store::admin_actions(&mut conn, Some(all[0].id), nz(10), nz(10))
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
        &ErrorFilter::Exact("x".into()),
        "ops",
        "why",
        store::BULK_BUDGET,
    )
    .await
    .unwrap();
    assert_eq!(out.affected, 1);
    let a = store::admin_actions(&mut conn, None, nz(10), nz(10))
        .await
        .unwrap();
    assert_eq!(a.len(), 1);
    let AdminActionTarget::Bulk {
        action: BulkAction::Replay { error },
        task_name,
        ..
    } = &a[0].target
    else {
        panic!("not a bulk replay: {:?}", a[0].target)
    };
    assert_eq!(error, &ErrorFilter::Exact("x".into()));
    assert_eq!(task_name.as_str(), "email");
    db.cleanup().await;
}
