mod common;

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use common::TestDb;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::deadpool::Pool;
use pg_task_scheduler::store::{
    self, QueueSlots, WorkerQueueView, WorkerRegistration, WorkerStatus, WorkerView,
};
use pg_task_scheduler::{
    EnqueueOptions, HealthStatus, QueueName, Scheduler, SchedulerBuilder, Task, TaskId, TaskKey,
    TaskStatus, WorkerId, enqueue,
};
use serde_json::json;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

struct Job;
impl Task for Job {
    const NAME: &'static str = "job";
    type Args = serde_json::Value;
}

struct Report;
impl Task for Report {
    const NAME: &'static str = "report";
    const QUEUE: &'static str = "bulk";
    type Args = serde_json::Value;
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn n(v: usize) -> NonZeroUsize {
    NonZeroUsize::new(v).unwrap()
}

fn q(name: &str) -> QueueName {
    QueueName::try_from(name).unwrap()
}

/// A worker serving `default`/2 and `bulk`/1, with a `report` handler and the given `job`.
fn worker<F, Fut>(db: &TestDb, job: F) -> SchedulerBuilder<Pool<AsyncPgConnection>>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    Scheduler::builder(db.pool.clone())
        .max_poll_interval(ms(50))
        .queue("default", n(2))
        .unwrap()
        .queue("bulk", n(1))
        .unwrap()
        .register_task::<Job, _, _>(move |_ctx, _a| {
            let run = job();
            async move {
                run.await;
                Ok(())
            }
        })
        .unwrap()
        .register_task::<Report, _, _>(|_ctx, _a| async { Ok(()) })
        .unwrap()
}

async fn add(db: &TestDb, key: &str) -> TaskId {
    let mut conn = db.pool.get().await.unwrap();
    enqueue::<Job>(
        &mut conn,
        TaskKey::try_from(key).unwrap(),
        json!({}),
        EnqueueOptions::immediate(),
    )
    .await
    .unwrap()
    .id
}

async fn workers(db: &TestDb) -> Vec<WorkerView> {
    let mut conn = db.pool.get().await.unwrap();
    store::workers(&mut conn).await.unwrap()
}

/// Poll `store::workers` until `check` holds, for at most `within`.
async fn eventually(
    db: &TestDb,
    within: Duration,
    check: impl Fn(&[WorkerView]) -> bool,
) -> Vec<WorkerView> {
    let deadline = Instant::now() + within;
    loop {
        let rows = workers(db).await;
        if check(&rows) {
            return rows;
        }
        assert!(
            Instant::now() < deadline,
            "registry never matched: {rows:?}"
        );
        tokio::time::sleep(ms(20)).await;
    }
}

async fn done(db: &TestDb, id: TaskId) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut conn = db.pool.get().await.unwrap();
        let task = store::task(&mut conn, id).await.unwrap().unwrap();
        if task.status == TaskStatus::Done {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "task {id:?} not done: {:?}",
            task.status
        );
        tokio::time::sleep(ms(20)).await;
    }
}

#[derive(diesel::QueryableByName)]
struct Now {
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    now: DateTime<Utc>,
}

async fn db_now(db: &TestDb) -> DateTime<Utc> {
    let mut conn = db.pool.get().await.unwrap();
    diesel::sql_query("SELECT now()")
        .get_result::<Now>(&mut conn)
        .await
        .unwrap()
        .now
}

fn queue(name: &str, limit: i64) -> WorkerQueueView {
    WorkerQueueView {
        name: q(name),
        limit,
        in_use: 0,
        paused: false,
        last_finished_at: None,
    }
}

#[tokio::test]
async fn worker_registers_queues_and_task_names() {
    let db = TestDb::new().await;
    let scheduler = worker(&db, || async {}).label("w1").build().unwrap();
    let id = scheduler.worker_id().clone();
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(scheduler.run_until_shutdown(cancel.clone()));

    let rows = eventually(&db, Duration::from_secs(1), |rows| !rows.is_empty()).await;
    let w = &rows[0];
    assert_eq!(w.worker_id, id);
    assert_eq!(w.label.as_deref(), Some("w1"));
    assert!(
        matches!(w.status, WorkerStatus::Healthy | WorkerStatus::Starting),
        "{:?}",
        w.status
    );
    assert_eq!(w.queues, vec![queue("default", 2), queue("bulk", 1)]);
    let mut names = w.task_names.clone();
    names.sort();
    assert_eq!(names, ["job", "report"]);
    assert!(!w.lost);

    cancel.cancel();
    handle.await.unwrap().unwrap();
    db.cleanup().await;
}

#[tokio::test]
async fn registry_reports_last_finished_and_paused() {
    let db = TestDb::new().await;
    let scheduler = worker(&db, || async {})
        .registry_interval(ms(200))
        .build()
        .unwrap();
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(scheduler.run_until_shutdown(cancel.clone()));

    done(&db, add(&db, "a").await).await;
    let mut conn = db.pool.get().await.unwrap();
    store::pause_queue(&mut conn, &q("bulk"), "op", "why")
        .await
        .unwrap();
    drop(conn);

    let rows = eventually(&db, Duration::from_secs(1), |rows| {
        rows.first()
            .is_some_and(|w| w.queues[0].last_finished_at.is_some() && w.queues[1].paused)
    })
    .await;
    let finished = rows[0].queues[0].last_finished_at.unwrap();
    let skew = (db_now(&db).await - finished).abs();
    assert!(
        skew < chrono::Duration::seconds(5),
        "{finished} is {skew} from now()"
    );
    assert!(!rows[0].queues[0].paused);
    assert_eq!(rows[0].queues[1].last_finished_at, None);

    cancel.cancel();
    handle.await.unwrap().unwrap();
    db.cleanup().await;
}

#[tokio::test]
async fn worker_row_stopping_during_drain_then_removed() {
    let db = TestDb::new().await;
    let (started, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let (s, r) = (started.clone(), release.clone());
    let scheduler = worker(&db, move || {
        let (s, r) = (s.clone(), r.clone());
        async move {
            s.notify_one();
            r.notified().await; // ignores ctx.cancellation: the drain waits for it
        }
    })
    .build()
    .unwrap();
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(scheduler.run_until_shutdown(cancel.clone()));
    add(&db, "a").await;
    tokio::time::timeout(Duration::from_secs(10), started.notified())
        .await
        .expect("handler should start");

    cancel.cancel();
    let rows = eventually(&db, Duration::from_secs(1), |rows| {
        rows.first()
            .is_some_and(|w| w.status == WorkerStatus::Stopping)
    })
    .await;
    assert_eq!(rows[0].queues[0].in_use, 1, "drain still running");

    release.notify_one();
    handle.await.unwrap().unwrap();
    assert!(workers(&db).await.is_empty());
    db.cleanup().await;
}

#[tokio::test]
async fn lost_and_pruned_workers() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let live = WorkerRegistration {
        worker_id: WorkerId::try_from("live").unwrap(),
        label: Some("z".into()),
        status: WorkerStatus::Healthy,
        last_error: None,
        queues: vec![QueueSlots {
            name: q("default"),
            limit: 4,
            in_use: 1,
            finished_ago: Some(Duration::from_secs(3)),
        }],
        task_names: vec!["job".into()],
        lost_after: Duration::from_secs(45),
    };
    store::upsert_worker(&mut conn, &live).await.unwrap();
    diesel::sql_query(
        "INSERT INTO queue_workers \
             (worker_id, label, last_seen_at, lost_after, status, queues, task_names) \
         VALUES ('quiet', 'a', now() - interval '1 minute', now() - interval '1 second', \
                 'healthy', '[]', '{}'), \
                ('gone', 'b', now() - interval '25 hours', now() - interval '24 hours', \
                 'stopping', '[]', '{}')",
    )
    .execute(&mut conn)
    .await
    .unwrap();

    let rows = store::workers(&mut conn).await.unwrap();
    let seen: Vec<_> = rows
        .iter()
        .map(|w| (w.worker_id.as_str(), w.lost))
        .collect();
    assert_eq!(seen, [("live", false), ("quiet", true), ("gone", true)]);
    assert_eq!(rows[0].queues[0].in_use, 1);
    assert_eq!(rows[0].queues[0].limit, 4);
    assert!(rows[0].queues[0].last_finished_at.is_some());

    assert_eq!(store::prune_workers(&mut conn).await.unwrap(), 1);
    let left: Vec<_> = store::workers(&mut conn)
        .await
        .unwrap()
        .into_iter()
        .map(|w| w.worker_id)
        .collect();
    assert_eq!(
        left,
        [
            WorkerId::try_from("live").unwrap(),
            WorkerId::try_from("quiet").unwrap()
        ]
    );

    store::remove_worker(&mut conn, &live.worker_id)
        .await
        .unwrap();
    assert_eq!(store::workers(&mut conn).await.unwrap().len(), 1);
    drop(conn);
    db.cleanup().await;
}

#[tokio::test]
async fn registry_failure_does_not_stop_claiming() {
    let db = TestDb::new().await;
    let scheduler = worker(&db, || async {})
        .registry_interval(ms(50))
        .reaper_interval(ms(50))
        .build()
        .unwrap();
    let health = scheduler.health();
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(scheduler.run_until_shutdown(cancel.clone()));
    eventually(&db, Duration::from_secs(1), |rows| !rows.is_empty()).await;

    let mut conn = db.pool.get().await.unwrap();
    diesel::sql_query("DROP TABLE queue_workers")
        .execute(&mut conn)
        .await
        .unwrap();
    drop(conn);
    tokio::time::sleep(ms(200)).await; // several failed registry writes and prunes
    for key in ["a", "b", "c"] {
        done(&db, add(&db, key).await).await;
    }
    assert_eq!(health.borrow().status, HealthStatus::Healthy);

    cancel.cancel();
    handle.await.unwrap().unwrap(); // the failed remove is logged, not returned
    db.cleanup().await;
}
