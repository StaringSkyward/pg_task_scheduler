//! Its own test binary: the worker test installs a local metrics recorder.
mod common;

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use common::TestDb;
use diesel_async::AsyncPgConnection;
use diesel_async::SimpleAsyncConnection;
use pg_task_scheduler::AttemptOutcome;
use pg_task_scheduler::ClaimedTask;
use pg_task_scheduler::EnqueueOptions;
use pg_task_scheduler::QueueName;
use pg_task_scheduler::Scheduler;
use pg_task_scheduler::SchedulerError;
use pg_task_scheduler::Task;
use pg_task_scheduler::TaskId;
use pg_task_scheduler::TaskKey;
use pg_task_scheduler::TaskStatus;
use pg_task_scheduler::WorkerId;
use pg_task_scheduler::enqueue;
use pg_task_scheduler::store::AdminActionKind;
use pg_task_scheduler::store::CancelOutcome;
use pg_task_scheduler::store::{self};
use serde_json::json;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

struct Job;
impl Task for Job {
    const NAME: &'static str = "job";
    type Args = serde_json::Value;
}

async fn enqueue_job(conn: &mut AsyncPgConnection, key: &str) -> TaskId {
    enqueue::<Job>(
        conn,
        TaskKey::try_from(key).unwrap(),
        json!({}),
        EnqueueOptions::immediate(),
    )
    .await
    .unwrap()
    .id
}

async fn claim_one(conn: &mut AsyncPgConnection) -> ClaimedTask {
    store::claim(
        conn,
        &WorkerId::generate(),
        &QueueName::default(),
        &["job".to_owned()],
        NonZeroUsize::new(1).unwrap(),
        Duration::from_secs(60),
    )
    .await
    .unwrap()
    .pop()
    .unwrap()
}

/// Insert a task already in `status` (terminal statuses need `completed_at`).
async fn insert_with_status(db: &TestDb, key: &str, status: &str) -> TaskId {
    let mut conn = db.pool.get().await.unwrap();
    conn.batch_execute(&format!(
        "INSERT INTO queue_tasks (task_key, task_name, status, completed_at) \
         VALUES ('{key}', 'job', '{status}', now())"
    ))
    .await
    .unwrap();
    TaskId(
        db.scalar(&format!(
            "SELECT id AS n FROM queue_tasks WHERE task_key = '{key}'"
        ))
        .await,
    )
}

#[tokio::test]
async fn cancel_queued_task() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let id = enqueue_job(&mut conn, "k").await;

    let outcome = store::cancel(&mut conn, id, "ops", "not needed")
        .await
        .unwrap();
    assert_eq!(outcome, CancelOutcome::Cancelled);

    let t = store::task(&mut conn, id).await.unwrap().unwrap();
    assert_eq!(t.status, TaskStatus::Cancelled);
    assert!(t.completed_at.is_some());
    let actions = store::admin_actions(&mut conn, None, 10).await.unwrap();
    assert_eq!(actions.len(), 1);
    let a = &actions[0];
    assert_eq!(a.action, AdminActionKind::Cancel);
    assert_eq!(a.task_id, Some(id));
    assert_eq!(a.task_name.as_ref().map(|n| n.as_str()), Some("job"));
    assert_eq!(a.queue, Some(QueueName::default()));
    assert_eq!(a.affected, 1);
    assert_eq!(
        (a.operator.as_str(), a.reason.as_str()),
        ("ops", "not needed")
    );
    assert_eq!(t.admin_action_id, Some(a.id));
    drop(conn);
    db.cleanup().await;
}

#[tokio::test]
async fn cancel_running_task_closes_attempt_and_fences_worker() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let id = enqueue_job(&mut conn, "k").await;
    let claimed = claim_one(&mut conn).await;

    let outcome = store::cancel(&mut conn, id, "ops", "stop it")
        .await
        .unwrap();
    assert_eq!(outcome, CancelOutcome::Cancelled);

    let t = store::task(&mut conn, id).await.unwrap().unwrap();
    assert_eq!(t.status, TaskStatus::Cancelled);
    assert_eq!((t.locked_by, t.locked_until), (None, None));
    let attempts = store::attempts(&mut conn, id).await.unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].outcome, Some(AttemptOutcome::Cancelled));
    assert_eq!(attempts[0].error.as_deref(), Some("cancelled by operator"));
    assert!(attempts[0].finished_at.is_some());

    let beat = store::heartbeat(&mut conn, &claimed.lease, Duration::from_secs(60)).await;
    assert!(
        matches!(beat, Err(SchedulerError::LostOwnership(t)) if t == id),
        "{beat:?}"
    );
    assert_eq!(
        store::lease_status(&mut conn, &claimed.lease)
            .await
            .unwrap(),
        Some(TaskStatus::Cancelled)
    );
    drop(conn);
    db.cleanup().await;
}

#[tokio::test]
async fn lease_status_is_none_for_another_token() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    enqueue_job(&mut conn, "k").await;
    let mut lease = claim_one(&mut conn).await.lease;
    assert_eq!(
        store::lease_status(&mut conn, &lease).await.unwrap(),
        Some(TaskStatus::Running)
    );
    lease.lock_token.0 += 1;
    assert_eq!(store::lease_status(&mut conn, &lease).await.unwrap(), None);
    drop(conn);
    db.cleanup().await;
}

#[tokio::test]
async fn cancel_terminal_or_missing_is_not_cancellable() {
    let db = TestDb::new().await;
    let mut ids = Vec::new();
    for status in ["done", "dead", "cancelled"] {
        ids.push(insert_with_status(&db, status, status).await);
    }
    ids.push(TaskId(999_999));
    let mut conn = db.pool.get().await.unwrap();
    let started = Instant::now();
    for id in ids {
        let outcome = store::cancel(&mut conn, id, "ops", "r").await.unwrap();
        assert_eq!(outcome, CancelOutcome::NotCancellable, "{id:?}");
    }
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "waited on a terminal row"
    );
    assert_eq!(
        db.scalar("SELECT count(*) AS n FROM queue_admin_actions")
            .await,
        0
    );
    drop(conn);
    db.cleanup().await;
}

#[tokio::test]
async fn cancel_locked_row_is_busy() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let id = enqueue_job(&mut conn, "k").await;
    let mut locker = db.pool.get().await.unwrap();
    locker
        .batch_execute(&format!(
            "BEGIN; SELECT id FROM queue_tasks WHERE id = {} FOR UPDATE",
            id.0
        ))
        .await
        .unwrap();

    let started = Instant::now();
    let outcome = store::cancel(&mut conn, id, "ops", "r").await.unwrap();
    let waited = started.elapsed();
    assert_eq!(outcome, CancelOutcome::Busy);
    assert!(waited < Duration::from_secs(3), "waited {waited:?}");

    let t = store::task(&mut conn, id).await.unwrap().unwrap();
    assert_eq!(t.status, TaskStatus::Queued);
    assert_eq!((t.completed_at, t.admin_action_id), (None, None));
    assert_eq!(
        db.scalar("SELECT count(*) AS n FROM queue_admin_actions")
            .await,
        0
    );
    locker.batch_execute("ROLLBACK").await.unwrap();
    drop((conn, locker));
    db.cleanup().await;
}

/// Counts every counter increment by metric name. A local recorder sees the worker:
/// `#[tokio::test]` runs every task on the test thread.
#[derive(Clone, Default)]
struct Counters(Arc<Mutex<HashMap<String, u64>>>);

impl Counters {
    fn get(&self, name: &str) -> u64 {
        self.0.lock().unwrap().get(name).copied().unwrap_or(0)
    }
}

struct NamedCounter(String, Counters);

impl metrics::CounterFn for NamedCounter {
    fn increment(&self, value: u64) {
        *self.1.0.lock().unwrap().entry(self.0.clone()).or_default() += value;
    }
    fn absolute(&self, _: u64) {}
}

impl metrics::Recorder for Counters {
    fn describe_counter(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_gauge(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_histogram(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        metrics::Counter::from_arc(Arc::new(NamedCounter(key.name().to_owned(), self.clone())))
    }
    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }
    fn register_histogram(
        &self,
        _: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

/// A cancel fences the running worker: its next heartbeat matches no row, `ctx.cancellation`
/// fires, and the loss is counted as a cancellation, not a lost lease.
#[tokio::test]
async fn worker_drops_cancelled_handler_and_counts_cancellation() {
    let counters = Counters::default();
    let _recorder = metrics::set_default_local_recorder(&counters);
    let db = TestDb::new().await;
    let id = enqueue_job(&mut db.pool.get().await.unwrap(), "k").await;

    let (started, cancelled) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let (started_h, cancelled_h) = (started.clone(), cancelled.clone());
    let scheduler = Scheduler::builder(db.pool.clone())
        .lease_duration(Duration::from_secs(3))
        .heartbeat_interval(Duration::from_secs(1))
        .register_task::<Job, _, _>(move |ctx, _args| {
            let (started, cancelled) = (started_h.clone(), cancelled_h.clone());
            async move {
                // The handler future is dropped before `ctx.cancellation` fires, so a
                // separate task watches the token.
                let token = ctx.cancellation.clone();
                tokio::spawn(async move {
                    token.cancelled().await;
                    cancelled.notify_one();
                });
                started.notify_one();
                std::future::pending().await
            }
        })
        .unwrap()
        .build()
        .unwrap();
    let stop = CancellationToken::new();
    let run = tokio::spawn(scheduler.run_until_shutdown(stop.clone()));
    tokio::time::timeout(Duration::from_secs(10), started.notified())
        .await
        .expect("handler should start within 10 s");

    let mut conn = db.pool.get().await.unwrap();
    let outcome = store::cancel(&mut conn, id, "ops", "r").await.unwrap();
    assert_eq!(outcome, CancelOutcome::Cancelled);
    drop(conn);
    tokio::time::timeout(Duration::from_secs(3), cancelled.notified())
        .await
        .expect("ctx.cancellation should fire within 3 s of the cancel");

    // Shutdown drains the dispatch, so its post-loss check has been counted.
    stop.cancel();
    run.await.unwrap().unwrap();
    assert_eq!(counters.get("pg_task_scheduler_tasks_cancelled_total"), 1);
    assert_eq!(counters.get("pg_task_scheduler_leases_lost_total"), 0);
    db.cleanup().await;
}

/// A handler that returns after the cancel but before the next heartbeat gets `LostOwnership`
/// on completion; that too counts as a cancellation, not as done.
#[tokio::test]
async fn completion_after_cancel_counts_cancellation() {
    let counters = Counters::default();
    let _recorder = metrics::set_default_local_recorder(&counters);
    let db = TestDb::new().await;
    let id = enqueue_job(&mut db.pool.get().await.unwrap(), "k").await;

    let (started, go) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let (started_h, go_h) = (started.clone(), go.clone());
    // No heartbeat within the test: the handler's completion is what meets the cancel.
    let scheduler = Scheduler::builder(db.pool.clone())
        .lease_duration(Duration::from_secs(300))
        .heartbeat_interval(Duration::from_secs(100))
        .register_task::<Job, _, _>(move |_ctx, _args| {
            let (started, go) = (started_h.clone(), go_h.clone());
            async move {
                started.notify_one();
                go.notified().await;
                Ok(())
            }
        })
        .unwrap()
        .build()
        .unwrap();
    let stop = CancellationToken::new();
    let run = tokio::spawn(scheduler.run_until_shutdown(stop.clone()));
    tokio::time::timeout(Duration::from_secs(10), started.notified())
        .await
        .expect("handler should start within 10 s");

    let mut conn = db.pool.get().await.unwrap();
    let outcome = store::cancel(&mut conn, id, "ops", "r").await.unwrap();
    assert_eq!(outcome, CancelOutcome::Cancelled);
    go.notify_one();
    // Shutdown drains the dispatch, so its completion has been attempted and counted.
    stop.cancel();
    run.await.unwrap().unwrap();

    assert_eq!(counters.get("pg_task_scheduler_tasks_cancelled_total"), 1);
    assert_eq!(counters.get("pg_task_scheduler_tasks_done_total"), 0);
    assert_eq!(counters.get("pg_task_scheduler_leases_lost_total"), 0);
    let t = store::task(&mut conn, id).await.unwrap().unwrap();
    assert_eq!(t.status, TaskStatus::Cancelled);
    drop(conn);
    db.cleanup().await;
}
