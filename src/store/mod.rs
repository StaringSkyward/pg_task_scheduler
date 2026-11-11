//! Store operations on the queue tables (added task by task).

mod admin;
mod browse;
mod claim;
mod inspect;
mod maintenance;
#[cfg(feature = "cron")]
mod materialize;
mod transitions;
mod workers;

#[doc(hidden)]
pub use admin::REPLAY_BATCH;
pub use admin::{
    AdminAction, AdminActionKind, BULK_BATCH, BULK_BUDGET, BulkFilter, BulkOutcome, CancelOutcome,
    QueuePause, admin_actions, cancel, cancel_matching, count_cancellable, count_replayable,
    pause_queue, paused_queues, replay_matching, resume_queue, task_actions,
};
#[doc(hidden)]
pub use browse::{DONE, FAILED, FAILED_BY_NAME, QUEUED_DUE, QUEUED_FUTURE, RUNNING};
pub use browse::{
    FailedFilter, FailedStatus, FinishedCursor, Page, QueuedCursor, RunningCursor, TaskDetail,
    TaskRow, done_page, failed_page, queued_page, running_page, task_detail,
};
pub use claim::{claim, next_due_in};
pub use inspect::{attempts, queue_stats, task};
#[doc(hidden)]
pub use maintenance::CLEANUP_CANCELLED;
pub use maintenance::{cleanup, reap_expired};
#[cfg(feature = "cron")]
pub use materialize::materialize_due_jobs;
pub use transitions::{
    complete, complete_with_outbox, completed_by, fail_permanently, heartbeat, lease_status,
    lock_owned, release, retry,
};
pub use workers::{
    QueueSlots, WorkerQueueView, WorkerRegistration, WorkerStatus, WorkerView, prune_workers,
    remove_worker, upsert_worker, workers,
};
