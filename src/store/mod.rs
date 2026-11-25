//! Statements on the queue tables: worker transitions (claim, heartbeat, complete,
//! retry, fail), maintenance, inspection, and the admin operations (cancel, replay,
//! pause, bulk actions). [`Scheduler`](crate::Scheduler) calls most of these for you;
//! call them directly for transactional completion, operations tooling or tests.

// The rustdoc paragraph every worker-owned transition but `lock_owned` shares, `dead_letter`
// included. Defined before the modules so `claim` and `transitions` both see it.
macro_rules! bounded_doc {
    () => {
        "It runs in a transaction, so don't cancel it mid-flight (`select!`, `timeout`): spawn it, \
         as the worker does. Before it locks the row, it bounds `lock_timeout`, `statement_timeout` \
         and `idle_in_transaction_session_timeout` by the lease left until its transaction ends \
         (the caller's, when called inside one), keeping a stricter non-zero host setting. Each \
         statement and each idle gap is therefore limited by the lease left at the call, so a \
         dropped call's session ends within two to three times that. An expired lease is \
         `LostOwnership` before any lock."
    };
}

mod admin;
mod browse;
mod claim;
mod inspect;
mod maintenance;
#[cfg(feature = "cron")]
mod materialize;
mod overview;
mod transitions;
mod workers;

pub use admin::AdminAction;
pub use admin::AdminActionKind;
pub use admin::AdminActionTarget;
pub use admin::BULK_BATCH;
pub use admin::BULK_BUDGET;
pub use admin::BulkAction;
pub use admin::BulkFilter;
pub use admin::BulkOutcome;
pub use admin::BulkStop;
#[doc(hidden)]
pub use admin::CANCEL_BATCH;
#[doc(hidden)]
pub use admin::COUNT_CANCELLABLE;
#[doc(hidden)]
pub use admin::COUNT_REPLAYABLE;
pub use admin::CancelOutcome;
pub use admin::ErrorFilter;
pub use admin::QueueAction;
pub use admin::QueuePause;
#[doc(hidden)]
pub use admin::REPLAY_BATCH;
pub use admin::TaskAction;
pub use admin::admin_action;
pub use admin::admin_actions;
pub use admin::cancel;
pub use admin::cancel_matching;
pub use admin::count_cancellable;
pub use admin::count_replayable;
pub use admin::pause_queue;
pub use admin::paused_queues;
pub use admin::replay_matching;
pub use admin::resume_bulk;
pub use admin::resume_queue;
pub use admin::task_actions;
#[doc(hidden)]
pub use browse::DONE;
#[doc(hidden)]
pub use browse::FAILED;
#[doc(hidden)]
pub use browse::FAILED_BY_NAME;
pub use browse::FailedFilter;
pub use browse::FailedStatus;
pub use browse::FinishedCursor;
pub use browse::Page;
#[doc(hidden)]
pub use browse::QUEUED_DUE;
#[doc(hidden)]
pub use browse::QUEUED_FUTURE;
pub use browse::QueuedCursor;
#[doc(hidden)]
pub use browse::RUNNING;
pub use browse::RunningCursor;
pub use browse::TaskDetail;
pub use browse::TaskRow;
pub use browse::done_page;
pub use browse::failed_page;
pub use browse::queued_page;
pub use browse::running_page;
pub use browse::task_detail;
pub use claim::ClaimBatch;
pub use claim::PoisonedClaim;
pub use claim::claim;
pub use claim::dead_letter;
pub use claim::next_due_in;
pub use inspect::attempts;
pub use inspect::queue_stats;
pub use inspect::task;
#[doc(hidden)]
pub use maintenance::CLEANUP_CANCELLED;
#[doc(hidden)]
pub use maintenance::REAP;
pub use maintenance::cleanup;
pub use maintenance::reap_expired;
#[cfg(feature = "cron")]
#[doc(hidden)]
pub use materialize::SELECT_DUE;
#[cfg(feature = "cron")]
pub use materialize::materialize_due_jobs;
pub use overview::Count;
#[doc(hidden)]
pub use overview::DEAD_GROUPS;
pub use overview::FailureGroup;
#[doc(hidden)]
pub use overview::NO_HANDLER;
pub use overview::NoHandler;
#[doc(hidden)]
pub use overview::OVERVIEW_COUNTS;
#[doc(hidden)]
pub use overview::OVERVIEW_DEAD;
#[doc(hidden)]
pub use overview::OVERVIEW_DONE;
#[doc(hidden)]
pub use overview::OVERVIEW_FAILED_ATTEMPTS;
#[doc(hidden)]
pub use overview::OVERVIEW_QUEUES;
#[doc(hidden)]
pub use overview::OVERVIEW_RUNNING;
pub use overview::Overview;
pub use overview::OverviewParams;
pub use overview::QueueOverview;
pub use overview::overview;
pub use transitions::complete;
pub use transitions::complete_with_outbox;
pub use transitions::completed_by;
pub use transitions::fail_permanently;
pub use transitions::heartbeat;
#[doc(hidden)]
pub use transitions::heartbeat_bounded;
pub use transitions::lease_status;
pub use transitions::lock_owned;
pub use transitions::release;
pub use transitions::retry;
pub use workers::QueueSlots;
pub use workers::WorkerQueueView;
pub use workers::WorkerRegistration;
pub use workers::WorkerStatus;
pub use workers::WorkerView;
pub use workers::prune_workers;
pub use workers::remove_worker;
pub use workers::upsert_worker;
pub use workers::workers;
