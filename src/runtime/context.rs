use crate::ids::{JobId, JobName, LeaseToken, RunId};
use chrono::{DateTime, Utc};
use std::num::NonZeroU32;

#[derive(Debug, Clone)]
pub struct JobContext {
    pub run_id: RunId,
    pub job_id: Option<JobId>,
    pub job_name: JobName,
    pub scheduled_for: DateTime<Utc>,
    pub attempt: NonZeroU32,
    pub lease_token: LeaseToken,
    pub lease_expires_at: DateTime<Utc>,
    /// Cancelled when lease renewal proves that this worker no longer owns the
    /// task, or when scheduler shutdown aborts the handler.
    pub cancellation: tokio_util::sync::CancellationToken,
}
