use crate::ids::{JobId, JobName, LeaseToken, RunId};
use chrono::{DateTime, Utc};
use std::num::NonZeroU32;

#[derive(Debug, Clone)]
pub struct JobContext {
    pub run_id: RunId,
    pub job_id: JobId,
    pub job_name: JobName,
    pub scheduled_for: DateTime<Utc>,
    pub attempt: NonZeroU32,
    pub lease_token: LeaseToken,
    pub lease_expires_at: DateTime<Utc>,
}
