/// A scheduler's overall state, as reported by [`Scheduler::health`](crate::Scheduler::health).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    /// No claim cycle has finished yet.
    Starting,
    /// The last claim cycle succeeded.
    Healthy,
    /// The last claim cycle (or, with `cron`, materialization) failed.
    Degraded,
    /// `run_until_shutdown` returned, or its future was dropped.
    Stopped,
}

/// A snapshot of a scheduler's health.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerHealth {
    /// The overall state.
    pub status: HealthStatus,
    /// Failed cycles in a row; reset by a success.
    pub consecutive_failures: u64,
    /// The latest failure's message, cleared by a success.
    pub last_error: Option<String>,
}

impl Default for WorkerHealth {
    fn default() -> Self {
        Self {
            status: HealthStatus::Starting,
            consecutive_failures: 0,
            last_error: None,
        }
    }
}
