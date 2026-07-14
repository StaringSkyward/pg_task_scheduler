#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    Starting,
    Healthy,
    Degraded,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerHealth {
    pub status: HealthStatus,
    pub consecutive_failures: u64,
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
