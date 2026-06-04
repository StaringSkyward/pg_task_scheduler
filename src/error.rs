//! Crate error types.

#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    #[error("database error: {0}")]
    Database(#[from] diesel::result::Error),
    #[error("connection pool error: {0}")]
    Pool(String),
    #[error("cron parse error: {0}")]
    Cron(String),
    #[error("configuration error: {0}")]
    Config(String),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    /// A structural guarantee was violated (should be impossible); surfaced loudly.
    #[error("invariant violated: {0}")]
    Invariant(&'static str),
}

/// Opaque handler error. Deliberately does NOT implement `std::error::Error`
/// (same trick as `anyhow::Error`) so the blanket `From<E: Error>` below is
/// coherent — if `JobError` were itself an `Error`, that blanket would collide
/// with the std reflexive `From<JobError> for JobError`. Its `Display` is what
/// gets stored in `scheduler_run_outcomes.last_error`.
pub struct JobError(Box<dyn std::error::Error + Send + Sync>);

impl JobError {
    pub fn msg(message: impl Into<String>) -> Self {
        JobError(message.into().into())
    }
}

impl std::fmt::Debug for JobError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

impl std::fmt::Display for JobError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl<E: std::error::Error + Send + Sync + 'static> From<E> for JobError {
    fn from(e: E) -> Self {
        JobError(Box::new(e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_error_from_std_error() {
        let serde_err = serde_json::from_str::<i32>("x").unwrap_err();
        let job_err = JobError::from(serde_err);
        // Should produce a non-empty display string from the inner error.
        assert!(!job_err.to_string().is_empty());
    }

    #[test]
    fn job_error_msg_display() {
        let job_err = JobError::msg("e");
        assert_eq!(job_err.to_string(), "e");
    }
}
