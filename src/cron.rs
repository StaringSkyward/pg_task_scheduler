use chrono::{DateTime, Utc};

use crate::error::{CorruptJobRow, SchedulerError};
use crate::ids::JobId;

/// A five-field cron expression validated at construction (parse, don't validate).
#[derive(Debug, Clone)]
pub struct CronExpression {
    source: String,
    schedule: croner::Cron, // croner::Cron is Clone in 2.x
}

impl CronExpression {
    pub fn parse(source: impl Into<String>) -> Result<Self, SchedulerError> {
        Self::parse_inner(source.into()).map_err(SchedulerError::Cron)
    }

    pub fn as_str(&self) -> &str {
        &self.source
    }

    /// Parse a cron string read back from storage, attributing a parse failure to
    /// the owning job as [`SchedulerError::CorruptJob`] rather than a bare
    /// [`SchedulerError::Cron`]. Used by the `Job` projection and `jobs::reschedule`.
    pub(crate) fn parse_stored(job_id: JobId, source: &str) -> Result<Self, SchedulerError> {
        Self::parse_inner(source.to_owned()).map_err(|detail| SchedulerError::CorruptJob {
            job_id,
            source: CorruptJobRow::Cron(detail),
        })
    }

    /// Shared parse core. On failure returns the formatted detail
    /// (`"<source>": <croner error>`), which each public constructor wraps in its
    /// own error variant — so the detail is never double-prefixed by passing through
    /// another error type's `Display`.
    fn parse_inner(source: String) -> Result<Self, String> {
        let schedule = croner::Cron::new(&source)
            .parse()
            .map_err(|e| format!("{source:?}: {e}"))?;
        Ok(Self { source, schedule })
    }

    /// Next occurrence strictly after `after` (run_once misfire semantics).
    pub fn next_after(&self, after: DateTime<Utc>) -> Result<DateTime<Utc>, SchedulerError> {
        self.schedule
            .find_next_occurrence(&after, false)
            .map_err(|e| SchedulerError::Cron(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn utc(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0).unwrap()
    }

    #[test]
    fn next_is_strictly_after() {
        let c = CronExpression::parse("17 * * * *").unwrap();
        assert_eq!(
            c.next_after(utc(2026, 1, 1, 10, 17)).unwrap(),
            utc(2026, 1, 1, 11, 17)
        );
    }
    #[test]
    fn rolls_forward_within_hour() {
        let c = CronExpression::parse("17 * * * *").unwrap();
        assert_eq!(
            c.next_after(utc(2026, 1, 1, 10, 0)).unwrap(),
            utc(2026, 1, 1, 10, 17)
        );
    }
    #[test]
    fn daily_midnight() {
        let c = CronExpression::parse("0 0 * * *").unwrap();
        assert_eq!(
            c.next_after(utc(2026, 3, 14, 12, 0)).unwrap(),
            utc(2026, 3, 15, 0, 0)
        );
    }
    #[test]
    fn sunday_0_equals_7() {
        let z = CronExpression::parse("0 0 * * 0").unwrap();
        let s = CronExpression::parse("0 0 * * 7").unwrap();
        let from = utc(2026, 1, 1, 0, 0);
        assert_eq!(z.next_after(from).unwrap(), s.next_after(from).unwrap());
    }
    #[test]
    fn rejects_garbage() {
        assert!(CronExpression::parse("not a cron").is_err());
    }
    #[test]
    fn preserves_source() {
        assert_eq!(
            CronExpression::parse("17 * * * *").unwrap().as_str(),
            "17 * * * *"
        );
    }

    #[test]
    fn parse_stored_maps_garbage_to_corrupt_job() {
        let id = JobId(uuid::Uuid::nil());
        let err = CronExpression::parse_stored(id, "not a cron").unwrap_err();
        assert!(matches!(
            err,
            SchedulerError::CorruptJob { source: CorruptJobRow::Cron(_), .. }
        ));
    }

    #[test]
    fn parse_stored_accepts_valid() {
        let id = JobId(uuid::Uuid::nil());
        assert!(CronExpression::parse_stored(id, "17 * * * *").is_ok());
    }
}
