use diesel_derive_newtype::DieselNewType;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Maximum length (in `char`s) of a [`JobName`] or [`WorkerId`]. Mirrored by the
/// SQL CHECK on `scheduler_jobs.name` and `scheduler_run_leases.worker_id`.
const MAX_IDENTIFIER_LEN: usize = 255;

/// Why a string was rejected as a [`JobName`] or [`WorkerId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum IdentifierError {
    #[error("identifier must not be empty")]
    Empty,
    #[error("identifier must not have leading or trailing whitespace")]
    SurroundingWhitespace,
    // The literal "255" must track `MAX_IDENTIFIER_LEN` (thiserror cannot
    // interpolate a const into the `#[error(..)]` message).
    #[error("identifier must be at most 255 characters")]
    TooLong,
}

/// Absurd conversion so `JobName` itself satisfies the `register` bound
/// (`TryFrom<JobName> for JobName` has `Error = Infallible`).
impl From<std::convert::Infallible> for IdentifierError {
    fn from(never: std::convert::Infallible) -> Self {
        match never {}
    }
}

/// The crate-owned identifier policy for [`JobName`] and [`WorkerId`].
///
/// The SQL CHECKs on `scheduler_jobs.name` / `scheduler_run_leases.worker_id`
/// mirror the non-empty and length rules exactly, and the surrounding-whitespace
/// rule for ASCII whitespace (POSIX `[[:space:]]`). They do NOT reject the exotic
/// Unicode whitespace (e.g. NBSP U+00A0, NEL U+0085, NNBSP U+202F) that `str::trim`
/// rejects here. So a row written directly to the database with such padding can be
/// loaded via `DieselNewType`'s `FromSql` without re-validation — an accepted
/// approximation: the scheduler only ever writes validated identifiers, and a padded
/// name simply fails to match any registered handler.
fn validate_identifier(s: &str) -> Result<(), IdentifierError> {
    if s.is_empty() {
        return Err(IdentifierError::Empty);
    }
    if s.trim() != s {
        return Err(IdentifierError::SurroundingWhitespace);
    }
    if s.chars().count() > MAX_IDENTIFIER_LEN {
        return Err(IdentifierError::TooLong);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, DieselNewType)]
pub struct JobId(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, DieselNewType)]
pub struct RunId(pub Uuid);

/// Queue-oriented name for [`RunId`]. Scheduled occurrences and immediate work
/// use the same durable identifier and execution path.
pub type TaskId = RunId;

/// Queue-oriented name for [`JobId`].
pub type ScheduleId = JobId;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, DieselNewType)]
#[serde(try_from = "String")]
pub struct JobName(String);

impl JobName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for JobName {
    type Error = IdentifierError;
    fn try_from(s: &str) -> Result<Self, IdentifierError> {
        validate_identifier(s)?;
        Ok(JobName(s.to_owned()))
    }
}

impl TryFrom<String> for JobName {
    type Error = IdentifierError;
    fn try_from(s: String) -> Result<Self, IdentifierError> {
        validate_identifier(&s)?;
        Ok(JobName(s))
    }
}

impl std::str::FromStr for JobName {
    type Err = IdentifierError;
    fn from_str(s: &str) -> Result<Self, IdentifierError> {
        JobName::try_from(s)
    }
}

/// Security-sensitive fencing token. Inner is private; construct via `generate`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, DieselNewType)]
pub struct LeaseToken(Uuid);

impl LeaseToken {
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, DieselNewType)]
#[serde(try_from = "String")]
pub struct WorkerId(String);

impl WorkerId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for WorkerId {
    type Error = IdentifierError;
    fn try_from(s: &str) -> Result<Self, IdentifierError> {
        validate_identifier(s)?;
        Ok(WorkerId(s.to_owned()))
    }
}

impl TryFrom<String> for WorkerId {
    type Error = IdentifierError;
    fn try_from(s: String) -> Result<Self, IdentifierError> {
        validate_identifier(&s)?;
        Ok(WorkerId(s))
    }
}

impl std::str::FromStr for WorkerId {
    type Err = IdentifierError;
    fn from_str(s: &str) -> Result<Self, IdentifierError> {
        WorkerId::try_from(s)
    }
}

/// Optional producer-supplied idempotency key. Uniqueness is scoped to the task
/// name in PostgreSQL.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, DieselNewType)]
#[serde(try_from = "String")]
pub struct DeduplicationKey(String);

impl DeduplicationKey {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for DeduplicationKey {
    type Error = IdentifierError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        validate_identifier(value)?;
        Ok(Self(value.to_owned()))
    }
}

impl TryFrom<String> for DeduplicationKey {
    type Error = IdentifierError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_identifier(&value)?;
        Ok(Self(value))
    }
}

#[cfg(test)]
mod identifier_tests {
    use super::{IdentifierError, JobName, WorkerId};
    use std::str::FromStr;

    #[test]
    fn empty_is_rejected() {
        assert_eq!(JobName::try_from(""), Err(IdentifierError::Empty));
        assert_eq!(WorkerId::try_from(""), Err(IdentifierError::Empty));
    }

    #[test]
    fn whitespace_only_is_surrounding_whitespace() {
        assert_eq!(
            JobName::try_from("  "),
            Err(IdentifierError::SurroundingWhitespace)
        );
    }

    #[test]
    fn leading_or_trailing_whitespace_is_rejected() {
        assert_eq!(
            JobName::try_from(" x"),
            Err(IdentifierError::SurroundingWhitespace)
        );
        assert_eq!(
            JobName::try_from("x "),
            Err(IdentifierError::SurroundingWhitespace)
        );
        assert_eq!(
            WorkerId::try_from(" x"),
            Err(IdentifierError::SurroundingWhitespace)
        );
    }

    #[test]
    fn over_max_length_is_too_long() {
        let s = "x".repeat(256);
        assert_eq!(JobName::try_from(s.as_str()), Err(IdentifierError::TooLong));
        assert_eq!(WorkerId::try_from(s), Err(IdentifierError::TooLong));
    }

    #[test]
    fn at_max_length_is_accepted() {
        let s = "x".repeat(255);
        assert!(JobName::try_from(s.as_str()).is_ok());
        assert!(WorkerId::try_from(s).is_ok());
    }

    #[test]
    fn length_counts_chars_not_bytes() {
        let s = "é".repeat(255); // 255 chars, 510 bytes
        let name = JobName::try_from(s.as_str()).expect("255 chars is valid");
        assert_eq!(name.as_str(), s);
    }

    #[test]
    fn valid_round_trips_via_as_str() {
        let name = JobName::try_from("my-job").unwrap();
        assert_eq!(name.as_str(), "my-job");
        assert!(JobName::try_from("my job").is_ok()); // interior spaces allowed
    }

    #[test]
    fn from_str_delegates_to_try_from() {
        assert!(JobName::from_str("ok").is_ok());
        assert_eq!(JobName::from_str(""), Err(IdentifierError::Empty));
        assert!(WorkerId::from_str("ok").is_ok());
    }

    #[test]
    fn deserialize_validates() {
        assert!(serde_json::from_str::<JobName>("\"ok\"").is_ok());
        assert!(serde_json::from_str::<JobName>("\"\"").is_err());
        assert!(serde_json::from_str::<WorkerId>("\" x\"").is_err());
    }
}
