use diesel_derive_newtype::DieselNewType;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/// Maximum length (in `char`s) of a [`TaskName`], [`TaskKey`], [`QueueName`] or [`WorkerId`].
const MAX_IDENTIFIER_LEN: usize = 255;

/// Why a string was rejected as a [`TaskName`], [`TaskKey`], [`QueueName`] or
/// [`WorkerId`].
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

/// Absurd conversion so `TaskName` itself satisfies the `register` bound
/// (`TryFrom<TaskName> for TaskName` has `Error = Infallible`).
impl From<std::convert::Infallible> for IdentifierError {
    fn from(never: std::convert::Infallible) -> Self {
        match never {}
    }
}

/// The crate-owned identifier policy for [`TaskName`], [`TaskKey`], [`QueueName`] and [`WorkerId`].
///
/// Non-empty, no leading/trailing whitespace (per `str::trim`), at most 255 chars.
/// The queue tables carry no SQL CHECKs for this: only validated identifiers are
/// written by this crate, so a row edited directly in SQL can be loaded via
/// `DieselNewType`'s `FromSql` without re-validation.
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

/// Database identity of a task row (`queue_tasks.id`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, DieselNewType)]
pub struct TaskId(pub i64);

/// Fencing token of one claim (`queue_tasks.lock_token`); never repeats for a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, DieselNewType)]
pub struct LockToken(pub i64);

#[cfg(feature = "cron")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, DieselNewType)]
pub struct JobId(pub Uuid);

macro_rules! validated_string {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, DieselNewType)]
        #[serde(try_from = "String")]
        pub struct $name(String);

        impl $name {
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<&str> for $name {
            type Error = IdentifierError;
            fn try_from(s: &str) -> Result<Self, IdentifierError> {
                validate_identifier(s)?;
                Ok($name(s.to_owned()))
            }
        }

        impl TryFrom<String> for $name {
            type Error = IdentifierError;
            fn try_from(s: String) -> Result<Self, IdentifierError> {
                validate_identifier(&s)?;
                Ok($name(s))
            }
        }

        impl std::str::FromStr for $name {
            type Err = IdentifierError;
            fn from_str(s: &str) -> Result<Self, IdentifierError> {
                $name::try_from(s)
            }
        }
    };
}

validated_string!(
    /// Handler/type name of a task (`queue_tasks.task_name`).
    TaskName
);
validated_string!(
    /// Producer idempotency key of a task (`queue_tasks.task_key`).
    TaskKey
);
validated_string!(
    /// Identity of a worker process (`queue_tasks.locked_by`).
    WorkerId
);

validated_string!(
    /// Logical queue of a task (`queue_tasks.queue`).
    QueueName
);

impl Default for QueueName {
    fn default() -> Self {
        Self("default".to_owned())
    }
}

impl WorkerId {
    /// A fresh random identity (UUID v4 string).
    pub fn generate() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

#[cfg(test)]
mod identifier_tests {
    use std::str::FromStr;

    use super::IdentifierError;
    use super::QueueName;
    use super::TaskKey;
    use super::TaskName;
    use super::WorkerId;

    #[test]
    fn queue_name_defaults_to_default_and_rejects_empty() {
        assert_eq!(QueueName::default().as_str(), "default");
        assert_eq!(QueueName::try_from(""), Err(IdentifierError::Empty));
    }

    #[test]
    fn empty_is_rejected() {
        assert_eq!(TaskName::try_from(""), Err(IdentifierError::Empty));
        assert_eq!(WorkerId::try_from(""), Err(IdentifierError::Empty));
    }

    #[test]
    fn whitespace_only_is_surrounding_whitespace() {
        assert_eq!(
            TaskName::try_from("  "),
            Err(IdentifierError::SurroundingWhitespace)
        );
    }

    #[test]
    fn leading_or_trailing_whitespace_is_rejected() {
        assert_eq!(
            TaskName::try_from(" x"),
            Err(IdentifierError::SurroundingWhitespace)
        );
        assert_eq!(
            TaskName::try_from("x "),
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
        assert_eq!(
            TaskName::try_from(s.as_str()),
            Err(IdentifierError::TooLong)
        );
        assert_eq!(WorkerId::try_from(s), Err(IdentifierError::TooLong));
    }

    #[test]
    fn at_max_length_is_accepted() {
        let s = "x".repeat(255);
        assert!(TaskName::try_from(s.as_str()).is_ok());
        assert!(WorkerId::try_from(s).is_ok());
    }

    #[test]
    fn length_counts_chars_not_bytes() {
        let s = "é".repeat(255); // 255 chars, 510 bytes
        let name = TaskName::try_from(s.as_str()).expect("255 chars is valid");
        assert_eq!(name.as_str(), s);
    }

    #[test]
    fn valid_round_trips_via_as_str() {
        let name = TaskName::try_from("my-task").unwrap();
        assert_eq!(name.as_str(), "my-task");
        assert!(TaskName::try_from("my job").is_ok()); // interior spaces allowed
    }

    #[test]
    fn from_str_delegates_to_try_from() {
        assert!(TaskName::from_str("ok").is_ok());
        assert_eq!(TaskName::from_str(""), Err(IdentifierError::Empty));
        assert!(WorkerId::from_str("ok").is_ok());
    }

    #[test]
    fn task_key_and_generated_worker_id_are_valid() {
        assert!(TaskKey::try_from("k").is_ok());
        assert_eq!(TaskKey::try_from(""), Err(IdentifierError::Empty));
        let id = WorkerId::generate();
        assert!(WorkerId::try_from(id.as_str()).is_ok());
        assert_ne!(id, WorkerId::generate());
    }

    #[test]
    fn deserialize_validates() {
        assert!(serde_json::from_str::<TaskName>("\"ok\"").is_ok());
        assert!(serde_json::from_str::<TaskName>("\"\"").is_err());
        assert!(serde_json::from_str::<WorkerId>("\" x\"").is_err());
    }
}
