//! TOML config for the standalone server: parsing, validation, mapping to `AdminConfig`.
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use pg_task_scheduler::store::OverviewParams;
use pg_task_scheduler_admin::AdminConfig;
use pg_task_scheduler_admin::QueueThresholds;
use pg_task_scheduler_admin::Thresholds;
use pg_task_scheduler_admin::auth::BasicUser;
use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// Only the message and line, never the source text, which may hold a password hash.
    #[error("invalid config (line {line}): {message}")]
    Toml { line: usize, message: String },
    #[error("no database_url in the config and DATABASE_URL is not set")]
    NoDatabaseUrl,
    #[error("auth.mode = \"basic\" needs at least one [[auth.users]]")]
    NoUsers,
    #[error("duplicate user name {0:?} in [[auth.users]]")]
    DuplicateUser(String),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, default)]
struct Raw {
    database_url: Option<String>,
    bind: SocketAddr,
    pool_size: usize,
    base_path: String,
    public_origin: Option<String>,
    refresh_seconds: u64,
    overview_cache_seconds: u64,
    statement_timeout_seconds: u64,
    count_cap: i64,
    window_seconds: u64,
    thresholds: RawThresholds,
    #[serde(default = "missing_auth")]
    auth: RawAuth,
}

impl Default for Raw {
    fn default() -> Self {
        Self {
            database_url: None,
            bind: ([127, 0, 0, 1], 8080).into(),
            pool_size: 4,
            base_path: String::new(),
            public_origin: None,
            refresh_seconds: 10,
            overview_cache_seconds: 5,
            statement_timeout_seconds: 5,
            count_cap: 10_000,
            window_seconds: 3600,
            thresholds: RawThresholds::default(),
            auth: missing_auth(),
        }
    }
}

// Placeholder so `default` on Raw works; `auth` is checked for presence in `parse_with_env`.
fn missing_auth() -> RawAuth {
    RawAuth {
        mode: None,
        header: default_header(),
        users: vec![],
    }
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
struct RawThresholds {
    backlog_after_seconds: Option<u64>,
    stall_after_seconds: Option<u64>,
    queues: HashMap<String, RawQueueThresholds>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawQueueThresholds {
    backlog_after_seconds: Option<u64>,
    stall_after_seconds: Option<u64>,
}

#[derive(Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Mode {
    Basic,
    TrustedHeader,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAuth {
    mode: Option<Mode>,
    #[serde(default = "default_header")]
    header: String,
    #[serde(default)]
    users: Vec<RawUser>,
}

fn default_header() -> String {
    "X-Forwarded-User".into()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawUser {
    name: String,
    password_hash: String,
}

pub enum Auth {
    Basic(Vec<BasicUser>),
    TrustedHeader(String),
}

// No Debug: holds the database URL and password hashes.
pub struct StandaloneConfig {
    pub database_url: String,
    pub bind: SocketAddr,
    pub pool_size: usize,
    pub base_path: String,
    pub public_origin: Option<String>,
    pub refresh_seconds: u64,
    pub overview_cache_seconds: u64,
    pub statement_timeout_seconds: u64,
    pub count_cap: i64,
    pub window_seconds: u64,
    pub backlog_after_seconds: u64,
    pub stall_after_seconds: u64,
    pub queue_thresholds: HashMap<String, QueueThresholds>,
    pub auth: Auth,
}

pub fn parse(text: &str) -> Result<StandaloneConfig, ConfigError> {
    parse_with_env(text, std::env::var("DATABASE_URL").ok())
}

pub fn parse_with_env(text: &str, env: Option<String>) -> Result<StandaloneConfig, ConfigError> {
    let raw: Raw = toml::from_str(text).map_err(|e| ConfigError::Toml {
        line: e
            .span()
            .map_or(0, |s| text[..s.start].matches('\n').count() + 1),
        message: e.message().to_string(),
    })?;
    let database_url = raw.database_url.or(env).ok_or(ConfigError::NoDatabaseUrl)?;
    let a = raw.auth;
    let mode = a.mode.ok_or(ConfigError::Toml {
        line: 0,
        message: "missing field `auth.mode`".into(),
    })?;
    let auth = match mode {
        Mode::TrustedHeader => Auth::TrustedHeader(a.header),
        Mode::Basic => {
            if a.users.is_empty() {
                return Err(ConfigError::NoUsers);
            }
            let mut seen = std::collections::HashSet::new();
            for u in &a.users {
                if !seen.insert(&u.name) {
                    return Err(ConfigError::DuplicateUser(u.name.clone()));
                }
            }
            Auth::Basic(
                a.users
                    .into_iter()
                    .map(|u| BasicUser {
                        name: u.name,
                        password_hash: u.password_hash,
                    })
                    .collect(),
            )
        }
    };
    let d = QueueThresholds::default();
    let backlog = raw
        .thresholds
        .backlog_after_seconds
        .unwrap_or(d.backlog_after.as_secs());
    let stall = raw
        .thresholds
        .stall_after_seconds
        .unwrap_or(d.stall_after.as_secs());
    let queue_thresholds = raw
        .thresholds
        .queues
        .into_iter()
        .map(|(q, t)| {
            let t = QueueThresholds {
                backlog_after: Duration::from_secs(t.backlog_after_seconds.unwrap_or(backlog)),
                stall_after: Duration::from_secs(t.stall_after_seconds.unwrap_or(stall)),
            };
            (q, t)
        })
        .collect();
    Ok(StandaloneConfig {
        database_url,
        bind: raw.bind,
        pool_size: raw.pool_size,
        base_path: raw.base_path,
        public_origin: raw.public_origin,
        refresh_seconds: raw.refresh_seconds,
        overview_cache_seconds: raw.overview_cache_seconds,
        statement_timeout_seconds: raw.statement_timeout_seconds,
        count_cap: raw.count_cap,
        window_seconds: raw.window_seconds,
        backlog_after_seconds: backlog,
        stall_after_seconds: stall,
        queue_thresholds,
        auth,
    })
}

impl StandaloneConfig {
    pub fn admin_config(&self) -> AdminConfig {
        AdminConfig {
            base_path: self.base_path.clone(),
            public_origin: self.public_origin.clone(),
            refresh_seconds: self.refresh_seconds,
            overview_cache: Duration::from_secs(self.overview_cache_seconds),
            statement_timeout: Duration::from_secs(self.statement_timeout_seconds),
            overview: OverviewParams {
                window: Duration::from_secs(self.window_seconds),
                count_cap: self.count_cap,
                ..OverviewParams::default()
            },
            thresholds: Thresholds {
                default: QueueThresholds {
                    backlog_after: Duration::from_secs(self.backlog_after_seconds),
                    stall_after: Duration::from_secs(self.stall_after_seconds),
                },
                per_queue: self.queue_thresholds.clone(),
            },
            ..AdminConfig::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FULL: &str = r#"
database_url = "postgres://u:p@h/db"
bind = "0.0.0.0:9000"
pool_size = 7
base_path = "/admin"
public_origin = "https://admin.example.com"
refresh_seconds = 20
overview_cache_seconds = 6
statement_timeout_seconds = 3
count_cap = 500
window_seconds = 600
[thresholds]
backlog_after_seconds = 120
stall_after_seconds = 240
[thresholds.queues."bulk"]
backlog_after_seconds = 1800
[auth]
mode = "basic"
[[auth.users]]
name = "alice"
password_hash = "$argon2id$x"
"#;

    fn auth(body: &str) -> String {
        format!("database_url = \"x\"\n{body}")
    }

    #[test]
    fn parses_full_example() {
        let c = parse_with_env(FULL, None).ok().unwrap();
        assert_eq!(c.database_url, "postgres://u:p@h/db");
        assert_eq!(c.bind.port(), 9000);
        assert_eq!(c.pool_size, 7);
        let a = c.admin_config();
        assert_eq!(a.base_path, "/admin");
        assert_eq!(
            a.public_origin.as_deref(),
            Some("https://admin.example.com")
        );
        assert_eq!(a.refresh_seconds, 20);
        assert_eq!(a.overview_cache, Duration::from_secs(6));
        assert_eq!(a.statement_timeout, Duration::from_secs(3));
        assert_eq!(a.overview.count_cap, 500);
        assert_eq!(a.overview.window, Duration::from_secs(600));
        assert_eq!(a.thresholds.default.backlog_after, Duration::from_secs(120));
        assert_eq!(a.thresholds.default.stall_after, Duration::from_secs(240));
        let bulk = a.thresholds.for_queue("bulk");
        assert_eq!(bulk.backlog_after, Duration::from_secs(1800));
        assert_eq!(bulk.stall_after, Duration::from_secs(240));
        assert!(matches!(c.auth, Auth::Basic(ref u) if u.len() == 1));
    }

    #[test]
    fn defaults_apply() {
        let c = parse_with_env(&auth("[auth]\nmode = \"trusted_header\"\n"), None)
            .ok()
            .unwrap();
        assert_eq!(c.bind, "127.0.0.1:8080".parse().unwrap());
        assert_eq!(c.pool_size, 4);
        assert!(matches!(c.auth, Auth::TrustedHeader(ref h) if h == "X-Forwarded-User"));
    }

    #[test]
    fn rejects_missing_auth() {
        assert!(parse_with_env(&auth(""), None).is_err());
    }

    #[test]
    fn rejects_unknown_mode() {
        assert!(parse_with_env(&auth("[auth]\nmode = \"none\"\n"), None).is_err());
    }

    #[test]
    fn rejects_basic_without_users() {
        let r = parse_with_env(&auth("[auth]\nmode = \"basic\"\n"), None);
        assert!(matches!(r, Err(ConfigError::NoUsers)));
    }

    #[test]
    fn rejects_unknown_key() {
        let t = auth("bogus = 1\n[auth]\nmode = \"trusted_header\"\n");
        assert!(parse_with_env(&t, None).is_err());
    }

    #[test]
    fn rejects_duplicate_users() {
        let u = "[[auth.users]]\nname = \"a\"\npassword_hash = \"SECRETHASH\"\n";
        let t = auth(&format!("[auth]\nmode = \"basic\"\n{u}{u}"));
        let e = parse_with_env(&t, None).err().unwrap();
        assert!(matches!(e, ConfigError::DuplicateUser(ref n) if n == "a"));
        assert!(!e.to_string().contains("SECRETHASH"));
    }

    #[test]
    fn database_url_falls_back_to_env() {
        let t = "[auth]\nmode = \"trusted_header\"\n";
        let c = parse_with_env(t, Some("postgres://env".into()))
            .ok()
            .unwrap();
        assert_eq!(c.database_url, "postgres://env");
        assert!(matches!(
            parse_with_env(t, None),
            Err(ConfigError::NoDatabaseUrl)
        ));
    }
}
