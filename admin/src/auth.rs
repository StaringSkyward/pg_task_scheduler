//! Authentication layers. Each one inserts [`Operator`] or answers 401 itself. An
//! embedding application can use its own layer instead: it only has to insert
//! [`Operator`].
use std::collections::HashMap;
use std::sync::Arc;

use argon2::ARGON2ID_IDENT;
use argon2::Argon2;
use argon2::PasswordHash;
use argon2::PasswordHasher;
use argon2::PasswordVerifier;
use axum::Router;
use axum::extract::Request;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::HeaderName;
use axum::http::StatusCode;
use axum::http::header;
use axum::middleware::Next;
use axum::middleware::{self};
use axum::response::IntoResponse;
use axum::response::Response;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use tokio::sync::Semaphore;

use crate::Operator;

/// A basic-auth user. `password_hash` is an argon2id PHC string, as made by
/// [`hash_password`].
#[derive(Clone)]
pub struct BasicUser {
    /// The login name; it becomes the [`Operator`].
    pub name: String,
    /// An argon2id PHC string.
    pub password_hash: String,
}

/// Leaves the hash out, so logging a config never prints it.
impl std::fmt::Debug for BasicUser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BasicUser")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// Why [`with_basic_auth`] rejected its users.
#[derive(Debug, thiserror::Error)]
pub enum AuthConfigError {
    /// A user's `password_hash` is not an argon2id PHC string.
    #[error("user {user:?}: password_hash is not an argon2id PHC string")]
    InvalidHash {
        /// The user's name.
        user: String,
    },
}

/// An argon2id PHC string for `password`, with a random salt and default parameters.
pub fn hash_password(password: &str) -> String {
    Argon2::default()
        .hash_password(password.as_bytes())
        .expect("argon2 with default parameters fails only if the OS random source does")
        .to_string()
}

struct Users {
    hashes: HashMap<String, String>,
    /// Verified for unknown names, so the response time does not reveal which exist.
    dummy: String,
    /// One permit per verify in flight. Each verify holds ~19 MiB and a core for tens
    /// of milliseconds, and anyone can trigger one, so this caps that at one per CPU.
    verifies: Arc<Semaphore>,
}

/// Require HTTP basic auth against `users`; the user name becomes the [`Operator`].
/// Fails if any hash is not an argon2id PHC string.
pub fn with_basic_auth(router: Router, users: Vec<BasicUser>) -> Result<Router, AuthConfigError> {
    let mut hashes = HashMap::new();
    for u in users {
        match PasswordHash::new(&u.password_hash) {
            Ok(h) if h.algorithm == ARGON2ID_IDENT => {}
            _ => return Err(AuthConfigError::InvalidHash { user: u.name }),
        }
        hashes.insert(u.name, u.password_hash);
    }
    // Default parameters, like `hash_password`, so its cost matches a real user's.
    // ponytail: a user hashed with custom parameters verifies at a different cost than
    // unknown names; derive the dummy from that user's parameters if that ever matters.
    let dummy = hash_password("");
    let cpus = std::thread::available_parallelism().map_or(4, |n| n.get());
    let verifies = Arc::new(Semaphore::new(cpus));
    let users = Arc::new(Users {
        hashes,
        dummy,
        verifies,
    });
    Ok(router
        .layer(middleware::from_fn_with_state(users, basic))
        .layer(middleware::map_response(crate::no_framing)))
}

async fn basic(State(users): State<Arc<Users>>, mut req: Request, next: Next) -> Response {
    let Some((name, password)) = credentials(req.headers()) else {
        return challenge();
    };
    let limit = users.verifies.clone();
    let verified = limited(limit, move || {
        let (hash, known) = match users.hashes.get(&name) {
            Some(h) => (h, true),
            None => (&users.dummy, false),
        };
        let ok = Argon2::default()
            .verify_password(password.as_bytes(), hash.as_str())
            .is_ok();
        (ok && known).then_some(name)
    })
    .await;
    match verified {
        Some(Some(name)) => {
            req.extensions_mut().insert(Operator(name));
            next.run(req).await
        }
        _ => challenge(),
    }
}

/// Run `f` on the blocking pool (argon2 would stall the async workers) once `limit`
/// grants a permit; callers queue for one. The permit moves into the blocking task, so
/// a request dropped mid-verify (client gone) does not free its slot early. `None` if
/// `f` panicked.
async fn limited<R: Send + 'static>(
    limit: Arc<Semaphore>,
    f: impl FnOnce() -> R + Send + 'static,
) -> Option<R> {
    let permit = limit.acquire_owned().await.ok()?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        f()
    })
    .await
    .ok()
}

/// `(user, password)` from `Authorization: Basic base64(user:password)`.
fn credentials(h: &HeaderMap) -> Option<(String, String)> {
    let value = h.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, encoded) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("basic") {
        return None;
    }
    let decoded = String::from_utf8(BASE64.decode(encoded.trim()).ok()?).ok()?;
    let (user, password) = decoded.split_once(':')?;
    Some((user.to_owned(), password.to_owned()))
}

fn challenge() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(
            header::WWW_AUTHENTICATE,
            r#"Basic realm="pg_task_scheduler admin", charset="UTF-8""#,
        )],
    )
        .into_response()
}

/// Take the [`Operator`] from `header`, set by an authenticating proxy. Missing, empty
/// or repeated → 401. Only safe when nothing but the proxy can reach the server, and
/// the proxy overwrites any value the client sent.
pub fn with_trusted_header(router: Router, header: HeaderName) -> Router {
    router
        .layer(middleware::from_fn_with_state(header, trusted))
        .layer(middleware::map_response(crate::no_framing))
}

async fn trusted(State(header): State<HeaderName>, mut req: Request, next: Next) -> Response {
    let mut values = req.headers().get_all(&header).iter();
    // A second value means the proxy appended to a client-supplied one.
    let name = match (values.next(), values.next()) {
        (Some(v), None) => std::str::from_utf8(v.as_bytes())
            .ok()
            .filter(|s| !s.is_empty()),
        _ => None,
    };
    let Some(name) = name.map(str::to_owned) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    req.extensions_mut().insert(Operator(name));
    next.run(req).await
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::SeqCst;
    use std::time::Duration;

    use super::*;

    /// At most `N` verifies run at once, even when callers give up mid-verify.
    #[tokio::test(flavor = "multi_thread")]
    async fn verifies_are_bounded_even_when_callers_give_up() {
        const N: usize = 2;
        let limit = Arc::new(Semaphore::new(N));
        let running = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let job = || {
            let (running, peak) = (running.clone(), peak.clone());
            move || {
                peak.fetch_max(running.fetch_add(1, SeqCst) + 1, SeqCst);
                std::thread::sleep(Duration::from_millis(50));
                running.fetch_sub(1, SeqCst);
            }
        };
        // Callers that are dropped while their verify runs, like a closed connection.
        let quitters: Vec<_> = (0..8)
            .map(|_| tokio::spawn(limited(limit.clone(), job())))
            .collect();
        tokio::time::timeout(Duration::from_secs(5), async {
            while running.load(SeqCst) < N {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("no verify started");
        quitters.iter().for_each(|q| q.abort());
        let stayers: Vec<_> = (0..8)
            .map(|_| tokio::spawn(limited(limit.clone(), job())))
            .collect();
        for s in stayers {
            s.await.unwrap().unwrap();
        }
        assert_eq!(peak.load(SeqCst), N);
    }
}
