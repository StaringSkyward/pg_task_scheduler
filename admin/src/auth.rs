//! Authentication layers. Each one inserts [`Operator`] or answers 401 itself. An
//! embedding application can use its own layer instead: it only has to insert
//! [`Operator`].
use std::collections::HashMap;
use std::sync::Arc;

use argon2::{ARGON2ID_IDENT, Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use axum::Router;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;

use crate::Operator;

/// A basic-auth user. `password_hash` is an argon2id PHC string, as made by
/// [`hash_password`].
#[derive(Clone)]
pub struct BasicUser {
    pub name: String,
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

#[derive(Debug, thiserror::Error)]
pub enum AuthConfigError {
    #[error("user {user:?}: password_hash is not an argon2id PHC string")]
    InvalidHash { user: String },
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
    let users = Arc::new(Users { hashes, dummy });
    Ok(router.layer(middleware::from_fn_with_state(users, basic)))
}

async fn basic(State(users): State<Arc<Users>>, mut req: Request, next: Next) -> Response {
    let Some((name, password)) = credentials(req.headers()) else {
        return challenge();
    };
    // argon2 takes tens of milliseconds of CPU: keep it off the async workers.
    let verified = tokio::task::spawn_blocking(move || {
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
        Ok(Some(name)) => {
            req.extensions_mut().insert(Operator(name));
            next.run(req).await
        }
        _ => challenge(),
    }
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
    router.layer(middleware::from_fn_with_state(header, trusted))
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
