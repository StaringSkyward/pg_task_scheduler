//! Cross-site request check for state-changing requests (spec §6.1). Browsers attach
//! basic-auth credentials and proxy cookies to cross-site requests, so without it a
//! hostile page could trigger an action.
use axum::extract::Request;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::Method;
use axum::http::StatusCode;
use axum::http::header;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;

pub(crate) async fn guard(
    State(public_origin): State<Option<String>>,
    req: Request,
    next: Next,
) -> Response {
    // POST is what the rule names; DELETE (the cron API) is covered too, as it costs nothing.
    let unsafe_method = !matches!(*req.method(), Method::GET | Method::HEAD | Method::OPTIONS);
    if unsafe_method && !allowed(req.headers(), public_origin.as_deref()) {
        return (StatusCode::FORBIDDEN, "cross-site request rejected").into_response();
    }
    next.run(req).await
}

/// 1. `Sec-Fetch-Site` present: only `same-origin` and `none` pass.
/// 2. Else `Origin` present: it must be `http(s)://{Host}` or `public_origin`.
/// 3. Else (scripts, curl): pass; they still have to authenticate.
fn allowed(h: &HeaderMap, public_origin: Option<&str>) -> bool {
    if let Some(site) = h.get("sec-fetch-site") {
        return site == "same-origin" || site == "none";
    }
    let Some(origin) = h.get(header::ORIGIN) else {
        return true;
    };
    let Ok(origin) = origin.to_str() else {
        return false;
    };
    if public_origin == Some(origin) {
        return true;
    }
    let Some(host) = h.get(header::HOST).and_then(|v| v.to_str().ok()) else {
        return false;
    };
    origin
        .strip_prefix("http://")
        .or_else(|| origin.strip_prefix("https://"))
        == Some(host)
}
