//! Metric shims: gnort aggregated counters with the `metrics` feature, zero-overhead no-ops
//! without.
//!
//! # gnort 0.2 API note
//!
//! gnort 0.2 has **no** free function like `gnort::count(name, n)`. Counters must be
//! registered as `Count` instrument handles against a `MetricsRegistry`, then incremented via
//! `.fetch_add(n)`.  The crate exposes a process-global `global_metrics_registry()` singleton
//! that initialises itself lazily (spawning a background thread to flush to Datadog/StatsD every
//! 3 s by default).  We piggyback on that singleton and hold each of the five `Count` handles in
//! a `std::sync::OnceLock` so each counter is registered exactly once, on first use, without
//! requiring the embedding application to do any setup.
//!
//! Limitation: because gnort's `Count.fetch_add` takes `usize`, we saturate `u64` values at
//! `usize::MAX` before passing them in — on 64-bit targets this is a no-op.

// ── feature-gated implementation ─────────────────────────────────────────────

#[cfg(feature = "metrics")]
mod imp {
    use gnort::instrument::Count;
    use gnort::registry::global_metrics_registry;
    use std::sync::OnceLock;

    /// Lazily register a `Count` handle against the global gnort registry.
    ///
    /// On 64-bit platforms `usize` == `u64`, so the saturating cast in `incr_by` is a no-op.
    fn get_or_register(name: &'static str) -> &'static Count {
        // We use a small lookup table of the five well-known names to avoid a DashMap lookup on
        // every hot-path call after first initialisation.
        macro_rules! named_lock {
            ($lock:ident, $n:expr) => {{
                static $lock: OnceLock<Count> = OnceLock::new();
                $lock.get_or_init(|| {
                    global_metrics_registry()
                        .register_count(name)
                        .unwrap_or_default()
                })
            }};
        }
        match name {
            crate::metrics::RUNS_MATERIALIZED => named_lock!(M0, RUNS_MATERIALIZED),
            crate::metrics::RUNS_CLAIMED => named_lock!(M1, RUNS_CLAIMED),
            crate::metrics::RUNS_COMPLETED => named_lock!(M2, RUNS_COMPLETED),
            crate::metrics::RUNS_FAILED => named_lock!(M3, RUNS_FAILED),
            crate::metrics::RUNS_REAPED => named_lock!(M4, RUNS_REAPED),
            // Fallback for any name not in the known set: register each time (slow path).
            _ => {
                // Safety: 'static str keys are unique enough for the registry's DashMap;
                // register_count has get-or-insert semantics so duplicates are harmless.
                // We cannot cache an arbitrary name in a OnceLock, so we fall back to a
                // per-call approach and accept the DashMap lookup overhead.
                //
                // LIMITATION: the returned Count is a fresh clone; if the caller retains it
                // they will see aggregated data, but this fallback path is not expected to be
                // called with our five named constants.
                Box::leak(Box::new(
                    global_metrics_registry()
                        .register_count(name)
                        .unwrap_or_default(),
                ))
            }
        }
    }

    #[inline]
    pub fn incr(name: &'static str) {
        get_or_register(name).increment();
    }

    #[inline]
    pub fn incr_by(name: &'static str, n: u64) {
        // Count::fetch_add takes usize; on 64-bit this cast never truncates.
        let n_usize = usize::try_from(n).unwrap_or(usize::MAX);
        get_or_register(name).fetch_add(n_usize);
    }
}

// ── zero-overhead no-op implementation ───────────────────────────────────────

#[cfg(not(feature = "metrics"))]
mod imp {
    #[inline]
    pub fn incr(_name: &'static str) {}

    #[inline]
    pub fn incr_by(_name: &'static str, _n: u64) {}
}

// ── public surface ────────────────────────────────────────────────────────────

pub use imp::{incr, incr_by};

pub const RUNS_MATERIALIZED: &str = "pg_task_scheduler.runs.materialized";
pub const RUNS_CLAIMED: &str = "pg_task_scheduler.runs.claimed";
pub const RUNS_COMPLETED: &str = "pg_task_scheduler.runs.completed";
pub const RUNS_FAILED: &str = "pg_task_scheduler.runs.failed";
pub const RUNS_REAPED: &str = "pg_task_scheduler.runs.reaped";
