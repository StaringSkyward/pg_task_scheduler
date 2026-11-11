//! A single-flight, time-boxed cache for the overview (spec §7.4).
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use pg_task_scheduler::store::Overview;
use tokio::sync::Mutex;

pub type Cached = Arc<Result<Overview, String>>;

pub struct OverviewCache {
    ttl: Duration,
    slot: Mutex<Option<(Instant, Cached)>>,
}

impl OverviewCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            slot: Mutex::new(None),
        }
    }

    /// The cached result if younger than the TTL, else `compute`d. The lock is held while
    /// computing, so concurrent callers share one computation. Errors are cached too, so a
    /// failing database is not hammered.
    pub async fn get(&self, compute: impl Future<Output = Result<Overview, String>>) -> Cached {
        let mut slot = self.slot.lock().await;
        if let Some((at, v)) = &*slot
            && at.elapsed() < self.ttl
        {
            return v.clone();
        }
        let v = Arc::new(compute.await);
        *slot = Some((Instant::now(), v.clone()));
        v
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    async fn counted(n: &AtomicUsize) -> Result<Overview, String> {
        n.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(50)).await;
        Err("boom".into())
    }

    #[tokio::test]
    async fn single_flight() {
        let cache = OverviewCache::new(Duration::from_secs(60));
        let n = AtomicUsize::new(0);
        let (a, b) = tokio::join!(cache.get(counted(&n)), cache.get(counted(&n)));
        assert_eq!(n.load(Ordering::SeqCst), 1);
        assert!(Arc::ptr_eq(&a, &b));
        assert!(a.is_err());
    }

    #[tokio::test]
    async fn expires_after_ttl() {
        let cache = OverviewCache::new(Duration::from_millis(100));
        let n = AtomicUsize::new(0);
        cache.get(counted(&n)).await;
        cache.get(counted(&n)).await;
        assert_eq!(n.load(Ordering::SeqCst), 1);
        tokio::time::sleep(Duration::from_millis(120)).await;
        cache.get(counted(&n)).await;
        assert_eq!(n.load(Ordering::SeqCst), 2);
    }
}
