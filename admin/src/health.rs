//! The queue status rules (spec §7.2). Pure: everything it needs is passed in.
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use pg_task_scheduler::store::QueueOverview;
use pg_task_scheduler::store::WorkerView;

use crate::QueueThresholds;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueStatus {
    Paused,
    Stalled,
    Backlog,
    Healthy,
}

impl QueueStatus {
    pub fn label(self) -> &'static str {
        match self {
            Self::Paused => "paused",
            Self::Stalled => "stalled",
            Self::Backlog => "backlog",
            Self::Healthy => "healthy",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verdict {
    pub status: QueueStatus,
    pub reason: String,
}

/// `5m`, `14m 03s`, `1h 05m`.
pub fn fmt_duration(d: Duration) -> String {
    let s = d.as_secs();
    match s {
        0..60 => format!("{s}s"),
        60..3600 if s.is_multiple_of(60) => format!("{}m", s / 60),
        60..3600 => format!("{}m {:02}s", s / 60, s % 60),
        _ if s % 3600 / 60 == 0 => format!("{}h", s / 3600),
        _ => format!("{}h {:02}m", s / 3600, s % 3600 / 60),
    }
}

fn verdict_of(status: QueueStatus, reason: impl Into<String>) -> Verdict {
    Verdict {
        status,
        reason: reason.into(),
    }
}

/// `serving` holds the live workers whose queues include `q.queue`.
pub fn verdict(
    q: &QueueOverview,
    serving: &[&WorkerView],
    paused: bool,
    unhandled_names: usize,
    t: &QueueThresholds,
    now: DateTime<Utc>,
) -> Verdict {
    if paused {
        return verdict_of(QueueStatus::Paused, "paused");
    }
    if q.due.value > 0 {
        if serving.is_empty() {
            return verdict_of(QueueStatus::Stalled, "no live worker");
        }
        let recent = |w: &&WorkerView| {
            let last = w
                .queues
                .iter()
                .find(|wq| wq.name == q.queue)
                .and_then(|wq| wq.last_finished_at)
                .unwrap_or(w.started_at);
            (now - last).to_std().is_ok_and(|age| age <= t.stall_after) || last > now
        };
        if !serving.iter().any(recent) {
            let reason = format!("nothing finished in {}", fmt_duration(t.stall_after));
            return verdict_of(QueueStatus::Stalled, reason);
        }
    }
    if let Some(age) = q.oldest_due_age.filter(|a| *a > t.backlog_after) {
        let slots: i64 = serving
            .iter()
            .flat_map(|w| w.queues.iter().filter(|wq| wq.name == q.queue))
            .map(|wq| wq.limit)
            .sum();
        let reason = if unhandled_names > 0 {
            format!("{unhandled_names} task names have no handler")
        } else if q.running >= slots {
            format!("all {slots} slots busy")
        } else {
            format!("oldest due task waited {}", fmt_duration(age))
        };
        return verdict_of(QueueStatus::Backlog, reason);
    }
    verdict_of(QueueStatus::Healthy, "")
}

#[cfg(test)]
mod tests {
    use pg_task_scheduler::QueueName;
    use pg_task_scheduler::WorkerId;
    use pg_task_scheduler::store::Count;
    use pg_task_scheduler::store::WorkerQueueView;
    use pg_task_scheduler::store::WorkerStatus;

    use super::*;

    const MIN: Duration = Duration::from_secs(60);

    fn now() -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000, 0).unwrap()
    }

    fn queue(due: i64, age: Duration, running: i64) -> QueueOverview {
        let c = |value| Count {
            value,
            capped: false,
        };
        QueueOverview {
            queue: QueueName::try_from("q").unwrap(),
            queued: c(due),
            due: c(due),
            oldest_due_age: (due > 0).then_some(age),
            running,
            done: c(0),
            dead: c(0),
        }
    }

    fn worker(started_ago: Duration, finished_ago: Option<Duration>, limit: i64) -> WorkerView {
        WorkerView {
            worker_id: WorkerId::try_from("w").unwrap(),
            label: None,
            started_at: now() - started_ago,
            last_seen_at: now(),
            status: WorkerStatus::Healthy,
            last_error: None,
            queues: vec![WorkerQueueView {
                name: QueueName::try_from("q").unwrap(),
                limit,
                in_use: 0,
                paused: false,
                last_finished_at: finished_ago.map(|a| now() - a),
            }],
            task_names: vec![],
            lost: false,
        }
    }

    fn run(
        q: &QueueOverview,
        w: &[WorkerView],
        paused: bool,
        unhandled: usize,
    ) -> (QueueStatus, String) {
        let refs: Vec<&WorkerView> = w.iter().collect();
        let v = verdict(
            q,
            &refs,
            paused,
            unhandled,
            &QueueThresholds::default(),
            now(),
        );
        (v.status, v.reason)
    }

    #[test]
    fn table() {
        use QueueStatus::*;
        let fresh = [worker(60 * MIN, Some(MIN), 4)];
        let s = |x: &str| x.to_owned();
        // paused beats stalled
        assert_eq!(run(&queue(1, MIN, 0), &[], true, 0), (Paused, s("paused")));
        assert_eq!(
            run(&queue(1, MIN, 0), &[], false, 0),
            (Stalled, s("no live worker"))
        );
        let old = [worker(60 * MIN, Some(6 * MIN), 4)];
        assert_eq!(
            run(&queue(1, MIN, 0), &old, false, 0),
            (Stalled, s("nothing finished in 5m"))
        );
        let never = [worker(60 * MIN, None, 4)];
        assert_eq!(run(&queue(1, MIN, 0), &never, false, 0).0, Stalled);
        // recently started, nothing finished yet: not stalled
        let young = [worker(MIN, None, 4)];
        assert_eq!(run(&queue(1, MIN, 0), &young, false, 0).0, Healthy);
        // backlog reasons
        let q = queue(5, 6 * MIN, 1);
        assert_eq!(
            run(&q, &fresh, false, 2),
            (Backlog, s("2 task names have no handler"))
        );
        let q = queue(5, 6 * MIN, 4);
        assert_eq!(run(&q, &fresh, false, 0), (Backlog, s("all 4 slots busy")));
        let q = queue(5, 14 * MIN + Duration::from_secs(3), 1);
        assert_eq!(
            run(&q, &fresh, false, 0),
            (Backlog, s("oldest due task waited 14m 03s"))
        );
        // nothing due
        assert_eq!(run(&queue(0, MIN, 0), &fresh, false, 0), (Healthy, s("")));
        assert_eq!(run(&queue(0, MIN, 0), &[], false, 0).0, Healthy);
    }
}
