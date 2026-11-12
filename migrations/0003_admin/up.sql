-- 1. cancelled status: a terminal state beside done and dead.
ALTER TABLE queue_tasks DROP CONSTRAINT queue_tasks_status_check;
ALTER TABLE queue_tasks ADD CONSTRAINT queue_tasks_status_check
    CHECK (status IN ('queued', 'running', 'done', 'dead', 'cancelled'));

ALTER TABLE queue_tasks DROP CONSTRAINT queue_tasks_check1;
ALTER TABLE queue_tasks ADD CONSTRAINT queue_tasks_check1
    CHECK (
        (status IN ('done', 'dead', 'cancelled') AND completed_at IS NOT NULL)
        OR
        (status IN ('queued', 'running') AND completed_at IS NULL)
    );

ALTER TABLE queue_task_attempts DROP CONSTRAINT queue_task_attempts_outcome_check;
ALTER TABLE queue_task_attempts ADD CONSTRAINT queue_task_attempts_outcome_check
    CHECK (outcome IN ('done', 'retry', 'dead', 'lease_expired', 'released', 'cancelled'));

DROP INDEX queue_tasks_terminal_cleanup_idx;
CREATE INDEX queue_tasks_terminal_cleanup_idx
    ON queue_tasks (completed_at ASC, id ASC)
    WHERE status IN ('done', 'dead', 'cancelled');

-- 2. admin action audit: one row per operator action; replaces queue_task_replays.
-- No FKs: the audit outlives task retention.
CREATE TABLE queue_admin_actions (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    action            TEXT NOT NULL CHECK (action IN ('replay', 'cancel', 'pause', 'resume')),
    task_id           BIGINT,
    task_name         TEXT,
    queue             TEXT,
    last_error_filter TEXT,
    max_task_id       BIGINT,
    affected          BIGINT NOT NULL DEFAULT 0,
    operator          TEXT NOT NULL,
    reason            TEXT NOT NULL,
    at                TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX queue_admin_actions_task_idx
    ON queue_admin_actions (task_id) WHERE task_id IS NOT NULL;

ALTER TABLE queue_tasks ADD COLUMN admin_action_id BIGINT;

INSERT INTO queue_admin_actions (action, task_id, affected, operator, reason, at)
SELECT 'replay', task_id, 1, operator, reason, replayed_at
FROM queue_task_replays ORDER BY id;
DROP TABLE queue_task_replays;

-- 3. bulk replay and the failed-task browser find dead/cancelled rows by name. Partial, so
-- enqueue, claim and completion never maintain it.
CREATE INDEX queue_tasks_failed_name_idx
    ON queue_tasks (task_name, status, completed_at DESC, id DESC)
    WHERE status IN ('dead', 'cancelled');

-- 4. queue pauses: a row stops every worker claiming from that queue.
CREATE TABLE queue_pauses (
    queue     TEXT PRIMARY KEY,
    operator  TEXT NOT NULL,
    reason    TEXT NOT NULL,
    paused_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 5. worker registry: display-only, never read by claims or ownership. Each worker upserts its
-- row every registry_interval and deletes it after a clean shutdown. lost_after is
-- now() + 3 × registry_interval at each write, so readers need no worker config.
CREATE TABLE queue_workers (
    worker_id    TEXT PRIMARY KEY,
    label        TEXT,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lost_after   TIMESTAMPTZ NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('starting', 'healthy', 'degraded', 'stopping')),
    last_error   TEXT,
    queues       JSONB NOT NULL,
    task_names   TEXT[] NOT NULL
);

-- 6. the failed/cancelled browser lists newest first without a task_name filter. Partial like
-- section 3's index, so enqueue, claim and completion never maintain it.
CREATE INDEX queue_tasks_failed_idx
    ON queue_tasks (status, completed_at DESC, id DESC)
    WHERE status IN ('dead', 'cancelled');

-- No index on queue_task_attempts: closing an attempt sets finished_at, outcome and error, and
-- an index on any of them (key or predicate) would make every close a non-HOT update that
-- writes all of the table's indexes. The overview's failed-attempt groups read the newest
-- attempts through queue_task_attempts_started_idx instead.
