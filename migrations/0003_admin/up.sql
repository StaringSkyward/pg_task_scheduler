-- 1. admin action audit: one row per operator action.
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
    at                TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Shape per action: pause/resume name a queue only; a single replay/cancel names its task,
    -- name and queue; a bulk one names the task name and the max_task_id bound (only a bulk
    -- replay may filter on last_error).
    CHECK (
        (action IN ('pause', 'resume') AND queue IS NOT NULL AND task_id IS NULL
            AND task_name IS NULL AND last_error_filter IS NULL AND max_task_id IS NULL)
        OR (action IN ('replay', 'cancel') AND task_id IS NOT NULL AND task_name IS NOT NULL
            AND queue IS NOT NULL AND last_error_filter IS NULL AND max_task_id IS NULL)
        OR (action IN ('replay', 'cancel') AND task_id IS NULL AND task_name IS NOT NULL
            AND max_task_id IS NOT NULL AND (last_error_filter IS NULL OR action = 'replay'))
    )
);
CREATE INDEX queue_admin_actions_task_idx
    ON queue_admin_actions (task_id) WHERE task_id IS NOT NULL;

-- 2. bulk replay and the failed-task browser find dead/cancelled rows by name. Partial, so
-- enqueue, claim and completion never maintain it.
CREATE INDEX queue_tasks_failed_name_idx
    ON queue_tasks (task_name, status, completed_at DESC, id DESC)
    WHERE status IN ('dead', 'cancelled');

-- 3. queue pauses: a row stops every worker claiming from that queue.
CREATE TABLE queue_pauses (
    queue     TEXT PRIMARY KEY,
    operator  TEXT NOT NULL,
    reason    TEXT NOT NULL,
    paused_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 4. worker registry: display-only, never read by claims or ownership. Each worker upserts its
-- row every registry_interval and deletes it after a clean shutdown. lost_after is
-- now() + 3 × registry_interval at each write, so readers need no worker config.
CREATE TABLE queue_workers (
    worker_id    TEXT PRIMARY KEY,
    label        TEXT,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lost_after   TIMESTAMPTZ NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('starting', 'healthy', 'degraded', 'stopping')),
    last_error   TEXT
);

-- One row per queue a worker serves; rewritten at each registry write.
CREATE TABLE queue_worker_queues (
    worker_id        TEXT NOT NULL REFERENCES queue_workers (worker_id) ON DELETE CASCADE,
    queue            TEXT NOT NULL,
    slot_limit       BIGINT NOT NULL CHECK (slot_limit >= 1),
    in_use           BIGINT NOT NULL CHECK (in_use >= 0 AND in_use <= slot_limit),
    last_finished_at TIMESTAMPTZ,
    PRIMARY KEY (worker_id, queue)
);

-- One row per task name a worker has a handler for.
CREATE TABLE queue_worker_task_names (
    worker_id TEXT NOT NULL REFERENCES queue_workers (worker_id) ON DELETE CASCADE,
    task_name TEXT NOT NULL,
    PRIMARY KEY (worker_id, task_name)
);

-- 5. the failed/cancelled browser lists newest first without a task_name filter. Partial like
-- section 2's index, so enqueue, claim and completion never maintain it.
CREATE INDEX queue_tasks_failed_idx
    ON queue_tasks (status, completed_at DESC, id DESC)
    WHERE status IN ('dead', 'cancelled');

-- No index on queue_task_attempts: closing an attempt sets finished_at, outcome and error, and
-- an index on any of them (key or predicate) would make every close a non-HOT update that
-- writes all of the table's indexes. The overview's failed-attempt groups read the newest
-- attempts through queue_task_attempts_started_idx instead.
