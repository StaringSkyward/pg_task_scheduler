-- 1. admin action audit: one row per operator action, never changed after its insert, and never
-- deleted. The tasks a replay or cancel changed are its queue_admin_action_tasks rows.
CREATE TABLE queue_admin_actions (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    action            TEXT NOT NULL CHECK (action IN ('replay', 'cancel', 'pause', 'resume')),
    task_name         TEXT
        CONSTRAINT queue_admin_actions_task_name_identifier CHECK (queue_is_identifier(task_name)),
    queue             TEXT
        CONSTRAINT queue_admin_actions_queue_identifier CHECK (queue_is_identifier(queue)),
    -- A bulk replay's error filter: any last_error, none (NULL), or exactly last_error_filter.
    error_filter      TEXT CHECK (error_filter IN ('any', 'missing', 'exact')),
    last_error_filter TEXT,
    max_task_id       BIGINT,
    operator          TEXT NOT NULL,
    reason            TEXT NOT NULL,
    -- Who and why must say something: not empty, not only whitespace.
    CONSTRAINT queue_admin_actions_text_present
        CHECK (operator ~ '[^[:space:]]' AND reason ~ '[^[:space:]]'),
    at                queue_timestamptz NOT NULL DEFAULT statement_timestamp(),

    -- The targets of queue_admin_action_tasks' and queue_pauses' composite foreign keys.
    UNIQUE (id, action),
    UNIQUE (id, action, queue),

    -- Shape per action: pause/resume name a queue only; a single replay/cancel names its task's
    -- name and queue (the task is its one membership row, which its one statement writes; a
    -- CHECK cannot see that); a bulk one names the task name and the max_task_id bound. Only a
    -- bulk replay has an error filter, always, and its text exactly when it is 'exact'.
    CONSTRAINT queue_admin_actions_shape CHECK (
        (action IN ('pause', 'resume') AND queue IS NOT NULL AND task_name IS NULL
            AND error_filter IS NULL AND last_error_filter IS NULL AND max_task_id IS NULL)
        OR (action IN ('replay', 'cancel') AND task_name IS NOT NULL AND queue IS NOT NULL
            AND error_filter IS NULL AND last_error_filter IS NULL AND max_task_id IS NULL)
        OR (action = 'cancel' AND task_name IS NOT NULL AND max_task_id IS NOT NULL
            AND error_filter IS NULL AND last_error_filter IS NULL)
        OR (action = 'replay' AND task_name IS NOT NULL AND max_task_id IS NOT NULL
            AND error_filter IS NOT NULL
            AND (error_filter = 'exact') = (last_error_filter IS NOT NULL))
    )
);

-- One row per task a replay or cancel changed, written in the statement that changes the task.
-- The composite key binds the row to a replay or cancel action. No FK to the task: the audit
-- outlives task retention. A bulk batch skips the tasks its action already changed (an
-- anti-join on the primary key), so an action changes each task at most once.
CREATE TABLE queue_admin_action_tasks (
    action_id BIGINT NOT NULL,
    action    TEXT NOT NULL CHECK (action IN ('replay', 'cancel')),
    task_id   BIGINT NOT NULL,
    PRIMARY KEY (action_id, task_id),
    FOREIGN KEY (action_id, action) REFERENCES queue_admin_actions (id, action)
);
-- The task page's actions.
CREATE INDEX queue_admin_action_tasks_task_idx ON queue_admin_action_tasks (task_id, action_id);

-- 2. bulk replay and the failed-task browser find dead/cancelled rows by name. Partial, so
-- enqueue, claim and completion never maintain it.
CREATE INDEX queue_tasks_failed_name_idx
    ON queue_tasks (task_name, status, completed_at DESC, id DESC)
    WHERE status IN ('dead', 'cancelled');

-- 3. queue pauses: a row stops every worker claiming from that queue. Operator, reason and time
-- are the pause action's; the composite key ties the row to a pause action for the same queue.
CREATE TABLE queue_pauses (
    queue     TEXT PRIMARY KEY
        CONSTRAINT queue_pauses_queue_identifier CHECK (queue_is_identifier(queue)),
    action_id BIGINT NOT NULL,
    action    TEXT NOT NULL DEFAULT 'pause' CHECK (action = 'pause'),
    FOREIGN KEY (action_id, action, queue) REFERENCES queue_admin_actions (id, action, queue)
);

-- 4. worker registry: display-only, never read by claims or ownership. Each worker upserts its
-- row every registry_interval and deletes it after a clean shutdown. lost_after is
-- statement_timestamp() + 3 × registry_interval at each write, so readers need no worker config.
CREATE TABLE queue_workers (
    worker_id    TEXT PRIMARY KEY
        CONSTRAINT queue_workers_worker_id_identifier CHECK (queue_is_identifier(worker_id)),
    label        TEXT,
    started_at   queue_timestamptz NOT NULL DEFAULT statement_timestamp(),
    last_seen_at queue_timestamptz NOT NULL DEFAULT statement_timestamp(),
    lost_after   queue_timestamptz NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('starting', 'healthy', 'degraded', 'stopping')),
    last_error   TEXT
);

-- One row per queue a worker serves; rewritten at each registry write.
CREATE TABLE queue_worker_queues (
    worker_id        TEXT NOT NULL REFERENCES queue_workers (worker_id) ON DELETE CASCADE
        CONSTRAINT queue_worker_queues_worker_id_identifier CHECK (queue_is_identifier(worker_id)),
    queue            TEXT NOT NULL
        CONSTRAINT queue_worker_queues_queue_identifier CHECK (queue_is_identifier(queue)),
    slot_limit       BIGINT NOT NULL CHECK (slot_limit >= 1),
    in_use           BIGINT NOT NULL CHECK (in_use >= 0 AND in_use <= slot_limit),
    last_finished_at queue_timestamptz,
    PRIMARY KEY (worker_id, queue)
);

-- One row per task name a worker has a handler for.
CREATE TABLE queue_worker_task_names (
    worker_id TEXT NOT NULL REFERENCES queue_workers (worker_id) ON DELETE CASCADE
        CONSTRAINT queue_worker_task_names_worker_id_identifier
        CHECK (queue_is_identifier(worker_id)),
    task_name TEXT NOT NULL
        CONSTRAINT queue_worker_task_names_task_name_identifier
        CHECK (queue_is_identifier(task_name)),
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
