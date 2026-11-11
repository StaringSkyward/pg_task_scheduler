CREATE TABLE queue_tasks (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Stable producer idempotency key for one logical task.
    task_key TEXT NOT NULL UNIQUE,

    -- Handler/type name and immutable input payload.
    task_name TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,

    status TEXT NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'running', 'done', 'dead')),

    -- Higher integer means higher priority.
    priority INTEGER NOT NULL DEFAULT 0,

    -- Eligibility time:
    --   immediate task -> now()
    --   scheduled task -> future timestamp
    --   retry          -> retry timestamp
    due_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Number of attempts that have been claimed/started.
    attempts INTEGER NOT NULL DEFAULT 0
        CHECK (attempts >= 0),

    max_attempts INTEGER NOT NULL DEFAULT 3
        CHECK (max_attempts >= 1),

    -- Valid only while status = 'running'.
    locked_by TEXT,
    lock_token BIGINT NOT NULL DEFAULT 0
        CHECK (lock_token >= 0),
    locked_until TIMESTAMPTZ,

    last_error TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,

    -- A running row always has an owner and lease.
    -- A non-running row never retains an active owner or lease.
    CHECK (
        (status = 'running'
            AND locked_by IS NOT NULL
            AND locked_until IS NOT NULL)
        OR
        (status <> 'running'
            AND locked_by IS NULL
            AND locked_until IS NULL)
    ),

    -- Terminal states have a completion timestamp; active states do not.
    CHECK (
        (status IN ('done', 'dead') AND completed_at IS NOT NULL)
        OR
        (status IN ('queued', 'running') AND completed_at IS NULL)
    )
);

CREATE INDEX queue_tasks_claim_idx
    ON queue_tasks (priority DESC, due_at ASC, id ASC)
    WHERE status = 'queued';

CREATE INDEX queue_tasks_due_idx
    ON queue_tasks (due_at ASC, id ASC)
    WHERE status = 'queued';

CREATE INDEX queue_tasks_expired_lease_idx
    ON queue_tasks (locked_until ASC, id ASC)
    WHERE status = 'running';

CREATE INDEX queue_tasks_terminal_cleanup_idx
    ON queue_tasks (completed_at ASC, id ASC)
    WHERE status IN ('done', 'dead');

-- PRIMARY KEY is (task_id, lock_token), not (task_id, attempt_no): replay resets
-- attempts to 0, so attempt_no repeats for a task but lock_token never does.
CREATE TABLE queue_task_attempts (
    task_id BIGINT NOT NULL
        REFERENCES queue_tasks(id) ON DELETE CASCADE,
    attempt_no INTEGER NOT NULL,
    lock_token BIGINT NOT NULL,
    worker_id TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    outcome TEXT
        CHECK (outcome IN ('done', 'retry', 'dead', 'lease_expired', 'released')),
    error TEXT,

    PRIMARY KEY (task_id, lock_token)
);

CREATE INDEX queue_task_attempts_task_idx
    ON queue_task_attempts (task_id, attempt_no);

CREATE INDEX queue_task_attempts_started_idx
    ON queue_task_attempts (started_at DESC);

CREATE TABLE queue_outbox (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_key TEXT NOT NULL,
    effect_name TEXT NOT NULL,
    command_key TEXT NOT NULL UNIQUE,
    topic TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,

    UNIQUE (task_key, effect_name)
);

CREATE INDEX queue_outbox_unpublished_idx
    ON queue_outbox (id)
    WHERE published_at IS NULL;

-- Audit of administrative replays. No FK: the audit outlives task retention.
CREATE TABLE queue_task_replays (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_id     BIGINT NOT NULL,
    task_key    TEXT NOT NULL,
    lock_token  BIGINT NOT NULL,
    operator    TEXT NOT NULL,
    reason      TEXT NOT NULL,
    replayed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
