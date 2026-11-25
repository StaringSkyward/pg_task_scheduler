-- The identifier CHECK's code-point class and char_length, and JSONB's non-ASCII text, need
-- UTF8: in another encoding they see bytes (in SQL_ASCII, 'voila' with a grave accent ends in
-- byte 0xA0 and fails the CHECK).
DO $$
BEGIN
    IF current_setting('server_encoding') <> 'UTF8' THEN
        RAISE EXCEPTION 'pg_task_scheduler requires a UTF8 database; this one is %',
            current_setting('server_encoding');
    END IF;
END
$$;

-- The crate's identifier rule, mirroring `validate_identifier` in src/ids.rs: 1 to 255
-- characters, with no leading or trailing Unicode White_Space (what Rust's `str::trim`
-- strips). Every identifier column's CHECK calls it. The class is spelled out because
-- `[[:space:]]` follows the locale and misses U+0085 and U+00A0. NUL needs no test: text
-- cannot hold it. Not STRICT: that would stop PostgreSQL inlining the body into each CHECK,
-- and NULL already yields NULL, which a CHECK accepts (so nullable columns stay nullable).
CREATE FUNCTION queue_is_identifier(s TEXT) RETURNS BOOLEAN
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $$
    SELECT char_length(s) BETWEEN 1 AND 255
       AND s !~ E'^[\\u0009-\\u000D\\u0020\\u0085\\u00A0\\u1680\\u2000-\\u200A\\u2028\\u2029\\u202F\\u205F\\u3000]'
       AND s !~ E'[\\u0009-\\u000D\\u0020\\u0085\\u00A0\\u1680\\u2000-\\u200A\\u2028\\u2029\\u202F\\u205F\\u3000]$'
$$;

-- A TIMESTAMPTZ that chrono's DateTime<Utc> can hold: finite, and below 262143-01-01
-- (chrono 0.4's NaiveDate::MAX is 262142-12-31). PostgreSQL's own lower bound (4713 BC)
-- is already inside chrono's range. Every timestamp column uses it, so every stored
-- timestamp decodes.
CREATE DOMAIN queue_timestamptz AS TIMESTAMPTZ
    CHECK (isfinite(VALUE) AND VALUE < '262143-01-01 00:00:00+00');

CREATE TABLE queue_tasks (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Stable producer idempotency key for one logical task.
    task_key TEXT NOT NULL UNIQUE
        CONSTRAINT queue_tasks_task_key_identifier CHECK (queue_is_identifier(task_key)),

    -- Handler/type name and immutable input payload.
    task_name TEXT NOT NULL
        CONSTRAINT queue_tasks_task_name_identifier CHECK (queue_is_identifier(task_name)),

    -- Logical queue; workers run each queue with its own concurrency.
    queue TEXT NOT NULL DEFAULT 'default'
        CONSTRAINT queue_tasks_queue_identifier CHECK (queue_is_identifier(queue)),
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,

    status TEXT NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'running', 'done', 'dead', 'cancelled')),

    -- Higher integer means higher priority.
    priority INTEGER NOT NULL DEFAULT 0,

    -- Eligibility time:
    --   immediate task -> statement_timestamp()
    --   scheduled task -> future timestamp
    --   retry          -> retry timestamp
    due_at queue_timestamptz NOT NULL DEFAULT statement_timestamp(),

    -- Number of attempts that have been claimed/started.
    attempts INTEGER NOT NULL DEFAULT 0
        CHECK (attempts >= 0),

    max_attempts INTEGER NOT NULL DEFAULT 3
        CHECK (max_attempts >= 1),

    -- Valid only while status = 'running'.
    locked_by TEXT
        CONSTRAINT queue_tasks_locked_by_identifier CHECK (queue_is_identifier(locked_by)),
    lock_token BIGINT NOT NULL DEFAULT 0
        CHECK (lock_token >= 0),
    locked_until queue_timestamptz,

    last_error TEXT,

    created_at queue_timestamptz NOT NULL DEFAULT statement_timestamp(),
    updated_at queue_timestamptz NOT NULL DEFAULT statement_timestamp(),
    completed_at queue_timestamptz,

    -- The claim's predicate (attempts < max_attempts) depends on these: a queued row with no
    -- attempts left would never run and never be cleaned up.
    CHECK (attempts <= max_attempts),
    CHECK (status <> 'queued' OR attempts < max_attempts),
    -- A running row is a claimed attempt, and the reaper and cancel upserts insert
    -- attempt_no = attempts (>= 1) into queue_task_attempts.
    CHECK (status <> 'running' OR attempts >= 1),

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
        (status IN ('done', 'dead', 'cancelled') AND completed_at IS NOT NULL)
        OR
        (status IN ('queued', 'running') AND completed_at IS NULL)
    )
);

CREATE INDEX queue_tasks_claim_idx
    ON queue_tasks (queue, priority DESC, due_at ASC, id ASC)
    WHERE status = 'queued';

CREATE INDEX queue_tasks_due_idx
    ON queue_tasks (queue, due_at ASC, id ASC)
    WHERE status = 'queued';

CREATE INDEX queue_tasks_expired_lease_idx
    ON queue_tasks (locked_until ASC, id ASC)
    WHERE status = 'running';

CREATE INDEX queue_tasks_terminal_cleanup_idx
    ON queue_tasks (completed_at ASC, id ASC)
    WHERE status IN ('done', 'dead', 'cancelled');

-- PRIMARY KEY is (task_id, lock_token), not (task_id, attempt_no): replay resets
-- attempts to 0, so attempt_no repeats for a task but lock_token never does.
CREATE TABLE queue_task_attempts (
    task_id BIGINT NOT NULL
        REFERENCES queue_tasks(id) ON DELETE CASCADE,
    attempt_no INTEGER NOT NULL CHECK (attempt_no >= 1),
    lock_token BIGINT NOT NULL,
    worker_id TEXT NOT NULL
        CONSTRAINT queue_task_attempts_worker_id_identifier CHECK (queue_is_identifier(worker_id)),
    started_at queue_timestamptz NOT NULL DEFAULT statement_timestamp(),
    finished_at queue_timestamptz,
    outcome TEXT
        CHECK (outcome IN ('done', 'retry', 'dead', 'lease_expired', 'released', 'cancelled')),
    error TEXT,

    PRIMARY KEY (task_id, lock_token),

    -- An attempt is closed exactly when it has an outcome.
    CHECK ((finished_at IS NULL) = (outcome IS NULL)),

    -- Only a failed close carries a diagnostic; an open, done or released attempt has none.
    CONSTRAINT queue_task_attempts_error_on_failure
        -- A NULL outcome makes the IN unknown, which a CHECK passes, so test it explicitly.
        CHECK (error IS NULL OR coalesce(outcome IN ('retry', 'dead', 'lease_expired', 'cancelled'), false))
);

CREATE INDEX queue_task_attempts_task_idx
    ON queue_task_attempts (task_id, attempt_no);

CREATE INDEX queue_task_attempts_started_idx
    ON queue_task_attempts (started_at DESC);

CREATE TABLE queue_outbox (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_key TEXT NOT NULL
        CONSTRAINT queue_outbox_task_key_identifier CHECK (queue_is_identifier(task_key)),
    effect_name TEXT NOT NULL
        CONSTRAINT queue_outbox_effect_name_identifier CHECK (queue_is_identifier(effect_name)),
    -- `CommandKey`'s rule: whitespace allowed, at most 2048 bytes for the unique index.
    command_key TEXT NOT NULL UNIQUE
        CONSTRAINT queue_outbox_command_key_length
        CHECK (octet_length(command_key) BETWEEN 1 AND 2048),
    topic TEXT NOT NULL
        CONSTRAINT queue_outbox_topic_identifier CHECK (queue_is_identifier(topic)),
    payload JSONB NOT NULL,
    created_at queue_timestamptz NOT NULL DEFAULT statement_timestamp(),
    published_at queue_timestamptz,

    UNIQUE (task_key, effect_name)
);

CREATE INDEX queue_outbox_unpublished_idx
    ON queue_outbox (id)
    WHERE published_at IS NULL;

-- Retention cleanup of published rows: `published_at < cutoff` is an index condition, so a
-- backlog of unpublished rows is never walked.
CREATE INDEX queue_outbox_published_idx
    ON queue_outbox (published_at, id)
    WHERE published_at IS NOT NULL;
