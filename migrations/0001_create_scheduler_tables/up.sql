CREATE TYPE scheduler_run_state AS ENUM (
    'pending',
    'running',
    'completed',
    'failed',
    'cancelled'
);

CREATE TYPE scheduler_attempt_outcome AS ENUM (
    'completed',
    'failed',
    'expired',
    'cancelled'
);

CREATE TABLE scheduler_jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL UNIQUE
                    CHECK (char_length(name) BETWEEN 1 AND 255
                           AND name !~ '^[[:space:]]|[[:space:]]$'),
    cron_expression TEXT NOT NULL,
    job_args        JSONB NOT NULL DEFAULT '{}'::jsonb,
    next_run_at     TIMESTAMPTZ NOT NULL,
    lease_duration  INTERVAL NOT NULL DEFAULT INTERVAL '5 minutes'
                    CHECK (
                        lease_duration > INTERVAL '0 seconds'
                        AND EXTRACT(YEAR FROM lease_duration) = 0
                        AND EXTRACT(MONTH FROM lease_duration) = 0
                        AND EXTRACT(DAY FROM lease_duration) = 0
                    ),
    max_attempts    INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts >= 1),
    is_paused       BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    retry_backoff   INTERVAL NOT NULL DEFAULT INTERVAL '1 second'
                    CHECK (
                        retry_backoff >= INTERVAL '0 seconds'
                        AND EXTRACT(YEAR FROM retry_backoff) = 0
                        AND EXTRACT(MONTH FROM retry_backoff) = 0
                        AND EXTRACT(DAY FROM retry_backoff) = 0
                    )
);

CREATE INDEX scheduler_jobs_due_idx
    ON scheduler_jobs (next_run_at)
    WHERE is_paused = false;

CREATE TABLE scheduler_runs (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id            UUID,
    scheduled_for     TIMESTAMPTZ NOT NULL,
    attempt_count     INTEGER NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    job_name          TEXT NOT NULL,
    job_args          JSONB NOT NULL,
    available_at      TIMESTAMPTZ NOT NULL,
    priority          SMALLINT NOT NULL DEFAULT 0,
    state             scheduler_run_state NOT NULL DEFAULT 'pending',
    max_attempts      INTEGER NOT NULL,
    lease_duration    INTERVAL NOT NULL,
    retry_backoff     INTERVAL NOT NULL,
    worker_id         TEXT,
    lease_token       UUID,
    lease_expires_at  TIMESTAMPTZ,
    started_at        TIMESTAMPTZ,
    finished_at       TIMESTAMPTZ,
    last_error        TEXT,
    deduplication_key TEXT,
    CONSTRAINT scheduler_runs_name_check CHECK (
        char_length(job_name) BETWEEN 1 AND 255
        AND job_name !~ '^[[:space:]]|[[:space:]]$'
    ),
    CONSTRAINT scheduler_runs_worker_check CHECK (
        worker_id IS NULL OR (
            char_length(worker_id) BETWEEN 1 AND 255
            AND worker_id !~ '^[[:space:]]|[[:space:]]$'
        )
    ),
    CONSTRAINT scheduler_runs_attempts_check CHECK (
        attempt_count >= 0
        AND max_attempts >= 1
        AND attempt_count <= max_attempts
        AND (state <> 'pending' OR attempt_count < max_attempts)
    ),
    CONSTRAINT scheduler_runs_lease_duration_check CHECK (
        lease_duration > INTERVAL '0 seconds'
        AND EXTRACT(YEAR FROM lease_duration) = 0
        AND EXTRACT(MONTH FROM lease_duration) = 0
        AND EXTRACT(DAY FROM lease_duration) = 0
    ),
    CONSTRAINT scheduler_runs_retry_backoff_check CHECK (
        retry_backoff >= INTERVAL '0 seconds'
        AND EXTRACT(YEAR FROM retry_backoff) = 0
        AND EXTRACT(MONTH FROM retry_backoff) = 0
        AND EXTRACT(DAY FROM retry_backoff) = 0
    ),
    CONSTRAINT scheduler_runs_deduplication_key_check CHECK (
        deduplication_key IS NULL
        OR char_length(deduplication_key) BETWEEN 1 AND 255
    ),
    CONSTRAINT scheduler_runs_state_check CHECK (
        (
            state = 'pending'
            AND worker_id IS NULL
            AND lease_token IS NULL
            AND lease_expires_at IS NULL
            AND started_at IS NULL
            AND finished_at IS NULL
            AND last_error IS NULL
        ) OR (
            state = 'running'
            AND worker_id IS NOT NULL
            AND lease_token IS NOT NULL
            AND lease_expires_at IS NOT NULL
            AND started_at IS NOT NULL
            AND lease_expires_at > started_at
            AND finished_at IS NULL
            AND last_error IS NULL
        ) OR (
            state = 'completed'
            AND worker_id IS NULL
            AND lease_token IS NULL
            AND lease_expires_at IS NULL
            AND started_at IS NULL
            AND finished_at IS NOT NULL
            AND last_error IS NULL
        ) OR (
            state = 'failed'
            AND worker_id IS NULL
            AND lease_token IS NULL
            AND lease_expires_at IS NULL
            AND started_at IS NULL
            AND finished_at IS NOT NULL
            AND last_error IS NOT NULL
        ) OR (
            state = 'cancelled'
            AND worker_id IS NULL
            AND lease_token IS NULL
            AND lease_expires_at IS NULL
            AND started_at IS NULL
            AND finished_at IS NOT NULL
            AND last_error IS NULL
        )
    )
);

CREATE UNIQUE INDEX scheduler_runs_schedule_occurrence_idx
    ON scheduler_runs (job_id, scheduled_for)
    WHERE job_id IS NOT NULL;

CREATE UNIQUE INDEX scheduler_runs_deduplication_idx
    ON scheduler_runs (job_name, deduplication_key)
    WHERE deduplication_key IS NOT NULL;

CREATE INDEX scheduler_runs_pending_claim_idx
    ON scheduler_runs (job_name, priority DESC, available_at, id)
    WHERE state = 'pending';

CREATE INDEX scheduler_runs_running_expiry_idx
    ON scheduler_runs (lease_expires_at, id)
    WHERE state = 'running';

CREATE INDEX scheduler_runs_terminal_retention_idx
    ON scheduler_runs (finished_at, id)
    WHERE state IN ('completed', 'failed', 'cancelled');

CREATE TABLE scheduler_run_attempts (
    run_id           UUID NOT NULL REFERENCES scheduler_runs (id) ON DELETE CASCADE,
    attempt_number   INTEGER NOT NULL CHECK (attempt_number >= 1),
    worker_id        TEXT NOT NULL
                     CHECK (char_length(worker_id) BETWEEN 1 AND 255
                            AND worker_id !~ '^[[:space:]]|[[:space:]]$'),
    lease_token      UUID NOT NULL UNIQUE,
    started_at       TIMESTAMPTZ NOT NULL,
    lease_expires_at TIMESTAMPTZ NOT NULL CHECK (lease_expires_at > started_at),
    finished_at      TIMESTAMPTZ,
    outcome          scheduler_attempt_outcome,
    error            TEXT,
    PRIMARY KEY (run_id, attempt_number),
    CONSTRAINT scheduler_attempt_terminal_check CHECK (
        (finished_at IS NULL AND outcome IS NULL AND error IS NULL)
        OR (finished_at IS NOT NULL AND outcome IS NOT NULL)
    ),
    CONSTRAINT scheduler_attempt_error_check CHECK (
        error IS NULL OR outcome IN ('failed', 'expired')
    )
);

CREATE INDEX scheduler_run_attempts_run_idx
    ON scheduler_run_attempts (run_id, attempt_number DESC);
