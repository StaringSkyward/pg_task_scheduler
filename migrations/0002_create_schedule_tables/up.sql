-- Recurring schedules (`cron` feature). A schedule only produces `queue_tasks` rows.
CREATE TABLE scheduler_jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL UNIQUE
                    CHECK (char_length(name) BETWEEN 1 AND 255
                           AND name !~ '^[[:space:]]|[[:space:]]$'),
    cron_expression TEXT NOT NULL,
    job_args        JSONB NOT NULL DEFAULT '{}'::jsonb,
    next_run_at     TIMESTAMPTZ NOT NULL,
    max_attempts    INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts >= 1),
    is_paused       BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX scheduler_jobs_due_idx
    ON scheduler_jobs (next_run_at)
    WHERE is_paused = false;
