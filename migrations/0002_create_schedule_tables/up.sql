-- Recurring schedules (`cron` feature). A schedule only produces `queue_tasks` rows.
CREATE TABLE scheduler_jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL UNIQUE
                    CHECK (char_length(name) BETWEEN 1 AND 255
                           AND name !~ '^[[:space:]]|[[:space:]]$'),
    task_name       TEXT NOT NULL
                    CHECK (char_length(task_name) BETWEEN 1 AND 255
                           AND task_name !~ '^[[:space:]]|[[:space:]]$'),
    cron_expression TEXT NOT NULL,
    job_args        JSONB NOT NULL DEFAULT '{}'::jsonb,
    next_run_at     TIMESTAMPTZ NOT NULL,
    max_attempts    INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts >= 1),
    queue           TEXT NOT NULL DEFAULT 'default',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX scheduler_jobs_due_idx ON scheduler_jobs (next_run_at);

-- A row pauses its job: no occurrences are materialized until it is deleted.
CREATE TABLE scheduler_job_pauses (
    job_id    UUID PRIMARY KEY REFERENCES scheduler_jobs (id) ON DELETE CASCADE,
    paused_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
