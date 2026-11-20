-- Recurring schedules (`cron` feature). A schedule only produces `queue_tasks` rows.
CREATE TABLE scheduler_jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL UNIQUE
                    CONSTRAINT scheduler_jobs_name_identifier CHECK (queue_is_identifier(name)),
    task_name       TEXT NOT NULL
                    CONSTRAINT scheduler_jobs_task_name_identifier
                    CHECK (queue_is_identifier(task_name)),
    cron_expression TEXT NOT NULL,
    job_args        JSONB NOT NULL DEFAULT '{}'::jsonb,
    next_run_at     queue_timestamptz NOT NULL,
    max_attempts    INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts >= 1),
    queue           TEXT NOT NULL DEFAULT 'default'
                    CONSTRAINT scheduler_jobs_queue_identifier CHECK (queue_is_identifier(queue)),
    created_at      queue_timestamptz NOT NULL DEFAULT now(),
    updated_at      queue_timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX scheduler_jobs_due_idx ON scheduler_jobs (next_run_at);

-- A row pauses its job: no occurrences are materialized until it is deleted.
CREATE TABLE scheduler_job_pauses (
    job_id    UUID PRIMARY KEY REFERENCES scheduler_jobs (id) ON DELETE CASCADE,
    paused_at queue_timestamptz NOT NULL DEFAULT now()
);
