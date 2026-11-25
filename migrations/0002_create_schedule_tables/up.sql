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
    max_attempts    INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts >= 1),
    queue           TEXT NOT NULL DEFAULT 'default'
                    CONSTRAINT scheduler_jobs_queue_identifier CHECK (queue_is_identifier(queue)),
    created_at      queue_timestamptz NOT NULL DEFAULT statement_timestamp(),
    updated_at      queue_timestamptz NOT NULL DEFAULT statement_timestamp()
);

-- A job's cursor: the next occurrence to materialize. Every write to it happens under its job's
-- row lock (FOR UPDATE on scheduler_jobs), so the lock order is job, cursor, pause.
CREATE TABLE scheduler_job_cursors (
    job_id      UUID PRIMARY KEY REFERENCES scheduler_jobs (id) ON DELETE CASCADE,
    next_run_at queue_timestamptz NOT NULL
);
CREATE INDEX scheduler_job_cursors_due_idx ON scheduler_job_cursors (next_run_at);

-- A row pauses its job: no occurrences are materialized until it is deleted.
CREATE TABLE scheduler_job_pauses (
    job_id    UUID PRIMARY KEY REFERENCES scheduler_jobs (id) ON DELETE CASCADE,
    paused_at queue_timestamptz NOT NULL DEFAULT statement_timestamp()
);
