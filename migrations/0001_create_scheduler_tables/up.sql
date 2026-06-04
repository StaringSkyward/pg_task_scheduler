CREATE TYPE run_outcome AS ENUM ('completed', 'failed');

CREATE TABLE scheduler_jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL UNIQUE,
    cron_expression TEXT NOT NULL,
    job_args        JSONB NOT NULL DEFAULT '{}'::jsonb,
    next_run_at     TIMESTAMPTZ NOT NULL,
    lease_duration  INTERVAL NOT NULL DEFAULT INTERVAL '5 minutes',
    max_attempts    INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts >= 1),
    is_paused       BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE scheduler_runs (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id        UUID NOT NULL REFERENCES scheduler_jobs (id) ON DELETE CASCADE,
    scheduled_for TIMESTAMPTZ NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX scheduler_runs_job_id_scheduled_for_idx
    ON scheduler_runs (job_id, scheduled_for);

CREATE TABLE scheduler_run_leases (
    run_id           UUID PRIMARY KEY REFERENCES scheduler_runs (id) ON DELETE CASCADE,
    worker_id        TEXT NOT NULL,
    lease_token      UUID NOT NULL,
    lease_expires_at TIMESTAMPTZ NOT NULL,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX scheduler_run_leases_expiry_idx ON scheduler_run_leases (lease_expires_at);

CREATE TABLE scheduler_run_outcomes (
    run_id      UUID PRIMARY KEY REFERENCES scheduler_runs (id) ON DELETE CASCADE,
    outcome     run_outcome NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error  TEXT,
    CONSTRAINT error_iff_failed CHECK ((outcome = 'failed') = (last_error IS NOT NULL))
);

CREATE INDEX scheduler_jobs_due_idx ON scheduler_jobs (next_run_at) WHERE is_paused = false;

-- Trigger: finalizing a run atomically clears its lease (enforces lease XOR outcome).
CREATE FUNCTION scheduler_clear_lease_on_outcome() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM scheduler_run_leases WHERE run_id = NEW.run_id;
    RETURN NEW;
END;
$$;

CREATE TRIGGER scheduler_outcome_clears_lease
    AFTER INSERT ON scheduler_run_outcomes
    FOR EACH ROW EXECUTE FUNCTION scheduler_clear_lease_on_outcome();

-- Trigger: a terminal run can never be (re)leased.
CREATE FUNCTION scheduler_reject_lease_on_terminal() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM scheduler_run_outcomes WHERE run_id = NEW.run_id) THEN
        RAISE EXCEPTION 'cannot lease run %: already terminal', NEW.run_id;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER scheduler_lease_requires_open
    BEFORE INSERT ON scheduler_run_leases
    FOR EACH ROW EXECUTE FUNCTION scheduler_reject_lease_on_terminal();

-- Read model for inspection / admin.
CREATE VIEW scheduler_runs_status AS
SELECT r.id, r.job_id, r.scheduled_for, r.attempt_count,
       l.worker_id, l.lease_token, l.lease_expires_at, l.started_at,
       o.outcome, o.finished_at, o.last_error
FROM scheduler_runs r
LEFT JOIN scheduler_run_leases   l ON l.run_id = r.id
LEFT JOIN scheduler_run_outcomes o ON o.run_id = r.id;
