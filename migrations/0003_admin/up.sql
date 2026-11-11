-- 1. cancelled status: a terminal state beside done and dead.
ALTER TABLE queue_tasks DROP CONSTRAINT queue_tasks_status_check;
ALTER TABLE queue_tasks ADD CONSTRAINT queue_tasks_status_check
    CHECK (status IN ('queued', 'running', 'done', 'dead', 'cancelled'));

ALTER TABLE queue_tasks DROP CONSTRAINT queue_tasks_check1;
ALTER TABLE queue_tasks ADD CONSTRAINT queue_tasks_check1
    CHECK (
        (status IN ('done', 'dead', 'cancelled') AND completed_at IS NOT NULL)
        OR
        (status IN ('queued', 'running') AND completed_at IS NULL)
    );

ALTER TABLE queue_task_attempts DROP CONSTRAINT queue_task_attempts_outcome_check;
ALTER TABLE queue_task_attempts ADD CONSTRAINT queue_task_attempts_outcome_check
    CHECK (outcome IN ('done', 'retry', 'dead', 'lease_expired', 'released', 'cancelled'));

DROP INDEX queue_tasks_terminal_cleanup_idx;
CREATE INDEX queue_tasks_terminal_cleanup_idx
    ON queue_tasks (completed_at ASC, id ASC)
    WHERE status IN ('done', 'dead', 'cancelled');
