-- Deliberately NO unique constraint on task_key: the exactly-once demo must be
-- kept to one row per order by the transaction alone, not by a constraint.
CREATE TABLE demo_orders (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_key     TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
