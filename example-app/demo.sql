-- Deliberately NO unique constraint on task_key: the exactly-once demo must be
-- kept to one row per order by the transaction alone, not by a constraint.
CREATE TABLE demo_orders (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_key     TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The relay records a message it cannot decode here, and leaves published_at
-- for delivered rows only.
CREATE TABLE outbox_rejected (
    outbox_id   BIGINT PRIMARY KEY REFERENCES queue_outbox (id) ON DELETE CASCADE,
    error       TEXT NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
