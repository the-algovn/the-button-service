-- +goose Up
-- Retires the manual cleanup left pending by the 2026-07-17 outbox removal.
-- On production this drops the real (0-row) table; on a fresh database it is a
-- no-op, since 001 never creates it. Safe under the expand/contract rule only
-- because the api/publisher split already shipped: nothing running reads this
-- table.
DROP TABLE IF EXISTS counter_outbox;

-- +goose Down
-- Recreates the original DDL so a rollback to a pre-split image (whose write
-- path INSERTs here on every accepted batch) has a table to write to.
CREATE TABLE IF NOT EXISTS counter_outbox (
  id         uuid PRIMARY KEY,
  clicks     bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS counter_outbox_created_at_idx ON counter_outbox (created_at);
