-- +goose Up
-- Baseline. Production already has these tables (created by the pre-migration
-- idempotent startup apply) and no goose_db_version table, so goose runs this
-- migration, finds every object present, and records version 1 — a no-op. On a
-- fresh database (tests, a new environment) it creates them for real. The
-- IF NOT EXISTS is what makes both paths converge.
CREATE TABLE IF NOT EXISTS user_clicks (user_sub text PRIMARY KEY, clicks bigint NOT NULL);

CREATE TABLE IF NOT EXISTS user_achievements (
  user_sub       text NOT NULL,
  achievement_id text NOT NULL,
  unlocked_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (user_sub, achievement_id)
);

-- +goose Down
DROP TABLE IF EXISTS user_achievements;
DROP TABLE IF EXISTS user_clicks;
