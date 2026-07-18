-- +goose Up
CREATE TABLE IF NOT EXISTS user_weekly_clicks (
  user_sub   text   NOT NULL,
  week_start date   NOT NULL,
  clicks     bigint NOT NULL,
  PRIMARY KEY (user_sub, week_start)
);
CREATE INDEX IF NOT EXISTS user_weekly_clicks_week_idx ON user_weekly_clicks (week_start);

CREATE TABLE IF NOT EXISTS user_profile (
  user_sub     text        NOT NULL PRIMARY KEY,
  display_name text        NOT NULL,
  updated_at   timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS user_profile;
DROP TABLE IF EXISTS user_weekly_clicks;
