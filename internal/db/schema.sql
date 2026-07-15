CREATE TABLE IF NOT EXISTS user_clicks (user_sub text PRIMARY KEY, clicks bigint NOT NULL);

CREATE TABLE IF NOT EXISTS user_achievements (
  user_sub       text NOT NULL,
  achievement_id text NOT NULL,
  unlocked_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (user_sub, achievement_id)
);

CREATE TABLE IF NOT EXISTS counter_outbox (
  id         uuid PRIMARY KEY,
  clicks     bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS counter_outbox_created_at_idx ON counter_outbox (created_at);
