-- +goose Up
CREATE TABLE IF NOT EXISTS counter_state (
  id    int    NOT NULL PRIMARY KEY DEFAULT 1,
  total bigint NOT NULL,
  CONSTRAINT counter_state_singleton CHECK (id = 1)
);

CREATE TABLE IF NOT EXISTS user_streak (
  user_sub  text NOT NULL PRIMARY KEY,
  cur_days  int  NOT NULL,
  best_days int  NOT NULL,
  last_day  text NOT NULL
);

-- +goose Down
DROP TABLE IF EXISTS user_streak;
DROP TABLE IF EXISTS counter_state;
