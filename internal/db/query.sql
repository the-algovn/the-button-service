-- name: UpsertUserClicks :one
INSERT INTO user_clicks AS u (user_sub, clicks) VALUES ($1, $2)
ON CONFLICT (user_sub) DO UPDATE SET clicks = u.clicks + $2
RETURNING clicks;

-- name: InsertUserAchievement :one
INSERT INTO user_achievements (user_sub, achievement_id) VALUES ($1, $2)
ON CONFLICT DO NOTHING
RETURNING unlocked_at;

-- name: ListUserAchievements :many
SELECT achievement_id, unlocked_at FROM user_achievements WHERE user_sub = $1;

-- name: SumUserClicks :one
SELECT COALESCE(SUM(clicks), 0)::bigint AS total FROM user_clicks;

-- name: CountUsers :one
SELECT COUNT(*) AS count FROM user_clicks;

-- name: GetUserClicks :one
SELECT clicks FROM user_clicks WHERE user_sub = $1;

-- name: UpsertUserWeeklyClicks :one
INSERT INTO user_weekly_clicks AS w (user_sub, week_start, clicks) VALUES ($1, $2, $3)
ON CONFLICT (user_sub, week_start) DO UPDATE SET clicks = w.clicks + $3
RETURNING clicks;

-- name: UpsertUserProfile :exec
INSERT INTO user_profile AS p (user_sub, display_name, updated_at) VALUES ($1, $2, now())
ON CONFLICT (user_sub) DO UPDATE SET display_name = $2, updated_at = now();

-- name: ListAllUserClicks :many
SELECT user_sub, clicks FROM user_clicks;

-- name: ListWeekUserClicks :many
SELECT user_sub, clicks FROM user_weekly_clicks WHERE week_start = $1;

-- name: ListProfileNames :many
SELECT user_sub, display_name FROM user_profile WHERE user_sub = ANY($1::text[]);

-- name: BatchUpsertUserClicks :exec
-- Absolute set (not increment): the caller passes the authoritative Redis
-- ZSET score, so re-running a flush is idempotent.
INSERT INTO user_clicks AS u (user_sub, clicks)
SELECT unnest(@subs::text[]), unnest(@clicks::bigint[])
ON CONFLICT (user_sub) DO UPDATE SET clicks = EXCLUDED.clicks;

-- name: BatchUpsertUserWeeklyClicks :exec
INSERT INTO user_weekly_clicks AS w (user_sub, week_start, clicks)
SELECT unnest(@subs::text[]), @week_start::date, unnest(@clicks::bigint[])
ON CONFLICT (user_sub, week_start) DO UPDATE SET clicks = EXCLUDED.clicks;

-- name: BatchUpsertUserProfile :exec
INSERT INTO user_profile AS p (user_sub, display_name, updated_at)
SELECT unnest(@subs::text[]), unnest(@names::text[]), now()
ON CONFLICT (user_sub) DO UPDATE SET display_name = EXCLUDED.display_name, updated_at = now();

-- name: InsertUserAchievementAt :exec
INSERT INTO user_achievements (user_sub, achievement_id, unlocked_at) VALUES ($1, $2, $3)
ON CONFLICT DO NOTHING;
