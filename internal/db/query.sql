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
