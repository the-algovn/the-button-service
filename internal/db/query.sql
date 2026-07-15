-- name: UpsertUserClicks :one
INSERT INTO user_clicks AS u (user_sub, clicks) VALUES ($1, $2)
ON CONFLICT (user_sub) DO UPDATE SET clicks = u.clicks + $2
RETURNING clicks;

-- name: InsertOutbox :exec
INSERT INTO counter_outbox (id, clicks) VALUES ($1, $2);

-- name: InsertUserAchievement :one
INSERT INTO user_achievements (user_sub, achievement_id) VALUES ($1, $2)
ON CONFLICT DO NOTHING
RETURNING unlocked_at;

-- name: DeleteOutboxByID :exec
DELETE FROM counter_outbox WHERE id = $1;

-- name: ListUserAchievements :many
SELECT achievement_id, unlocked_at FROM user_achievements WHERE user_sub = $1;

-- name: SumUserClicks :one
SELECT COALESCE(SUM(clicks), 0)::bigint AS total FROM user_clicks;

-- name: SumUserClicksNow :one
SELECT COALESCE(SUM(clicks), 0)::bigint AS total, now()::timestamptz AS now FROM user_clicks;

-- name: CountUsers :one
SELECT COUNT(*) AS count FROM user_clicks;

-- name: CountOutbox :one
SELECT COUNT(*) AS count FROM counter_outbox;

-- name: ListOutboxBefore :many
SELECT id FROM counter_outbox WHERE created_at <= $1;

-- name: DeleteOutboxBefore :exec
DELETE FROM counter_outbox WHERE created_at <= $1;

-- name: ListSweepableOutbox :many
SELECT id, clicks, created_at FROM counter_outbox
WHERE created_at < now() - interval '30 seconds'
ORDER BY created_at
LIMIT $1;
