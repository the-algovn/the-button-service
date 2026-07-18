package leaderboard

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// ScoreRow is a (user, clicks) pair read from Postgres for a ZSET refresh.
type ScoreRow struct {
	Sub    string
	Clicks uint64
}

// Sync writes both sorted sets from Postgres rows (idempotent full refresh) and
// refreshes the week key's TTL. A missing week (nil rows) is a no-op for that
// set. Errors are best-effort: the read model self-heals next cycle.
func Sync(ctx context.Context, rdb redis.Cmdable, allTime, week []ScoreRow, weekKey string) {
	if len(allTime) > 0 {
		zs := make([]redis.Z, len(allTime))
		for i, r := range allTime {
			zs[i] = redis.Z{Score: float64(r.Clicks), Member: r.Sub}
		}
		rdb.ZAdd(ctx, AllTimeKey, zs...)
	}
	if len(week) > 0 {
		zs := make([]redis.Z, len(week))
		for i, r := range week {
			zs[i] = redis.Z{Score: float64(r.Clicks), Member: r.Sub}
		}
		rdb.ZAdd(ctx, weekKey, zs...)
		rdb.Expire(ctx, weekKey, WeekTTL)
	}
}

// TopN reads the top n (sub, clicks) from a sorted set, highest first.
func TopN(ctx context.Context, rdb redis.Cmdable, key string, n int64) ([]ScoreRow, error) {
	zs, err := rdb.ZRevRangeWithScores(ctx, key, 0, n-1).Result()
	if err != nil {
		return nil, err
	}
	out := make([]ScoreRow, len(zs))
	for i, z := range zs {
		out[i] = ScoreRow{Sub: z.Member.(string), Clicks: uint64(z.Score)}
	}
	return out, nil
}
