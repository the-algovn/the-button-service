package leaderboard

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// Rank returns sub's 1-based rank in key without mutating anything, or 0 if
// unranked. Used by the read-only GetLeaderboard path (which must not ZADD).
func Rank(ctx context.Context, rdb redis.Cmdable, key, sub string) uint32 {
	return revRank(ctx, rdb, key, sub)
}

func revRank(ctx context.Context, rdb redis.Cmdable, key, sub string) uint32 {
	r, err := rdb.ZRevRank(ctx, key, sub).Result()
	if err != nil {
		return 0 // not present / redis error -> unranked
	}
	return uint32(r) + 1 // ZRevRank is 0-based
}
