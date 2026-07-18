package leaderboard

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// SelfRank writes the caller's own fresh scores into both sorted sets and
// returns their 1-based ranks. It makes the caller's rank reflect the click
// just committed rather than a stale publisher snapshot; the publisher's next
// full refresh re-writes the same values from Postgres truth (idempotent).
//
// Ranks are display-only: any Redis error yields 0 (unranked) and never
// propagates, so a committed submit is never failed by a ranking hiccup.
func SelfRank(ctx context.Context, rdb redis.Cmdable, sub string, allTime, weekly uint64, now time.Time) (uint32, uint32) {
	weekKey := WeekKey(now)
	rdb.ZAdd(ctx, AllTimeKey, redis.Z{Score: float64(allTime), Member: sub})
	if z := rdb.ZAdd(ctx, weekKey, redis.Z{Score: float64(weekly), Member: sub}); z.Err() == nil {
		rdb.Expire(ctx, weekKey, WeekTTL) // refresh the week TTL on write
	}
	return revRank(ctx, rdb, AllTimeKey, sub), revRank(ctx, rdb, weekKey, sub)
}

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
