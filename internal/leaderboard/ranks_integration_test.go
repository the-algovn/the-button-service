//go:build integration

package leaderboard

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/the-algovn/the-button-service/internal/store"
	"github.com/the-algovn/the-button-service/internal/testutil"
)

func redisForTest(t *testing.T) *redis.Client {
	t.Helper()
	rdb, err := store.NewRedis(context.Background(), testutil.StartRedis(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func TestSelfRank(t *testing.T) {
	rdb := redisForTest(t)
	ctx := context.Background()
	now := time.Now()

	// two existing users ahead of the caller, all-time
	rdb.ZAdd(ctx, AllTimeKey, redis.Z{Score: 1000, Member: "a"}, redis.Z{Score: 500, Member: "b"})
	rdb.ZAdd(ctx, WeekKey(now), redis.Z{Score: 40, Member: "a"})

	at, wk := SelfRank(ctx, rdb, "me", 600, 90, now)
	require.Equal(t, uint32(2), at) // 1000(a) > 600(me) > 500(b) -> rank 2
	require.Equal(t, uint32(1), wk) // 90(me) > 40(a) -> rank 1

	// the caller's scores were persisted into both zsets (self-fresh)
	require.Equal(t, 600.0, rdb.ZScore(ctx, AllTimeKey, "me").Val())
	require.Equal(t, 90.0, rdb.ZScore(ctx, WeekKey(now), "me").Val())
}
