//go:build integration

package snapshot

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/the-algovn/the-button-service/internal/leaderboard"
	"github.com/the-algovn/the-button-service/internal/store"
	"github.com/the-algovn/the-button-service/internal/testutil"
)

func TestSnapshotRestore_Parity(t *testing.T) {
	ctx := context.Background()
	pgURL := testutil.StartPostgres(t)
	testutil.Migrate(t, pgURL)
	pool, err := store.NewPG(ctx, pgURL)
	require.NoError(t, err)
	defer pool.Close()
	rdb, err := store.NewRedis(ctx, testutil.StartRedis(t))
	require.NoError(t, err)
	defer rdb.Close()

	now := time.Now()
	weekKey := leaderboard.WeekKey(now)
	require.NoError(t, rdb.Set(ctx, "counter:total", 123456, 0).Err())
	require.NoError(t, rdb.ZAdd(ctx, leaderboard.AllTimeKey,
		redis.Z{Member: "u1", Score: 150}, redis.Z{Member: "u2", Score: 999}).Err())
	require.NoError(t, rdb.ZAdd(ctx, weekKey, redis.Z{Member: "u1", Score: 40}).Err())
	require.NoError(t, rdb.HSet(ctx, "profile:names", "u1", "Ann", "u2", "Bob").Err())
	require.NoError(t, rdb.HSet(ctx, "unlocks:u1", "ten", now.Unix(), "palindrome", now.Unix()).Err())
	require.NoError(t, rdb.HSet(ctx, "streak:u1", "count", 3, "best", 5, "lastday", "2026-07-19").Err())

	s := &Snapshotter{Pool: pool, RDB: rdb, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil))}
	require.NoError(t, s.Snapshot(ctx))

	require.NoError(t, rdb.FlushAll(ctx).Err())
	require.EqualValues(t, 0, rdb.ZCard(ctx, leaderboard.AllTimeKey).Val())

	require.NoError(t, s.Restore(ctx))

	require.EqualValues(t, 123456, mustInt(t, rdb, "counter:total"))
	require.EqualValues(t, 999, rdb.ZScore(ctx, leaderboard.AllTimeKey, "u2").Val())
	require.EqualValues(t, 150, rdb.ZScore(ctx, leaderboard.AllTimeKey, "u1").Val())
	require.EqualValues(t, 40, rdb.ZScore(ctx, weekKey, "u1").Val())
	require.Equal(t, "Ann", rdb.HGet(ctx, "profile:names", "u1").Val())
	require.Equal(t, "Bob", rdb.HGet(ctx, "profile:names", "u2").Val())
	require.True(t, rdb.HExists(ctx, "unlocks:u1", "ten").Val())
	require.True(t, rdb.HExists(ctx, "unlocks:u1", "palindrome").Val())
	require.Equal(t, "3", rdb.HGet(ctx, "streak:u1", "count").Val())
	require.Equal(t, "5", rdb.HGet(ctx, "streak:u1", "best").Val())
	require.Equal(t, "2026-07-19", rdb.HGet(ctx, "streak:u1", "lastday").Val())

	// Restore is a no-op guard when Redis already has state.
	require.NoError(t, s.Restore(ctx))
	require.EqualValues(t, 999, rdb.ZScore(ctx, leaderboard.AllTimeKey, "u2").Val())
}

func mustInt(t *testing.T, rdb *redis.Client, key string) int64 {
	t.Helper()
	v, err := rdb.Get(context.Background(), key).Int64()
	require.NoError(t, err)
	return v
}
