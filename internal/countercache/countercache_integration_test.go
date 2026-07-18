//go:build integration

package countercache

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

func TestCache_ReadsRedisTotalAndUsers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rdb, err := store.NewRedis(ctx, testutil.StartRedis(t))
	require.NoError(t, err)
	defer rdb.Close()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	require.NoError(t, rdb.Set(ctx, "counter:total", 7, 0).Err())
	require.NoError(t, rdb.ZAdd(ctx, leaderboard.AllTimeKey,
		redis.Z{Score: 3, Member: "u1"}, redis.Z{Score: 4, Member: "u2"}).Err())

	c := &Cache{RDB: rdb, Logger: logger}
	_, ok := c.Total()
	require.False(t, ok)
	go c.Run(ctx)

	require.Eventually(t, func() bool { v, ok := c.Total(); return ok && v == 7 }, 10*time.Second, 100*time.Millisecond)
	require.Eventually(t, func() bool { v, ok := c.Users(); return ok && v == 2 }, 10*time.Second, 100*time.Millisecond)

	require.NoError(t, rdb.IncrBy(ctx, "counter:total", 5).Err())
	require.Eventually(t, func() bool { v, _ := c.Total(); return v == 12 }, 10*time.Second, 100*time.Millisecond)
}
