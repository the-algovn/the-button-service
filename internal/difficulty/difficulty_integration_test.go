//go:build integration

package difficulty

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/the-algovn/the-button-service/internal/store"
	"github.com/the-algovn/the-button-service/internal/testutil"
)

func TestCache_RefreshesFromRedis(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rdb, err := store.NewRedis(ctx, testutil.StartRedis(t))
	require.NoError(t, err)
	defer rdb.Close()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	c := &Cache{RDB: rdb, Logger: logger}
	_, _, ok := c.Get()
	require.False(t, ok, "not warmed before the keys exist / Run starts")

	require.NoError(t, rdb.Set(ctx, "pow:L", 3, 0).Err())
	require.NoError(t, rdb.Set(ctx, "pow:min_interval", 5, 0).Err())
	go c.Run(ctx)

	require.Eventually(t, func() bool {
		l, mi, ok := c.Get()
		return ok && l == 3 && mi == 5
	}, 5*time.Second, 100*time.Millisecond)

	// a later change is picked up by the 1s refresh
	require.NoError(t, rdb.Set(ctx, "pow:L", 7, 0).Err())
	require.Eventually(t, func() bool {
		l, _, _ := c.Get()
		return l == 7
	}, 5*time.Second, 100*time.Millisecond)
}
