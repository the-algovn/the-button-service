//go:build integration

package leaderboard

import (
	"context"
	"testing"

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

