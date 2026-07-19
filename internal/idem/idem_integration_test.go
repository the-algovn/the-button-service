//go:build integration

package idem

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/the-algovn/the-button-service/internal/testutil"
)

func TestFirstSeeIsOncePerGroup(t *testing.T) {
	url := testutil.StartRedis(t)
	opt, err := redis.ParseURL(url)
	require.NoError(t, err)
	rdb := redis.NewClient(opt)
	defer rdb.Close()

	ctx := context.Background()

	first, err := FirstSee(ctx, rdb, "counter", "ch-1", time.Minute)
	require.NoError(t, err)
	require.True(t, first)

	again, err := FirstSee(ctx, rdb, "counter", "ch-1", time.Minute)
	require.NoError(t, err)
	require.False(t, again)

	// Different group tracks independently.
	other, err := FirstSee(ctx, rdb, "progress", "ch-1", time.Minute)
	require.NoError(t, err)
	require.True(t, other)
}
