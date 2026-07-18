//go:build integration

package leaderboard

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTopN(t *testing.T) {
	rdb := redisForTest(t)
	ctx := context.Background()

	Sync(ctx, rdb,
		[]ScoreRow{{"a", 1000}, {"b", 500}, {"c", 1500}},
		nil, "lb:week:2026-07-13")

	top, err := TopN(ctx, rdb, AllTimeKey, 2)
	require.NoError(t, err)
	require.Len(t, top, 2)
	require.Equal(t, "c", top[0].Sub) // highest first
	require.Equal(t, uint64(1500), top[0].Clicks)
	require.Equal(t, "a", top[1].Sub)
}
