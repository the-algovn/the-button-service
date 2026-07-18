//go:build integration

package clicks

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/the-algovn/the-button-service/internal/leaderboard"
	"github.com/the-algovn/the-button-service/internal/pow"
	"github.com/the-algovn/the-button-service/internal/store"
	"github.com/the-algovn/the-button-service/internal/testutil"
)

func hp(sub string, minInterval uint32) pow.Payload {
	now := time.Now()
	return pow.Payload{ID: uuid.New().String(), Sub: sub, Iat: now.Unix(),
		Exp: now.Add(pow.TokenTTL).Unix(), W0: 16384, L: 1, MinIntervalS: minInterval, MaxBatch: pow.MaxBatch}
}

func TestRunHotPath_SideEffectsAndCodes(t *testing.T) {
	ctx := context.Background()
	rdb, err := store.NewRedis(ctx, testutil.StartRedis(t))
	require.NoError(t, err)
	defer rdb.Close()
	now := time.Now()

	// first accepted batch
	p := hp("user-1", 1)
	res, err := runHotPath(ctx, rdb, p, 5, now, "Tester")
	require.NoError(t, err)
	require.Equal(t, "ok", res.Status)
	require.EqualValues(t, 5, res.UserTotal)

	require.EqualValues(t, 5, int(rdb.ZScore(ctx, leaderboard.AllTimeKey, "user-1").Val()))
	require.EqualValues(t, 5, int(rdb.ZScore(ctx, leaderboard.WeekKey(now), "user-1").Val()))
	require.Equal(t, "5", rdb.Get(ctx, "counter:total").Val())
	require.Equal(t, "Tester", rdb.HGet(ctx, "profile:names", "user-1").Val())
	require.Equal(t, "1", rdb.Get(ctx, "stats:accepted_total").Val())
	require.EqualValues(t, 1, rdb.XLen(ctx, "clicks:events").Val())

	// replay of the same token
	res, err = runHotPath(ctx, rdb, p, 5, now, "Tester")
	require.NoError(t, err)
	require.Equal(t, "replay", res.Status)

	// a fresh token for the same user within the interval -> throttled, un-burned
	p2 := hp("user-1", 1)
	res, err = runHotPath(ctx, rdb, p2, 1, now, "Tester")
	require.NoError(t, err)
	require.Equal(t, "throttled", res.Status)
	require.EqualValues(t, 0, rdb.Exists(ctx, "pow:"+p2.ID).Val(), "throttle must un-burn")
	require.EqualValues(t, 5, int(rdb.ZScore(ctx, leaderboard.AllTimeKey, "user-1").Val()), "no counter change on throttle")
}
