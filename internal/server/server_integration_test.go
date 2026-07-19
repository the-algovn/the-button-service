//go:build integration

package server

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	buttonv2 "github.com/the-algovn/protos/gen/go/algovn/button/v2"
	"github.com/the-algovn/the-button-service/internal/achievements"
	"github.com/the-algovn/the-button-service/internal/leaderboard"
	"github.com/the-algovn/the-button-service/internal/store"
	"github.com/the-algovn/the-button-service/internal/testutil"
)

func newRDB(t *testing.T) *redis.Client {
	t.Helper()
	rdb, err := store.NewRedis(context.Background(), testutil.StartRedis(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func TestGetCounter_ReadsRedis(t *testing.T) {
	ctx := context.Background()
	rdb := newRDB(t)
	// no clicks yet -> 0, not Unavailable (clean slate).
	s := &Server{RDB: rdb, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil))}
	resp, err := s.GetCounter(ctx, &buttonv2.GetCounterRequest{})
	require.NoError(t, err)
	require.EqualValues(t, 0, resp.GetTotal())
	require.EqualValues(t, 0, resp.GetTotalUsers())

	require.NoError(t, rdb.Set(ctx, "counter:total", 1_204_882, 0).Err())
	require.NoError(t, rdb.ZAdd(ctx, leaderboard.AllTimeKey,
		redis.Z{Member: "u1", Score: 900_000}, redis.Z{Member: "u2", Score: 304_882}).Err())
	resp, err = s.GetCounter(ctx, &buttonv2.GetCounterRequest{})
	require.NoError(t, err)
	require.EqualValues(t, 1_204_882, resp.GetTotal())
	require.EqualValues(t, 2, resp.GetTotalUsers())
}

func TestGetPlayerState(t *testing.T) {
	ctx := context.Background()
	rdb := newRDB(t)
	now := time.Now()
	date := dateHCM(now)
	weekStart := leaderboard.WeekStartString(now)
	weekKey := leaderboard.WeekKey(now)

	// two players so u1 is rank 2 all-time, rank 1 weekly.
	require.NoError(t, rdb.ZAdd(ctx, leaderboard.AllTimeKey,
		redis.Z{Member: "u1", Score: 150}, redis.Z{Member: "u2", Score: 999}).Err())
	require.NoError(t, rdb.ZAdd(ctx, weekKey, redis.Z{Member: "u1", Score: 150}).Err())
	// unlocks: an achievement (ten) and a troll id (palindrome — not in catalog).
	require.NoError(t, rdb.HSet(ctx, "unlocks:u1", "ten", now.Unix(), "palindrome", now.Unix()).Err())
	require.NoError(t, rdb.Set(ctx, "milestone:1000", now.Unix(), 0).Err())
	require.NoError(t, rdb.HSet(ctx, "streak:u1", "count", 3, "best", 5, "lastday", date).Err())
	require.NoError(t, rdb.HSet(ctx, "daily:u1:"+date, "clicks", 150, "batches", 4, "maxbatch", 60).Err())
	require.NoError(t, rdb.SAdd(ctx, "weekdays:u1:"+weekStart, date).Err())

	s := &Server{RDB: rdb, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil))}
	resp, err := s.GetPlayerState(authCtx("u1"), &buttonv2.GetPlayerStateRequest{})
	require.NoError(t, err)

	require.EqualValues(t, 150, resp.GetTotalClicks())
	require.EqualValues(t, 2, resp.GetAllTimeRank())
	require.EqualValues(t, 1, resp.GetWeeklyRank())

	// full catalog returned; "ten" carries an unlocked_at, "mvh" does not.
	byID := map[string]*buttonv2.Achievement{}
	for _, a := range resp.GetAchievements() {
		byID[a.GetId()] = a
	}
	require.Len(t, resp.GetAchievements(), len(achievements.Catalog))
	require.NotNil(t, byID["ten"].GetUnlockedAt())
	require.Nil(t, byID["mvh"].GetUnlockedAt())
	// troll id is NOT in the catalog list (hidden; announced live via sse.user).
	require.Nil(t, byID["palindrome"])

	require.Len(t, resp.GetMilestones(), 1)
	require.EqualValues(t, 1000, resp.GetMilestones()[0].GetThreshold())

	require.Len(t, resp.GetQuests(), 5) // 3 daily + 2 weekly
	for _, q := range resp.GetQuests() {
		require.NotZero(t, q.GetKind())
		require.NotNil(t, q.GetResetsAt())
	}

	require.EqualValues(t, 3, resp.GetStreak().GetCurrentDays())
	require.EqualValues(t, 5, resp.GetStreak().GetBestDays())
	require.Equal(t, date, resp.GetStreak().GetLastContribDate())
}
