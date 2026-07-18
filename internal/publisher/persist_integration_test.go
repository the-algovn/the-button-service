//go:build integration

package publisher

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/the-algovn/the-button-service/internal/db"
	"github.com/the-algovn/the-button-service/internal/leaderboard"
	"github.com/the-algovn/the-button-service/internal/store"
	"github.com/the-algovn/the-button-service/internal/testutil"
)

func TestPersister_FlushesAndAwardsFromStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
	// simulate what the hot-path script writes for a 70-click user
	require.NoError(t, rdb.ZAdd(ctx, leaderboard.AllTimeKey, redis.Z{Score: 70, Member: "user-x"}).Err())
	require.NoError(t, rdb.ZAdd(ctx, weekKey, redis.Z{Score: 70, Member: "user-x"}).Err())
	require.NoError(t, rdb.HSet(ctx, "profile:names", "user-x", "Xavier").Err())
	require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: eventsStream, Values: map[string]any{
		"sub": "user-x", "count": "70", "total": "70", "ts": strconv.FormatInt(now.Unix(), 10),
	}}).Err())

	p := &Persister{Pool: pool, RDB: rdb, Logger: logger}
	go p.Run(ctx)

	// durable mirror catches up (absolute score)
	require.Eventually(t, func() bool {
		v, err := db.New(pool).GetUserClicks(ctx, "user-x")
		return err == nil && v == 70
	}, 15*time.Second, 200*time.Millisecond)

	// achievements the 70-click batch earned are persisted (mvh, ten, nice)
	require.Eventually(t, func() bool {
		rows, err := db.New(pool).ListUserAchievements(ctx, "user-x")
		if err != nil {
			return false
		}
		got := map[string]bool{}
		for _, r := range rows {
			got[r.AchievementID] = true
		}
		return got["mvh"] && got["ten"] && got["nice"]
	}, 15*time.Second, 200*time.Millisecond)

	// profile name flushed
	var name string
	require.NoError(t, pool.QueryRow(ctx, `SELECT display_name FROM user_profile WHERE user_sub=$1`, "user-x").Scan(&name))
	require.Equal(t, "Xavier", name)
}

func TestPersister_ReconcileAwardsThresholdBackstop(t *testing.T) {
	ctx := context.Background()
	pgURL := testutil.StartPostgres(t)
	testutil.Migrate(t, pgURL)
	pool, err := store.NewPG(ctx, pgURL)
	require.NoError(t, err)
	defer pool.Close()
	rdb, err := store.NewRedis(ctx, testutil.StartRedis(t))
	require.NoError(t, err)
	defer rdb.Close()

	// a user with a durable ZSET score but NO stream event (a dropped event)
	require.NoError(t, rdb.ZAdd(ctx, leaderboard.AllTimeKey, redis.Z{Score: 100, Member: "backstop-user"}).Err())

	p := &Persister{Pool: pool, RDB: rdb, Logger: logger}
	p.reconcile(ctx)

	rows, err := db.New(pool).ListUserAchievements(ctx, "backstop-user")
	require.NoError(t, err)
	got := map[string]bool{}
	for _, r := range rows {
		got[r.AchievementID] = true
	}
	require.True(t, got["mvh"] && got["ten"] && got["century"], "reconcile must backstop threshold achievements")
	require.Equal(t, "100", rdb.Get(ctx, "counter:total").Val())
}
