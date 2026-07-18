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

func TestPersister_ReconcileHealsCounterMonotonically(t *testing.T) {
	ctx := context.Background()
	pgURL := testutil.StartPostgres(t)
	testutil.Migrate(t, pgURL)
	pool, err := store.NewPG(ctx, pgURL)
	require.NoError(t, err)
	defer pool.Close()
	rdb, err := store.NewRedis(ctx, testutil.StartRedis(t))
	require.NoError(t, err)
	defer rdb.Close()

	require.NoError(t, rdb.ZAdd(ctx, leaderboard.AllTimeKey,
		redis.Z{Score: 70, Member: "u1"}, redis.Z{Score: 30, Member: "u2"}).Err())
	p := &Persister{Pool: pool, RDB: rdb, Logger: logger}

	// counter:total is low (disaster) -> heal UP to the ZSET sum (100)
	require.NoError(t, rdb.Set(ctx, "counter:total", 40, 0).Err())
	p.reconcile(ctx)
	require.Equal(t, "100", rdb.Get(ctx, "counter:total").Val())

	// counter:total already >= sum -> reconcile must NOT lower it (monotonic)
	require.NoError(t, rdb.Set(ctx, "counter:total", 150, 0).Err())
	p.reconcile(ctx)
	require.Equal(t, "150", rdb.Get(ctx, "counter:total").Val(), "reconcile must never move the counter backward")
}

func TestPersister_AchievementClaimRolledBackOnInsertFailure(t *testing.T) {
	ctx := context.Background()
	pgURL := testutil.StartPostgres(t)
	testutil.Migrate(t, pgURL)
	pool, err := store.NewPG(ctx, pgURL)
	require.NoError(t, err)
	defer pool.Close()
	rdb, err := store.NewRedis(ctx, testutil.StartRedis(t))
	require.NoError(t, err)
	defer rdb.Close()
	p := &Persister{Pool: pool, RDB: rdb, Logger: logger}

	// break the insert target so InsertUserAchievementAt fails
	_, err = pool.Exec(ctx, `ALTER TABLE user_achievements RENAME TO user_achievements_broken`)
	require.NoError(t, err)

	m := redis.XMessage{ID: "1-0", Values: map[string]any{
		"sub": "u1", "count": "1", "total": "1", "ts": "1752800000"}}
	require.False(t, p.process(ctx, m), "process must return false so the event redelivers")
	require.EqualValues(t, 0, rdb.Exists(ctx, "ach:u1:mvh").Val(), "the claim must be rolled back, not left set")

	// heal the table; reprocessing now persists the achievement
	_, err = pool.Exec(ctx, `ALTER TABLE user_achievements_broken RENAME TO user_achievements`)
	require.NoError(t, err)
	require.True(t, p.process(ctx, m))
	rows, err := db.New(pool).ListUserAchievements(ctx, "u1")
	require.NoError(t, err)
	require.Len(t, rows, 1)
}
