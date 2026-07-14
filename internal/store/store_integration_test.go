//go:build integration

package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/the-algovn/the-button-service/internal/testutil"
)

func TestNewPG_SchemaIdempotent(t *testing.T) {
	url := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool, err := NewPG(ctx, url)
	require.NoError(t, err)
	defer pool.Close()

	// Second application must be a no-op (CREATE TABLE IF NOT EXISTS).
	_, err = pool.Exec(ctx, Schema)
	require.NoError(t, err)

	// Both tables exist and accept the spec §7 shapes.
	_, err = pool.Exec(ctx, `INSERT INTO user_clicks (user_sub, clicks) VALUES ('u1', 5)`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO user_achievements (user_sub, achievement_id) VALUES ('u1', 'mvh')`)
	require.NoError(t, err)

	var clicks int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT clicks FROM user_clicks WHERE user_sub = 'u1'`).Scan(&clicks))
	require.EqualValues(t, 5, clicks)
	var unlockedAt time.Time
	require.NoError(t, pool.QueryRow(ctx, `SELECT unlocked_at FROM user_achievements WHERE user_sub = 'u1'`).Scan(&unlockedAt))
	require.WithinDuration(t, time.Now(), unlockedAt, time.Minute)
}

func TestNewPG_ConcurrentSchemaApply(t *testing.T) {
	// Two replicas starting at once against a fresh DB must both succeed.
	url := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range 4 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p, err := NewPG(ctx, url)
			if err != nil {
				errs[i] = err
				return
			}
			p.Close()
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "replica %d failed to apply schema concurrently", i)
	}
}

func TestNewRedis_Ping(t *testing.T) {
	url := testutil.StartRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	rdb, err := NewRedis(ctx, url)
	require.NoError(t, err)
	defer rdb.Close()
	require.NoError(t, rdb.Set(ctx, "k", "v", time.Minute).Err())
	require.Equal(t, "v", rdb.Get(ctx, "k").Val())
}
