//go:build integration

package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/the-algovn/the-button-service/internal/db"
	"github.com/the-algovn/the-button-service/internal/migrate"
	"github.com/the-algovn/the-button-service/internal/testutil"
)

// TestMigrate_FreshDatabaseIsIdempotent proves migrations build the schema on a
// fresh DB and that re-applying is a no-op (the PreSync Job runs on EVERY Argo
// sync, not only schema-changing ones).
func TestMigrate_FreshDatabaseIsIdempotent(t *testing.T) {
	url := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	testutil.Migrate(t, url)
	testutil.Migrate(t, url) // second run: no pending migrations, must not error

	pool, err := NewPG(ctx, url)
	require.NoError(t, err)
	defer pool.Close()

	_, err = db.New(pool).UpsertUserClicks(ctx, db.UpsertUserClicksParams{UserSub: "u1", Clicks: 5})
	require.NoError(t, err)
	_, err = db.New(pool).InsertUserAchievement(ctx, db.InsertUserAchievementParams{UserSub: "u1", AchievementID: "mvh"})
	require.NoError(t, err)

	var clicks int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT clicks FROM user_clicks WHERE user_sub = 'u1'`).Scan(&clicks))
	require.EqualValues(t, 5, clicks)
	var unlockedAt time.Time
	require.NoError(t, pool.QueryRow(ctx, `SELECT unlocked_at FROM user_achievements WHERE user_sub = 'u1'`).Scan(&unlockedAt))
	require.WithinDuration(t, time.Now(), unlockedAt, time.Minute)

	// 002 must leave no counter_outbox behind on a fresh database.
	var n int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.tables WHERE table_name = 'counter_outbox'`).Scan(&n))
	require.Zero(t, n, "counter_outbox must not exist after migrations")
}

// TestMigrate_ProductionBaseline is the load-bearing test (spec): it simulates
// the LIVE database — tables already present with data, a populated
// counter_outbox, and no goose_db_version table — and proves the baseline is
// safe: 001 no-ops over existing tables, 002 drops the outbox, and no click
// data is touched.
func TestMigrate_ProductionBaseline(t *testing.T) {
	url := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool, err := NewPG(ctx, url)
	require.NoError(t, err)
	defer pool.Close()

	// Recreate the pre-migration production schema exactly as the retired
	// startup apply left it, including counter_outbox WITH a row.
	_, err = pool.Exec(ctx, `
CREATE TABLE user_clicks (user_sub text PRIMARY KEY, clicks bigint NOT NULL);
CREATE TABLE user_achievements (
  user_sub       text NOT NULL,
  achievement_id text NOT NULL,
  unlocked_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (user_sub, achievement_id)
);
CREATE TABLE counter_outbox (
  id         uuid PRIMARY KEY,
  clicks     bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX counter_outbox_created_at_idx ON counter_outbox (created_at);
INSERT INTO user_clicks (user_sub, clicks) VALUES ('live-user', 79);
INSERT INTO user_achievements (user_sub, achievement_id) VALUES ('live-user', 'mvh');
INSERT INTO counter_outbox (id, clicks) VALUES ('11111111-1111-1111-1111-111111111111', 3);`)
	require.NoError(t, err)

	testutil.Migrate(t, url)

	// The live click data survived untouched — this is what must never break.
	var clicks int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT clicks FROM user_clicks WHERE user_sub = 'live-user'`).Scan(&clicks))
	require.EqualValues(t, 79, clicks, "migrations must not touch existing click data")
	var achievements int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM user_achievements WHERE user_sub = 'live-user'`).Scan(&achievements))
	require.Equal(t, 1, achievements)

	// 002 dropped the outbox even though it had a row.
	var n int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.tables WHERE table_name = 'counter_outbox'`).Scan(&n))
	require.Zero(t, n, "002 must drop counter_outbox from the live baseline")

	// goose recorded both versions.
	var version int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT max(version_id) FROM goose_db_version`).Scan(&version))
	require.EqualValues(t, 2, version)
}

// TestMigrate_ConcurrentRunnersSerialize proves the session locker holds: two
// runners racing (a re-synced Job overlapping a retry) must both succeed with
// the schema applied exactly once.
//
// Uses MigrateE, not Migrate: require.* calls t.FailNow(), which Go forbids
// outside the test's own goroutine. Errors are collected and asserted after
// the wait.
func TestMigrate_ConcurrentRunnersSerialize(t *testing.T) {
	url := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range 4 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = testutil.MigrateE(url)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "concurrent runner %d failed", i)
	}

	pool, err := NewPG(ctx, url)
	require.NoError(t, err)
	defer pool.Close()

	var applied int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM goose_db_version WHERE version_id > 0`).Scan(&applied))
	require.Equal(t, 2, applied, "each migration must be recorded exactly once")
}

// TestMigrate_DownRecreatesCounterOutbox proves the runbook's rollback claim
// for real: rolling a fresh, fully-migrated database back by one migration
// recreates counter_outbox — the exact scenario a rollback to a pre-split
// image (whose write path INSERTs there on every accepted batch) needs.
func TestMigrate_DownRecreatesCounterOutbox(t *testing.T) {
	url := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	testutil.Migrate(t, url) // applies 001+002; counter_outbox is dropped by 002

	pool, err := NewPG(ctx, url)
	require.NoError(t, err)
	defer pool.Close()

	var before int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.tables WHERE table_name = 'counter_outbox'`).Scan(&before))
	require.Zero(t, before, "precondition: counter_outbox must be dropped before rollback")

	reversed, err := migrate.Down(ctx, url)
	require.NoError(t, err)
	require.Contains(t, reversed, "version=2", "Down must reverse migration 002, the most recently applied")

	var after int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.tables WHERE table_name = 'counter_outbox'`).Scan(&after))
	require.Equal(t, 1, after, "migrate -down must recreate counter_outbox")

	// goose_db_version reflects the reversal: version 2's row is gone, leaving
	// 1 as the highest recorded version.
	var version int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT max(version_id) FROM goose_db_version`).Scan(&version))
	require.EqualValues(t, 1, version, "goose_db_version must reflect the reversal of migration 002")
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
