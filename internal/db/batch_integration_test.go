//go:build integration

package db_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/the-algovn/the-button-service/internal/db"
	"github.com/the-algovn/the-button-service/internal/store"
	"github.com/the-algovn/the-button-service/internal/testutil"
)

func TestBatchUpserts_AbsoluteAndIdempotent(t *testing.T) {
	ctx := context.Background()
	pgURL := testutil.StartPostgres(t)
	testutil.Migrate(t, pgURL)
	pool, err := store.NewPG(ctx, pgURL)
	require.NoError(t, err)
	defer pool.Close()
	q := db.New(pool)

	week := pgtype.Date{Time: time.Date(2026, 7, 13, 0, 0, 0, 0, time.UTC), Valid: true}
	up := func() {
		require.NoError(t, q.BatchUpsertUserClicks(ctx, db.BatchUpsertUserClicksParams{
			Subs: []string{"a", "b"}, Clicks: []int64{10, 20}}))
		require.NoError(t, q.BatchUpsertUserWeeklyClicks(ctx, db.BatchUpsertUserWeeklyClicksParams{
			Subs: []string{"a", "b"}, WeekStart: week, Clicks: []int64{3, 4}}))
		require.NoError(t, q.BatchUpsertUserProfile(ctx, db.BatchUpsertUserProfileParams{
			Subs: []string{"a", "b"}, Names: []string{"Ann", "Bob"}}))
	}
	up()
	up() // second run must be a no-op, not a double-count

	got, err := q.GetUserClicks(ctx, "a")
	require.NoError(t, err)
	require.EqualValues(t, 10, got, "absolute set, not +10+10")

	require.NoError(t, q.InsertUserAchievementAt(ctx, db.InsertUserAchievementAtParams{
		UserSub: "a", AchievementID: "mvh", UnlockedAt: time.Now()}))
	require.NoError(t, q.InsertUserAchievementAt(ctx, db.InsertUserAchievementAtParams{
		UserSub: "a", AchievementID: "mvh", UnlockedAt: time.Now()})) // ON CONFLICT DO NOTHING
	rows, err := q.ListUserAchievements(ctx, "a")
	require.NoError(t, err)
	require.Len(t, rows, 1)
}
