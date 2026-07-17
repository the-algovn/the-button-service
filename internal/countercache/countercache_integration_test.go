//go:build integration

package countercache

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/the-algovn/the-button-service/internal/db"
	"github.com/the-algovn/the-button-service/internal/store"
	"github.com/the-algovn/the-button-service/internal/testutil"
)

func TestCache_WarmupTotalsAndUsers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pgURL := testutil.StartPostgres(t)
	testutil.Migrate(t, pgURL)
	pool, err := store.NewPG(ctx, pgURL)
	require.NoError(t, err)
	defer pool.Close()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	_, err = db.New(pool).UpsertUserClicks(ctx, db.UpsertUserClicksParams{UserSub: "u1", Clicks: 3})
	require.NoError(t, err)
	_, err = db.New(pool).UpsertUserClicks(ctx, db.UpsertUserClicksParams{UserSub: "u2", Clicks: 4})
	require.NoError(t, err)

	c := &Cache{Pool: pool, Logger: logger}

	// not warmed before Run
	_, ok := c.Total()
	require.False(t, ok)
	_, ok = c.Users()
	require.False(t, ok)

	go c.Run(ctx)

	require.Eventually(t, func() bool {
		total, ok := c.Total()
		return ok && total == 7
	}, 10*time.Second, 100*time.Millisecond)
	require.Eventually(t, func() bool {
		users, ok := c.Users()
		return ok && users == 2
	}, 10*time.Second, 100*time.Millisecond)

	// a later write is picked up by the 1s poll
	_, err = db.New(pool).UpsertUserClicks(ctx, db.UpsertUserClicksParams{UserSub: "u1", Clicks: 5})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		total, _ := c.Total()
		return total == 12
	}, 10*time.Second, 100*time.Millisecond)
}
