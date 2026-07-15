//go:build integration

package ticker

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/the-algovn/the-button-service/internal/db"
	"github.com/the-algovn/the-button-service/internal/store"
	"github.com/the-algovn/the-button-service/internal/testutil"
)

func TestRefreshUsers_CountsUserClicksRows(t *testing.T) {
	ctx := context.Background()
	pgURL := testutil.StartPostgres(t)
	pool, err := store.NewPG(ctx, pgURL)
	require.NoError(t, err)
	defer pool.Close()

	q := db.New(pool)
	for _, u := range []struct {
		sub    string
		clicks int64
	}{{"a", 10}, {"b", 20}, {"c", 5}} {
		_, err = q.UpsertUserClicks(ctx, db.UpsertUserClicksParams{UserSub: u.sub, Clicks: u.clicks})
		require.NoError(t, err)
	}

	tk := &Ticker{Pool: pool, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil))}

	if _, ok := tk.Users(); ok {
		t.Fatal("Users() should report not-loaded before the first refresh")
	}

	tk.refreshUsers(ctx)

	users, ok := tk.Users()
	require.True(t, ok)
	require.Equal(t, uint64(3), users)
}
