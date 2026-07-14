//go:build integration

package clicks

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/the-algovn/the-button-service/internal/pow"
	"github.com/the-algovn/the-button-service/internal/store"
	"github.com/the-algovn/the-button-service/internal/testutil"
)

var logger = slog.New(slog.NewTextHandler(os.Stderr, nil))

func setup(t *testing.T) (*redis.Client, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	pool, err := store.NewPG(ctx, testutil.StartPostgres(t))
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	rdb, err := store.NewRedis(ctx, testutil.StartRedis(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb, pool
}

func payload(id, sub string, minInterval uint32) pow.Payload {
	now := time.Now()
	return pow.Payload{
		ID: id, Sub: sub, Iat: now.Unix(), Exp: now.Add(pow.TokenTTL).Unix(),
		W0: 16384, L: 1, MinIntervalS: minInterval, MaxBatch: pow.MaxBatch,
	}
}

func unlockedIDs(res *Result) []string {
	out := make([]string, 0, len(res.Unlocked))
	for _, u := range res.Unlocked {
		out = append(out, u.Achievement.ID)
	}
	return out
}

func TestSubmit_HappyPathAndReplay(t *testing.T) {
	rdb, pool := setup(t)
	ctx := context.Background()
	p := payload("tok-1", "user-1", 1)

	res, err := Submit(ctx, rdb, pool, logger, p, 5, time.Now())
	require.NoError(t, err)
	require.EqualValues(t, 5, res.UserTotal)
	require.Contains(t, unlockedIDs(res), "mvh")

	// step 4 side effects: hot counter + controller signal
	require.Equal(t, "5", rdb.Get(ctx, "counter:global").Val())
	require.Equal(t, "1", rdb.Get(ctx, "stats:accepted_total").Val())

	// replay: the same challenge id is burned
	_, err = Submit(ctx, rdb, pool, logger, p, 5, time.Now())
	require.Equal(t, codes.AlreadyExists, status.Code(err))
}

func TestSubmit_ThrottleUnburnsToken(t *testing.T) {
	rdb, pool := setup(t)
	ctx := context.Background()

	_, err := Submit(ctx, rdb, pool, logger, payload("tok-a", "user-2", 1), 1, time.Now())
	require.NoError(t, err)

	// immediately again with a fresh token: throttled AND un-burned
	p2 := payload("tok-b", "user-2", 1)
	_, err = Submit(ctx, rdb, pool, logger, p2, 1, time.Now())
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Equal(t, int64(0), rdb.Exists(ctx, "pow:tok-b").Val())

	// after the interval the SAME token succeeds — client did not re-solve
	time.Sleep(1200 * time.Millisecond)
	res, err := Submit(ctx, rdb, pool, logger, p2, 1, time.Now())
	require.NoError(t, err)
	require.EqualValues(t, 2, res.UserTotal)
}

func TestSubmit_TxnFailureCompensates(t *testing.T) {
	rdb, pool := setup(t)
	ctx := context.Background()

	// sabotage the txn
	_, err := pool.Exec(ctx, `ALTER TABLE user_clicks RENAME TO user_clicks_broken`)
	require.NoError(t, err)

	p := payload("tok-c", "user-3", 1)
	_, err = Submit(ctx, rdb, pool, logger, p, 3, time.Now())
	require.Equal(t, codes.Unavailable, status.Code(err))
	// compensation removed both keys — the token is spendable again
	require.Equal(t, int64(0), rdb.Exists(ctx, "pow:tok-c", "throttle:user-3").Val())
	// and no counter bump happened for the failed attempt
	require.Equal(t, int64(0), rdb.Exists(ctx, "counter:global").Val())

	// heal and retry the SAME token: accepted
	_, err = pool.Exec(ctx, `ALTER TABLE user_clicks_broken RENAME TO user_clicks`)
	require.NoError(t, err)
	res, err := Submit(ctx, rdb, pool, logger, p, 3, time.Now())
	require.NoError(t, err)
	require.EqualValues(t, 3, res.UserTotal)
	require.Equal(t, "3", rdb.Get(ctx, "counter:global").Val())
}

func TestSubmit_Crosses69(t *testing.T) {
	rdb, pool := setup(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `INSERT INTO user_clicks (user_sub, clicks) VALUES ('user-4', 60)`)
	require.NoError(t, err)

	res, err := Submit(ctx, rdb, pool, logger, payload("tok-d", "user-4", 1), 10, time.Now())
	require.NoError(t, err)
	require.EqualValues(t, 70, res.UserTotal)
	got := unlockedIDs(res)
	require.Contains(t, got, "nice")
	require.NotContains(t, got, "mvh") // old=60 already past 1 — no re-award
}

func TestSubmit_BatchAchievementsOnceOnly(t *testing.T) {
	rdb, pool := setup(t)
	ctx := context.Background()

	res, err := Submit(ctx, rdb, pool, logger, payload("tok-e", "user-5", 1), 10_000, time.Now())
	require.NoError(t, err)
	got := unlockedIDs(res)
	for _, want := range []string{"mvh", "ten", "nice", "century", "blaze", "comma", "carpal", "bigbatch", "maxbatch"} {
		require.Contains(t, got, want)
	}
	for _, u := range res.Unlocked {
		require.False(t, u.UnlockedAt.IsZero())
	}

	// second max batch: bigbatch/maxbatch rows already exist → not re-unlocked
	time.Sleep(1200 * time.Millisecond) // clear throttle
	res2, err := Submit(ctx, rdb, pool, logger, payload("tok-f", "user-5", 1), 10_000, time.Now())
	require.NoError(t, err)
	require.EqualValues(t, 20_000, res2.UserTotal)
	for _, u := range res2.Unlocked {
		require.NotContains(t, []string{"bigbatch", "maxbatch"}, u.Achievement.ID)
	}
}

// TestSubmit_ConcurrentReplayExactlyOneWinner proves Redis SETNX, not an
// assumption, arbitrates concurrent replay of the SAME token: exactly one
// goroutine must see success and the other AlreadyExists.
func TestSubmit_ConcurrentReplayExactlyOneWinner(t *testing.T) {
	rdb, pool := setup(t)
	ctx := context.Background()
	p := payload("tok-race", "user-race", 1)

	const n = 8
	codesCh := make(chan codes.Code, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			_, err := Submit(ctx, rdb, pool, logger, p, 1, time.Now())
			codesCh <- status.Code(err)
		}()
	}
	wg.Wait()
	close(codesCh)

	var ok, replayed int
	for c := range codesCh {
		switch c {
		case codes.OK:
			ok++
		case codes.AlreadyExists:
			replayed++
		default:
			t.Fatalf("unexpected code: %v", c)
		}
	}
	require.Equal(t, 1, ok)
	require.Equal(t, n-1, replayed)
}
