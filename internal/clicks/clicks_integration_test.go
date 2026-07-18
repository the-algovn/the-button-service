//go:build integration

package clicks

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/the-algovn/the-button-service/internal/leaderboard"
	"github.com/the-algovn/the-button-service/internal/store"
	"github.com/the-algovn/the-button-service/internal/testutil"
)

func rdbOnly(t *testing.T) *redis.Client {
	t.Helper()
	rdb, err := store.NewRedis(context.Background(), testutil.StartRedis(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func TestSubmit_AcceptedThenReplay(t *testing.T) {
	ctx := context.Background()
	rdb := rdbOnly(t)
	p := hp("user-1", 1)

	res, err := Submit(ctx, rdb, p, 5, time.Now(), "tester")
	require.NoError(t, err)
	require.EqualValues(t, 5, res.UserTotal)
	require.EqualValues(t, 5, int(rdb.ZScore(ctx, leaderboard.AllTimeKey, "user-1").Val()))
	require.EqualValues(t, 1, rdb.XLen(ctx, "clicks:events").Val())

	_, err = Submit(ctx, rdb, p, 5, time.Now(), "tester")
	require.Equal(t, codes.AlreadyExists, status.Code(err))
}

func TestSubmit_ThrottleUnburns(t *testing.T) {
	ctx := context.Background()
	rdb := rdbOnly(t)

	_, err := Submit(ctx, rdb, hp("user-2", 1), 1, time.Now(), "tester")
	require.NoError(t, err)

	p2 := hp("user-2", 1)
	_, err = Submit(ctx, rdb, p2, 1, time.Now(), "tester")
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.EqualValues(t, 0, rdb.Exists(ctx, "pow:"+p2.ID).Val())
}

func TestSubmit_ConcurrentReplayExactlyOneWinner(t *testing.T) {
	ctx := context.Background()
	rdb := rdbOnly(t)
	p := hp("user-race", 1)

	const n = 8
	codesCh := make(chan codes.Code, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			_, err := Submit(ctx, rdb, p, 1, time.Now(), "tester")
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
