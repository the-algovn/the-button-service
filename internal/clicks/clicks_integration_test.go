//go:build integration

package clicks

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/the-algovn/the-button-service/internal/db"
	"github.com/the-algovn/the-button-service/internal/pow"
	"github.com/the-algovn/the-button-service/internal/store"
	"github.com/the-algovn/the-button-service/internal/testutil"
)

var logger = slog.New(slog.NewTextHandler(os.Stderr, nil))

// setup returns a Redis client and a PG pool. No counter seeding: since the
// outbox removal, Redis holds no counter state — Postgres is the only
// counter truth.
func setup(t *testing.T) (*redis.Client, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	pgURL := testutil.StartPostgres(t)
	testutil.Migrate(t, pgURL)
	pool, err := store.NewPG(ctx, pgURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	rdb, err := store.NewRedis(ctx, testutil.StartRedis(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb, pool
}

// payload's ID stays a uuid: it is the Redis burn key ("pow:<id>") and shows
// up in logs — uuid keeps those unambiguous even though nothing enforces it.
func payload(sub string, minInterval uint32) pow.Payload {
	now := time.Now()
	return pow.Payload{
		ID: uuid.New().String(), Sub: sub, Iat: now.Unix(), Exp: now.Add(pow.TokenTTL).Unix(),
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

func sumClicks(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int64 {
	t.Helper()
	n, err := db.New(pool).SumUserClicks(ctx)
	require.NoError(t, err)
	return n
}

func TestSubmit_HappyPathAndReplay(t *testing.T) {
	rdb, pool := setup(t)
	ctx := context.Background()
	p := payload("user-1", 1)

	res, err := Submit(ctx, rdb, pool, logger, p, 5, time.Now(), "tester")
	require.NoError(t, err)
	require.EqualValues(t, 5, res.UserTotal)
	require.Contains(t, unlockedIDs(res), "mvh")

	// side effects: durable truth + controller signal
	require.EqualValues(t, 5, sumClicks(t, ctx, pool))
	require.Equal(t, "1", rdb.Get(ctx, "stats:accepted_total").Val())

	// replay: the same challenge id is burned
	_, err = Submit(ctx, rdb, pool, logger, p, 5, time.Now(), "tester")
	require.Equal(t, codes.AlreadyExists, status.Code(err))
}

func TestSubmit_ThrottleUnburnsToken(t *testing.T) {
	rdb, pool := setup(t)
	ctx := context.Background()

	_, err := Submit(ctx, rdb, pool, logger, payload("user-2", 1), 1, time.Now(), "tester")
	require.NoError(t, err)

	// immediately again with a fresh token: throttled AND un-burned
	p2 := payload("user-2", 1)
	_, err = Submit(ctx, rdb, pool, logger, p2, 1, time.Now(), "tester")
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Equal(t, int64(0), rdb.Exists(ctx, "pow:"+p2.ID).Val())

	// after the interval the SAME token succeeds — client did not re-solve
	time.Sleep(1200 * time.Millisecond)
	res, err := Submit(ctx, rdb, pool, logger, p2, 1, time.Now(), "tester")
	require.NoError(t, err)
	require.EqualValues(t, 2, res.UserTotal)
}

func TestSubmit_TxnFailureCompensates(t *testing.T) {
	rdb, pool := setup(t)
	ctx := context.Background()

	// sabotage the txn
	_, err := pool.Exec(ctx, `ALTER TABLE user_clicks RENAME TO user_clicks_broken`)
	require.NoError(t, err)

	p := payload("user-3", 1)
	_, err = Submit(ctx, rdb, pool, logger, p, 3, time.Now(), "tester")
	require.Equal(t, codes.Unavailable, status.Code(err))
	// compensation removed both keys — the token is spendable again
	require.Equal(t, int64(0), rdb.Exists(ctx, "pow:"+p.ID, "throttle:user-3").Val())

	// heal and retry the SAME token: accepted
	_, err = pool.Exec(ctx, `ALTER TABLE user_clicks_broken RENAME TO user_clicks`)
	require.NoError(t, err)
	// and no counter bump happened for the failed attempt
	require.Zero(t, sumClicks(t, ctx, pool))
	res, err := Submit(ctx, rdb, pool, logger, p, 3, time.Now(), "tester")
	require.NoError(t, err)
	require.EqualValues(t, 3, res.UserTotal)
	require.EqualValues(t, 3, sumClicks(t, ctx, pool))
}

func TestSubmit_Crosses69(t *testing.T) {
	rdb, pool := setup(t)
	ctx := context.Background()
	_, err := db.New(pool).UpsertUserClicks(ctx, db.UpsertUserClicksParams{UserSub: "user-4", Clicks: 60})
	require.NoError(t, err)

	res, err := Submit(ctx, rdb, pool, logger, payload("user-4", 1), 10, time.Now(), "tester")
	require.NoError(t, err)
	require.EqualValues(t, 70, res.UserTotal)
	got := unlockedIDs(res)
	require.Contains(t, got, "nice")
	require.NotContains(t, got, "mvh") // old=60 already past 1 — no re-award
}

func TestSubmit_BatchAchievementsOnceOnly(t *testing.T) {
	rdb, pool := setup(t)
	ctx := context.Background()

	res, err := Submit(ctx, rdb, pool, logger, payload("user-5", 1), 10_000, time.Now(), "tester")
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
	res2, err := Submit(ctx, rdb, pool, logger, payload("user-5", 1), 10_000, time.Now(), "tester")
	require.NoError(t, err)
	require.EqualValues(t, 20_000, res2.UserTotal)
	for _, u := range res2.Unlocked {
		require.NotContains(t, []string{"bigbatch", "maxbatch"}, u.Achievement.ID)
	}
}

// installCommitBlocker adds a deferred constraint trigger on user_clicks
// that blocks at COMMIT time by waiting on an advisory lock. A test holds
// that lock from a separate transaction, so Begin+Insert always succeed
// while Commit itself reliably fails once the caller's context deadline
// expires — reproducing "the caller's deadline expired ... after the
// durable work already landed" deterministically, with no wall-clock race
// against Redis/Postgres round trips.
const commitBlockLockKey = 918273645

func installCommitBlocker(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `CREATE OR REPLACE FUNCTION test_block_commit() RETURNS trigger AS $$
		BEGIN PERFORM pg_advisory_xact_lock(`+strconv.Itoa(commitBlockLockKey)+`); RETURN NULL; END;
		$$ LANGUAGE plpgsql`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `CREATE CONSTRAINT TRIGGER block_commit AFTER INSERT OR UPDATE ON user_clicks
		DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION test_block_commit()`)
	require.NoError(t, err)
}

// holdCommitBlockLock acquires the advisory lock installCommitBlocker's
// trigger waits on, from its own transaction, and returns a release func.
func holdCommitBlockLock(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (release func()) {
	t.Helper()
	holder, err := pool.Begin(ctx)
	require.NoError(t, err)
	_, err = holder.Exec(ctx, `SELECT pg_advisory_xact_lock(`+strconv.Itoa(commitBlockLockKey)+`)`)
	require.NoError(t, err)
	return func() { _ = holder.Rollback(context.Background()) }
}

// TestApplyBatch_CommitErrorWrappedAmbiguous proves applyBatch wraps a
// commit failure — and only a commit failure, i.e. one where Begin and the
// upsert already succeeded — with errCommitAmbiguous.
func TestApplyBatch_CommitErrorWrappedAmbiguous(t *testing.T) {
	_, pool := setup(t)
	ctx := context.Background()
	installCommitBlocker(t, ctx, pool)
	release := holdCommitBlockLock(t, ctx, pool)
	defer release()

	dctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	_, err := applyBatch(dctx, pool, "user-ambig-direct", 1, time.Now(), "tester")
	require.Error(t, err)
	require.ErrorIs(t, err, errCommitAmbiguous)
}

// TestSubmit_AmbiguousCommitKeepsBurn proves a commit whose outcome is
// unknown does NOT release the PoW burn or the throttle key: replaying the
// token could otherwise credit the same clicks twice, and there is no
// batch-level idempotency key to catch it. Uses the same deterministic
// commit-blocking trigger as TestApplyBatch_CommitErrorWrappedAmbiguous so
// Submit's commit reliably fails via context deadline, not a timing race.
func TestSubmit_AmbiguousCommitKeepsBurn(t *testing.T) {
	rdb, pool := setup(t)
	ctx := context.Background()
	installCommitBlocker(t, ctx, pool)
	release := holdCommitBlockLock(t, ctx, pool)
	defer release()

	p := payload("user-ambig", 1)
	sctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	_, err := Submit(sctx, rdb, pool, logger, p, 1, time.Now(), "tester")
	require.Error(t, err)

	// Both keys MUST still be present (fail-closed) — a retry of this token
	// must be rejected as a replay rather than credited again.
	exists, rerr := rdb.Exists(context.Background(), "pow:"+p.ID, "throttle:user-ambig").Result()
	require.NoError(t, rerr)
	require.EqualValues(t, 2, exists, "ambiguous commit must keep the PoW burn and throttle key")
}

// TestSubmit_ConcurrentReplayExactlyOneWinner proves Redis SETNX, not an
// assumption, arbitrates concurrent replay of the SAME token: exactly one
// goroutine must see success and the other AlreadyExists.
func TestSubmit_ConcurrentReplayExactlyOneWinner(t *testing.T) {
	rdb, pool := setup(t)
	ctx := context.Background()
	p := payload("user-race", 1)

	const n = 8
	codesCh := make(chan codes.Code, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			_, err := Submit(ctx, rdb, pool, logger, p, 1, time.Now(), "tester")
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
