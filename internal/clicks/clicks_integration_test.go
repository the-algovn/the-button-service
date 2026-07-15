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

// setup returns a Redis client with counter:global already seeded to 0 —
// the normal steady state assumed by these tests (Redis lost/not-yet-seeded
// is its own scenario, covered separately in the ticker package's
// TestApply_DoesNotCreateCounterKey).
func setup(t *testing.T) (*redis.Client, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	pool, err := store.NewPG(ctx, testutil.StartPostgres(t))
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	rdb, err := store.NewRedis(ctx, testutil.StartRedis(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	require.NoError(t, rdb.Set(ctx, "counter:global", 0, 0).Err())
	return rdb, pool
}

// payload's ID must be a valid uuid (the outbox table's primary key column
// is uuid — spec §7), so it is generated here rather than taken as a
// caller-supplied label.
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

func TestSubmit_HappyPathAndReplay(t *testing.T) {
	rdb, pool := setup(t)
	ctx := context.Background()
	p := payload("user-1", 1)

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

// TestSubmit_AppliesCounterAndClearsOutbox proves the happy path of the
// transactional outbox (spec §6): after a successful Submit, counter:global
// equals the batch's clicks, the idempotency marker applied:<id> exists, and
// the outbox row inserted alongside the user_clicks upsert is gone.
func TestSubmit_AppliesCounterAndClearsOutbox(t *testing.T) {
	rdb, pool := setup(t)
	ctx := context.Background()
	p := payload("user-outbox", 1)

	res, err := Submit(ctx, rdb, pool, logger, p, 7, time.Now())
	require.NoError(t, err)
	require.EqualValues(t, 7, res.UserTotal)

	require.Equal(t, "7", rdb.Get(ctx, "counter:global").Val())
	require.Equal(t, int64(1), rdb.Exists(ctx, "applied:"+p.ID).Val())

	var remaining int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM counter_outbox WHERE id = $1`, p.ID).Scan(&remaining))
	require.Zero(t, remaining, "outbox row must be deleted after a successful apply")
}

func TestSubmit_ThrottleUnburnsToken(t *testing.T) {
	rdb, pool := setup(t)
	ctx := context.Background()

	_, err := Submit(ctx, rdb, pool, logger, payload("user-2", 1), 1, time.Now())
	require.NoError(t, err)

	// immediately again with a fresh token: throttled AND un-burned
	p2 := payload("user-2", 1)
	_, err = Submit(ctx, rdb, pool, logger, p2, 1, time.Now())
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Equal(t, int64(0), rdb.Exists(ctx, "pow:"+p2.ID).Val())

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

	p := payload("user-3", 1)
	_, err = Submit(ctx, rdb, pool, logger, p, 3, time.Now())
	require.Equal(t, codes.Unavailable, status.Code(err))
	// compensation removed both keys — the token is spendable again
	require.Equal(t, int64(0), rdb.Exists(ctx, "pow:"+p.ID, "throttle:user-3").Val())
	// and no counter bump happened for the failed attempt (setup seeds
	// counter:global to 0; it must still read 0 after the failed txn)
	require.Equal(t, "0", rdb.Get(ctx, "counter:global").Val())

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
	_, err := db.New(pool).UpsertUserClicks(ctx, db.UpsertUserClicksParams{UserSub: "user-4", Clicks: 60})
	require.NoError(t, err)

	res, err := Submit(ctx, rdb, pool, logger, payload("user-4", 1), 10, time.Now())
	require.NoError(t, err)
	require.EqualValues(t, 70, res.UserTotal)
	got := unlockedIDs(res)
	require.Contains(t, got, "nice")
	require.NotContains(t, got, "mvh") // old=60 already past 1 — no re-award
}

func TestSubmit_BatchAchievementsOnceOnly(t *testing.T) {
	rdb, pool := setup(t)
	ctx := context.Background()

	res, err := Submit(ctx, rdb, pool, logger, payload("user-5", 1), 10_000, time.Now())
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
	res2, err := Submit(ctx, rdb, pool, logger, payload("user-5", 1), 10_000, time.Now())
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
	_, err := applyBatch(dctx, pool, uuid.New().String(), "user-ambig-direct", 1, time.Now())
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
	_, err := Submit(sctx, rdb, pool, logger, p, 1, time.Now())
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
