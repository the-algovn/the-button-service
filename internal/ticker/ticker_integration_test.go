//go:build integration

package ticker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	"github.com/the-algovn/the-button-service/internal/clicks"
	"github.com/the-algovn/the-button-service/internal/pow"
	"github.com/the-algovn/the-button-service/internal/store"
	"github.com/the-algovn/the-button-service/internal/testutil"
)

// Two replicas race the advisory lock; only one may claim + announce each
// milestone even while both loops run (spec §8: SETNX exactly-once claim).
func TestMilestone_ExactlyOnceAcrossTwoReplicas(t *testing.T) {
	pgURL := testutil.StartPostgres(t)
	redisURL := testutil.StartRedis(t)
	amqpURL := testutil.StartRabbit(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := store.NewPG(ctx, pgURL)
	require.NoError(t, err)
	defer pool.Close()
	rdb, err := store.NewRedis(ctx, redisURL)
	require.NoError(t, err)
	defer rdb.Close()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// milestone-worthy durable state; counter:global is absent so the
	// leader must seed it from SUM(user_clicks)
	_, err = pool.Exec(ctx, `INSERT INTO user_clicks (user_sub, clicks) VALUES ('seed', 1500)`)
	require.NoError(t, err)

	conn, err := amqp.Dial(amqpURL)
	require.NoError(t, err)
	defer conn.Close()
	ch, err := conn.Channel()
	require.NoError(t, err)
	require.NoError(t, ch.ExchangeDeclare("events", "topic", true, false, false, false, nil))
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.QueueBind(q.Name, "the-button.counter", "events", false, nil))
	msgs, err := ch.Consume(q.Name, "", true, true, false, false, nil)
	require.NoError(t, err)

	var mu sync.Mutex // amqp channels are not publish-concurrency-safe
	publish := func(channel string, body []byte) {
		mu.Lock()
		defer mu.Unlock()
		_ = ch.PublishWithContext(ctx, "events", channel, false, false,
			amqp.Publishing{ContentType: "application/json", Body: body})
	}

	// two replicas race for leadership on dedicated connections
	for range 2 {
		tk := &Ticker{PGURL: pgURL, Pool: pool, RDB: rdb, Publish: publish, Logger: logger}
		go tk.Run(ctx)
	}

	milestones, counters := 0, 0
	timeout := time.After(8 * time.Second)
collect:
	for {
		select {
		case m := <-msgs:
			var ev struct {
				Type      string `json:"type"`
				Threshold uint64 `json:"threshold"`
			}
			require.NoError(t, json.Unmarshal(m.Body, &ev))
			switch {
			case ev.Type == "milestone" && ev.Threshold == 1000:
				milestones++
			case ev.Type == "counter":
				counters++
			}
		case <-timeout:
			break collect
		}
	}
	require.Equal(t, 1, milestones, "milestone 1000 must be announced exactly once")
	require.GreaterOrEqual(t, counters, 1, "leader must publish the seeded total")
	// the claim persists and difficulty keys were initialized
	require.Equal(t, int64(1), rdb.Exists(ctx, "milestone:1000").Val())
	require.Equal(t, "1", rdb.Get(ctx, "pow:L").Val())
	require.Equal(t, "2", rdb.Get(ctx, "pow:min_interval").Val())
}

// sweepPayload builds a minimal signed-in-spirit challenge payload for
// driving clicks.Submit directly from ticker's integration tests (no server
// layer involved), mirroring clicks_integration_test.go's own helper.
func sweepPayload(sub string) pow.Payload {
	now := time.Now()
	return pow.Payload{
		ID: uuid.New().String(), Sub: sub, Iat: now.Unix(), Exp: now.Add(pow.TokenTTL).Unix(),
		W0: 16384, L: 1, MinIntervalS: 1, MaxBatch: pow.MaxBatch,
	}
}

// TestSweep_HealsLostApplyExactlyOnce proves the outbox sweeper (spec §8)
// heals a batch whose post-commit apply never happened — a crash, an
// ambiguous commit, or a Redis blip between COMMIT and the idempotent Lua
// apply — and that re-sweeping is a no-op (the sweeper's apply is keyed by
// the same "applied:<id>" marker as the write path, so it can never
// double-apply). This is the case the old diff-based reconcile could not
// distinguish from a batch merely in flight.
func TestSweep_HealsLostApplyExactlyOnce(t *testing.T) {
	pgURL := testutil.StartPostgres(t)
	redisURL := testutil.StartRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool, err := store.NewPG(ctx, pgURL)
	require.NoError(t, err)
	defer pool.Close()
	rdb, err := store.NewRedis(ctx, redisURL)
	require.NoError(t, err)
	defer rdb.Close()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	tk := &Ticker{PGURL: pgURL, Pool: pool, RDB: rdb, Logger: logger}

	// Steady state: counter:global already seeded (Fix A means Submit's own
	// apply would otherwise leave the outbox row in place since the counter
	// doesn't exist yet — that scenario is covered separately by
	// TestApply_DoesNotCreateCounterKey below).
	require.NoError(t, rdb.Set(ctx, "counter:global", 0, 0).Err())

	p := sweepPayload("sweep-user")
	res, err := clicks.Submit(ctx, rdb, pool, logger, p, 42, time.Now())
	require.NoError(t, err)
	require.EqualValues(t, 42, res.UserTotal)

	// Simulate a crash between COMMIT and apply: undo the apply's visible
	// effects (the idempotency marker and the counter bump). Submit's own
	// apply already deleted the outbox row on this successful run, so
	// reinstate it backdated outside the in-flight window — exactly the row
	// a crash there would have left behind (the commit landed, the apply and
	// its outbox delete never ran).
	require.NoError(t, rdb.Del(ctx, "applied:"+p.ID).Err())
	require.NoError(t, rdb.Set(ctx, "counter:global", 0, 0).Err())
	_, err = pool.Exec(ctx,
		`INSERT INTO counter_outbox (id, clicks, created_at) VALUES ($1, $2, now() - interval '5 minutes')`,
		p.ID, 42)
	require.NoError(t, err)

	require.NoError(t, tk.sweep(ctx))

	var sum int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT COALESCE(SUM(clicks), 0) FROM user_clicks`).Scan(&sum))
	got, err := rdb.Get(ctx, "counter:global").Int64()
	require.NoError(t, err)
	require.EqualValues(t, sum, got, "sweep must heal counter:global to the durable sum")

	var remaining int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM counter_outbox WHERE id = $1`, p.ID).Scan(&remaining))
	require.Zero(t, remaining, "outbox row must be deleted once applied")

	// Idempotency: sweeping again must not change the counter.
	require.NoError(t, tk.sweep(ctx))
	got2, err := rdb.Get(ctx, "counter:global").Int64()
	require.NoError(t, err)
	require.Equal(t, got, got2, "re-sweeping must not double-apply")
}

// TestSweep_NeverDoubleCountsInFlightSubmits is the test the old diff-based
// reconcile could not pass: at the design's target load, Redis structurally
// lags Postgres by the in-flight window, so a diff observed during live
// traffic is essentially always non-zero and positive, and "healing" it
// double-counts the in-flight batch's own pending apply. The outbox sidesteps
// the diff entirely — the sweeper only ever looks at rows outside the
// in-flight window, so it can run concurrently with live Submits without
// ever touching one of them, and the counter lands exactly on the durable sum
// once traffic quiesces.
func TestSweep_NeverDoubleCountsInFlightSubmits(t *testing.T) {
	pgURL := testutil.StartPostgres(t)
	redisURL := testutil.StartRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool, err := store.NewPG(ctx, pgURL)
	require.NoError(t, err)
	defer pool.Close()
	rdb, err := store.NewRedis(ctx, redisURL)
	require.NoError(t, err)
	defer rdb.Close()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	tk := &Ticker{PGURL: pgURL, Pool: pool, RDB: rdb, Logger: logger}

	// Steady state: counter:global already seeded — no leader loop runs in
	// this test, only sweep() called directly, so nothing else would seed it.
	require.NoError(t, rdb.Set(ctx, "counter:global", 0, 0).Err())

	stop := make(chan struct{})
	var sweepWG sync.WaitGroup
	sweepWG.Add(1)
	go func() {
		defer sweepWG.Done()
		st := time.NewTicker(50 * time.Millisecond)
		defer st.Stop()
		for {
			select {
			case <-stop:
				return
			case <-st.C:
				_ = tk.sweep(ctx)
			}
		}
	}()

	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			p := sweepPayload(fmt.Sprintf("sweep-race-user-%d", i))
			_, err := clicks.Submit(ctx, rdb, pool, logger, p, 3, time.Now())
			require.NoError(t, err)
		}(i)
	}
	wg.Wait()
	close(stop)
	sweepWG.Wait()

	// Quiescence: every Submit applies and deletes its own outbox row
	// synchronously, so the outbox drains almost immediately — no need to
	// wait out the sweeper's 30s in-flight window.
	require.Eventually(t, func() bool {
		var remaining int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM counter_outbox`).Scan(&remaining))
		return remaining == 0
	}, 15*time.Second, 200*time.Millisecond)

	var sum int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT COALESCE(SUM(clicks), 0) FROM user_clicks`).Scan(&sum))
	got, err := rdb.Get(ctx, "counter:global").Int64()
	require.NoError(t, err)
	require.EqualValues(t, sum, got, "counter:global must equal SUM(user_clicks) exactly at quiescence")
}

// TestLead_DemotesOnStalledConnection covers Fix 2. A genuine half-open TCP
// stall cannot be simulated deterministically without a network proxy (none
// is wired into this project's test containers, and adding one is out of
// scope here), so — per the task's fallback — this instead asserts the
// dedicated leader connection is configured with the server-side
// statement_timeout that makes a stuck statement fail fast: below the 5s
// self-demote SLA, so a stalled connection is detected rather than waited on.
func TestLead_DemotesOnStalledConnection(t *testing.T) {
	pgURL := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := store.NewPG(ctx, pgURL)
	require.NoError(t, err)
	defer pool.Close()

	tk := &Ticker{PGURL: pgURL, Pool: pool}
	conn, err := tk.dialLeaderConn(ctx)
	require.NoError(t, err)
	defer conn.Close(ctx) //nolint:errcheck // test cleanup

	var timeout string
	require.NoError(t, conn.QueryRow(ctx, `SHOW statement_timeout`).Scan(&timeout))
	require.Equal(t, "4s", timeout, "leader connection must carry a statement_timeout below the 5s self-demote SLA")
}

// TestApply_DoesNotCreateCounterKey proves Fix A. With counter:global absent
// (the state right after a Redis data loss — PVC loss, AOF truncation,
// FLUSHALL — with Postgres still holding the durable total), a stray apply
// must never spring the key into existence: INCRBY on a missing key would
// create it at just this one batch's clicks, which permanently defeats the
// leader's GET-succeeds-so-never-seed check and pins the public counter near
// zero while Postgres holds millions. A Submit racing that window must still
// succeed (the batch is durably committed either way) but leave both the
// counter key and its own outbox row untouched — only the leader's seed may
// create counter:global, and it must land at exactly SUM(user_clicks).
func TestApply_DoesNotCreateCounterKey(t *testing.T) {
	pgURL := testutil.StartPostgres(t)
	redisURL := testutil.StartRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool, err := store.NewPG(ctx, pgURL)
	require.NoError(t, err)
	defer pool.Close()
	rdb, err := store.NewRedis(ctx, redisURL)
	require.NoError(t, err)
	defer rdb.Close()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	tk := &Ticker{PGURL: pgURL, Pool: pool, RDB: rdb, Logger: logger}

	// Durable history Redis knows nothing about — the point of the seed.
	_, err = pool.Exec(ctx, `INSERT INTO user_clicks (user_sub, clicks) VALUES ('preexisting', 1000)`)
	require.NoError(t, err)
	require.Equal(t, int64(0), rdb.Exists(ctx, "counter:global").Val())

	// A concurrent Submit lands in this exact window: counter:global absent.
	p := sweepPayload("wipe-user")
	res, err := clicks.Submit(ctx, rdb, pool, logger, p, 42, time.Now())
	require.NoError(t, err)
	require.EqualValues(t, 42, res.UserTotal)

	// The batch is committed and correct, but the counter must STILL be
	// absent — no stray creation — and its outbox row must remain, since
	// nothing can apply until the leader seeds.
	require.Equal(t, int64(0), rdb.Exists(ctx, "counter:global").Val())
	var remaining int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM counter_outbox WHERE id = $1`, p.ID).Scan(&remaining))
	require.Equal(t, 1, remaining, "outbox row must remain until the counter is seeded")

	// The leader's seed path runs next: this must be the only thing allowed
	// to create counter:global, landing exactly on the durable sum.
	total, err := tk.leaderTotal(ctx)
	require.NoError(t, err)

	var sum int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT COALESCE(SUM(clicks), 0) FROM user_clicks`).Scan(&sum))
	require.EqualValues(t, sum, total, "seeded total must equal SUM(user_clicks) exactly")
	got, err := rdb.Get(ctx, "counter:global").Int64()
	require.NoError(t, err)
	require.EqualValues(t, sum, got)
}

// TestSeedPurge_MarksAppliedSoLateApplyIsNoop proves Fix B. A batch that
// committed just before the leader's seed timestamp may still have its
// post-commit apply in flight (e.g. go-redis retrying across the very Redis
// restart that triggered the seed): its clicks are already inside the seeded
// SUM, so if the seed purge simply deleted its outbox row, the delayed apply
// landing afterward would find applied:<id> gone (wiped with Redis) and
// INCRBY again — double-counting with no row left to notice. The purge must
// mark each row's applied:<id> BEFORE deleting it, so that late apply is a
// no-op.
func TestSeedPurge_MarksAppliedSoLateApplyIsNoop(t *testing.T) {
	pgURL := testutil.StartPostgres(t)
	redisURL := testutil.StartRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool, err := store.NewPG(ctx, pgURL)
	require.NoError(t, err)
	defer pool.Close()
	rdb, err := store.NewRedis(ctx, redisURL)
	require.NoError(t, err)
	defer rdb.Close()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	tk := &Ticker{PGURL: pgURL, Pool: pool, RDB: rdb, Logger: logger}

	// Steady state before the Redis-loss event: the batch commits and
	// applies normally (counter:global already seeded), so its outbox row
	// is deleted by Submit's own successful apply.
	require.NoError(t, rdb.Set(ctx, "counter:global", 0, 0).Err())
	p := sweepPayload("delayed-apply-user")
	res, err := clicks.Submit(ctx, rdb, pool, logger, p, 7, time.Now())
	require.NoError(t, err)
	require.EqualValues(t, 7, res.UserTotal)

	// Simulate that same batch's apply instead still being in flight across
	// the upcoming wipe: reinstate its outbox row (Submit's own apply already
	// deleted it on this successful run) created at-or-before the seed's own
	// read timestamp — exactly the row a delayed apply would have left
	// behind.
	_, err = pool.Exec(ctx,
		`INSERT INTO counter_outbox (id, clicks, created_at) VALUES ($1, $2, now())`,
		p.ID, 7)
	require.NoError(t, err)

	// Redis loses everything: the marker and the counter are both gone.
	require.NoError(t, rdb.FlushAll(ctx).Err())

	// Leader seed runs: SETNX wins and purges outbox rows at-or-before its
	// own read timestamp, including the reinstated row — marking it applied
	// first.
	total, err := tk.leaderTotal(ctx)
	require.NoError(t, err)
	var sum int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT COALESCE(SUM(clicks), 0) FROM user_clicks`).Scan(&sum))
	require.EqualValues(t, sum, total)

	// The delayed apply finally lands — must be a no-op: the marker the
	// purge stamped already claims it.
	require.NoError(t, store.ApplyCounter(ctx, rdb, p.ID, 7))

	got, err := rdb.Get(ctx, "counter:global").Int64()
	require.NoError(t, err)
	require.EqualValues(t, sum, got, "late apply after seed-purge must be a no-op")
}
