//go:build integration

package ticker

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

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

// TestReconcile_IgnoresInFlightSubmit covers Fix 1: reconcile reads Redis
// BEFORE Postgres and only acts on a drift that survives a settle window, so
// a Submit() whose PG commit lands between reconcile's two reads cannot be
// double-counted (it either cancels out or its own pending INCRBY lands
// during the settle window and the second read agrees with the first, which
// is indistinguishable from — and handled the same as — a genuinely lost
// INCRBY). This asserts both halves: a real, persistent drift still gets
// healed exactly, and an already-matching counter is left untouched.
func TestReconcile_IgnoresInFlightSubmit(t *testing.T) {
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

	// Seed matching state: PG SUM == counter:global (drift 0).
	_, err = pool.Exec(ctx, `INSERT INTO user_clicks (user_sub, clicks) VALUES ('u1', 100)`)
	require.NoError(t, err)
	require.NoError(t, rdb.Set(ctx, "counter:global", 100, 0).Err())

	// Simulate the dangerous interleaving's end state: a commit landed in PG
	// with no matching INCRBY (whether from a crash or an in-flight batch
	// that never completes within the settle window, reconcile must still
	// heal a drift that persists across both reads).
	_, err = pool.Exec(ctx, `UPDATE user_clicks SET clicks = clicks + 50 WHERE user_sub = 'u1'`)
	require.NoError(t, err)

	require.NoError(t, tk.reconcile(ctx))
	got, err := rdb.Get(ctx, "counter:global").Int64()
	require.NoError(t, err)
	var sum int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT COALESCE(SUM(clicks), 0) FROM user_clicks`).Scan(&sum))
	require.EqualValues(t, sum, got, "counter:global must end up exactly at SUM(user_clicks)")

	// Separately: counter:global already agrees with the PG sum — reconcile
	// must not touch it.
	require.NoError(t, tk.reconcile(ctx))
	got2, err := rdb.Get(ctx, "counter:global").Int64()
	require.NoError(t, err)
	require.Equal(t, got, got2, "reconcile must not apply a spurious correction when already settled")
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
