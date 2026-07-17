//go:build integration

package publisher

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	"github.com/the-algovn/the-button-service/internal/achievements"
	"github.com/the-algovn/the-button-service/internal/db"
	"github.com/the-algovn/the-button-service/internal/store"
	"github.com/the-algovn/the-button-service/internal/testutil"
)

var logger = slog.New(slog.NewTextHandler(os.Stderr, nil))

// counterQueue binds an exclusive queue to the-button.counter and returns
// its consume channel.
func counterQueue(t *testing.T, amqpURL string) <-chan amqp.Delivery {
	t.Helper()
	conn, err := amqp.Dial(amqpURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	ch, err := conn.Channel()
	require.NoError(t, err)
	require.NoError(t, ch.ExchangeDeclare("events", "topic", true, false, false, false, nil))
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.QueueBind(q.Name, "the-button.counter", "events", false, nil))
	msgs, err := ch.Consume(q.Name, "", true, true, false, false, nil)
	require.NoError(t, err)
	return msgs
}

type frame struct {
	Type      string `json:"type"`
	Total     uint64 `json:"total"`
	Threshold uint64 `json:"threshold"`
}

func TestPublisher_BroadcastsSumOnChange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pgURL := testutil.StartPostgres(t)
	testutil.Migrate(t, pgURL)
	pool, err := store.NewPG(ctx, pgURL)
	require.NoError(t, err)
	defer pool.Close()
	rdb, err := store.NewRedis(ctx, testutil.StartRedis(t))
	require.NoError(t, err)
	defer rdb.Close()
	amqpURL := testutil.StartRabbit(t)
	msgs := counterQueue(t, amqpURL)

	_, err = db.New(pool).UpsertUserClicks(ctx, db.UpsertUserClicksParams{UserSub: "u1", Clicks: 41})
	require.NoError(t, err)

	p := &Publisher{Pool: pool, RDB: rdb, Publish: NewAMQPPublisher(ctx, amqpURL, logger), Logger: logger}
	go p.Run(ctx)

	waitCounter := func(want uint64) {
		t.Helper()
		deadline := time.After(15 * time.Second)
		for {
			select {
			case m := <-msgs:
				var f frame
				require.NoError(t, json.Unmarshal(m.Body, &f))
				if f.Type == "counter" && f.Total == want {
					return
				}
			case <-deadline:
				t.Fatalf("no counter frame with total=%d", want)
			}
		}
	}
	waitCounter(41)

	_, err = db.New(pool).UpsertUserClicks(ctx, db.UpsertUserClicksParams{UserSub: "u2", Clicks: 1})
	require.NoError(t, err)
	waitCounter(42)
}

func TestPublisher_WritesDifficultyKeysAtStartup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pgURL := testutil.StartPostgres(t)
	testutil.Migrate(t, pgURL)
	pool, err := store.NewPG(ctx, pgURL)
	require.NoError(t, err)
	defer pool.Close()
	rdb, err := store.NewRedis(ctx, testutil.StartRedis(t))
	require.NoError(t, err)
	defer rdb.Close()

	p := &Publisher{Pool: pool, RDB: rdb, Publish: func(string, []byte) {}, Logger: logger}
	go p.Run(ctx)

	require.Eventually(t, func() bool {
		return rdb.Get(ctx, "pow:L").Val() == "1" && rdb.Get(ctx, "pow:min_interval").Val() != ""
	}, 10*time.Second, 100*time.Millisecond)
}

// Two publishers racing (the rolling-update overlap window): milestone
// frames must publish exactly once — the Redis SETNX claim dedupes.
func TestMilestone_ExactlyOnceAcrossTwoPublishers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pgURL := testutil.StartPostgres(t)
	testutil.Migrate(t, pgURL)
	pool, err := store.NewPG(ctx, pgURL)
	require.NoError(t, err)
	defer pool.Close()
	rdb, err := store.NewRedis(ctx, testutil.StartRedis(t))
	require.NoError(t, err)
	defer rdb.Close()
	amqpURL := testutil.StartRabbit(t)
	msgs := counterQueue(t, amqpURL)

	first := achievements.Milestones[0].Threshold
	_, err = db.New(pool).UpsertUserClicks(ctx, db.UpsertUserClicksParams{UserSub: "u1", Clicks: int64(first)})
	require.NoError(t, err)

	for range 2 {
		p := &Publisher{Pool: pool, RDB: rdb, Publish: NewAMQPPublisher(ctx, amqpURL, logger), Logger: logger}
		go p.Run(ctx)
	}

	seen := 0
	deadline := time.After(10 * time.Second)
	for done := false; !done; {
		select {
		case m := <-msgs:
			var f frame
			require.NoError(t, json.Unmarshal(m.Body, &f))
			if f.Type == "milestone" && f.Threshold == first {
				seen++
			}
		case <-deadline:
			done = true
		}
	}
	require.Equal(t, 1, seen, "milestone %d must publish exactly once", first)
}
