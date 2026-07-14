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
