//go:build integration

package counterworker

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/the-algovn/the-button-service/internal/kafka"
	"github.com/the-algovn/the-button-service/internal/store"
	"github.com/the-algovn/the-button-service/internal/testutil"
)

func TestCounterWorker_TickPublishesCounterMilestoneDifficulty(t *testing.T) {
	ctx := context.Background()
	rdb, err := store.NewRedis(ctx, testutil.StartRedis(t))
	require.NoError(t, err)
	defer rdb.Close()

	brokers := testutil.StartRedpanda(t)
	prod, err := kafka.NewProducer(brokers)
	require.NoError(t, err)
	defer prod.Close()
	adm := kadm.NewClient(prod)
	_, err = adm.CreateTopic(ctx, 1, 1, nil, kafka.TopicClicks)
	require.NoError(t, err)
	_, err = adm.CreateTopic(ctx, 1, 1, nil, kafka.TopicSSECounter)
	require.NoError(t, err)

	// Seed an authoritative counter that has crossed the 1,000 milestone.
	require.NoError(t, rdb.Set(ctx, "counter:total", 1000, 0).Err())

	w := &Worker{RDB: rdb, Prod: prod, Brokers: brokers, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil))}
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = w.Run(wctx) }()

	// The difficulty controller writes pow:L / pow:min_interval on startup so
	// IssueChallenge can fail-open.
	require.Eventually(t, func() bool {
		return rdb.Exists(ctx, "pow:L").Val() == 1 && rdb.Exists(ctx, "pow:min_interval").Val() == 1
	}, 15*time.Second, 200*time.Millisecond)

	// The 1,000 milestone is claimed exactly once.
	require.Eventually(t, func() bool {
		return rdb.Exists(ctx, "milestone:1000").Val() == 1
	}, 15*time.Second, 200*time.Millisecond)

	// sse.counter carries a counter frame (total=1000) and a milestone frame.
	cons, err := kafka.NewConsumer(brokers, "test-sse", kafka.TopicSSECounter)
	require.NoError(t, err)
	defer cons.Close()
	sawCounter, sawMilestone := false, false
	deadline := time.After(15 * time.Second)
	for !(sawCounter && sawMilestone) {
		select {
		case <-deadline:
			t.Fatalf("did not see counter+milestone frames (counter=%v milestone=%v)", sawCounter, sawMilestone)
		default:
		}
		f := cons.PollFetches(ctx)
		for _, rec := range f.Records() {
			var m map[string]any
			require.NoError(t, json.Unmarshal(rec.Value, &m))
			switch m["type"] {
			case "counter":
				if m["total"].(float64) == 1000 {
					sawCounter = true
				}
			case "milestone":
				if m["threshold"].(float64) == 1000 {
					sawMilestone = true
				}
			}
		}
	}
	require.True(t, sawCounter && sawMilestone)
}
