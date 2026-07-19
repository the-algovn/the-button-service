//go:build integration

package counterworker

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/the-algovn/the-button-service/internal/clickevent"
	"github.com/the-algovn/the-button-service/internal/kafka"
	"github.com/the-algovn/the-button-service/internal/store"
	"github.com/the-algovn/the-button-service/internal/testutil"
)

func TestCounterWorker_IdempotentIncr(t *testing.T) {
	ctx := context.Background()
	rdb, err := store.NewRedis(ctx, testutil.StartRedis(t))
	require.NoError(t, err)
	defer rdb.Close()

	brokers := testutil.StartRedpanda(t)
	prod, err := kafka.NewProducer(brokers)
	require.NoError(t, err)
	defer prod.Close()
	_, err = kadm.NewClient(prod).CreateTopic(ctx, 1, 1, nil, kafka.TopicClicks)
	require.NoError(t, err)

	// ch-1 is delivered twice (a duplicate); ch-2 once.
	evs := []clickevent.Click{
		{Sub: "u1", Count: 5, ChallengeID: "ch-1", TsUnix: 1, DisplayName: "a"},
		{Sub: "u1", Count: 5, ChallengeID: "ch-1", TsUnix: 1, DisplayName: "a"},
		{Sub: "u2", Count: 3, ChallengeID: "ch-2", TsUnix: 2, DisplayName: "b"},
	}
	for _, ev := range evs {
		val, err := ev.Marshal()
		require.NoError(t, err)
		require.NoError(t, kafka.Produce(ctx, prod, kafka.TopicClicks, ev.Key(), val))
	}

	w := &Worker{RDB: rdb, Prod: prod, Brokers: brokers, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil))}
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = w.Run(wctx) }()

	// counter:total converges to 5 + 3 = 8 — the duplicate ch-1 does NOT double count.
	require.Eventually(t, func() bool {
		v, _ := rdb.Get(ctx, "counter:total").Int64()
		return v == 8
	}, 25*time.Second, 200*time.Millisecond)

	// stats:accepted_total counts distinct accepted challenges: ch-1 + ch-2 = 2.
	stats, _ := rdb.Get(ctx, "stats:accepted_total").Int64()
	require.EqualValues(t, 2, stats)
}
