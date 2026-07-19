//go:build integration

package progressworker

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/the-algovn/the-button-service/internal/clickevent"
	"github.com/the-algovn/the-button-service/internal/kafka"
	"github.com/the-algovn/the-button-service/internal/leaderboard"
	"github.com/the-algovn/the-button-service/internal/store"
	"github.com/the-algovn/the-button-service/internal/testutil"
)

func TestProgressWorker_BoardsAndLeaderboardFrame(t *testing.T) {
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
	_, err = adm.CreateTopic(ctx, 1, 1, nil, kafka.TopicSSELeaderboard)
	require.NoError(t, err)

	now := time.Now()
	// ch-1 delivered twice (dup); ch-2 once.
	evs := []clickevent.Click{
		{Sub: "u1", Count: 5, ChallengeID: "ch-1", TsUnix: now.Unix(), DisplayName: "Alice"},
		{Sub: "u1", Count: 5, ChallengeID: "ch-1", TsUnix: now.Unix(), DisplayName: "Alice"},
		{Sub: "u2", Count: 3, ChallengeID: "ch-2", TsUnix: now.Unix(), DisplayName: "Bob"},
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

	// All-time board: u1=5 (dup NOT double-counted), u2=3.
	require.Eventually(t, func() bool {
		return rdb.ZScore(ctx, leaderboard.AllTimeKey, "u1").Val() == 5 &&
			rdb.ZScore(ctx, leaderboard.AllTimeKey, "u2").Val() == 3
	}, 25*time.Second, 200*time.Millisecond)

	// Weekly board mirrors it.
	require.EqualValues(t, 5, rdb.ZScore(ctx, leaderboard.WeekKey(now), "u1").Val())

	// Per-day counters for u1: 5 clicks in 1 batch.
	dk := "daily:u1:" + DateHCM(now)
	require.EqualValues(t, "5", rdb.HGet(ctx, dk, "clicks").Val())
	require.EqualValues(t, "1", rdb.HGet(ctx, dk, "batches").Val())
	require.EqualValues(t, "5", rdb.HGet(ctx, dk, "maxbatch").Val())

	// weekdays set has today.
	require.EqualValues(t, 1, rdb.SCard(ctx, "weekdays:u1:"+leaderboard.WeekStartString(now)).Val())

	// profile name recorded.
	require.Equal(t, "Alice", rdb.HGet(ctx, "profile:names", "u1").Val())

	// sse.leaderboard frame lands with u1 top (clicks 5).
	cons, err := kafka.NewConsumer(brokers, "test-lb", kafka.TopicSSELeaderboard)
	require.NoError(t, err)
	defer cons.Close()
	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("no leaderboard frame with u1")
		default:
		}
		f := cons.PollFetches(ctx)
		found := false
		for _, rec := range f.Records() {
			var m struct {
				Type    string `json:"type"`
				AllTime []struct {
					Name   string `json:"name"`
					Clicks uint64 `json:"clicks"`
				} `json:"allTime"`
			}
			if json.Unmarshal(rec.Value, &m) == nil && m.Type == "leaderboard" {
				for _, r := range m.AllTime {
					if r.Name == "Alice" && r.Clicks == 5 {
						found = true
					}
				}
			}
		}
		if found {
			return
		}
	}
}
