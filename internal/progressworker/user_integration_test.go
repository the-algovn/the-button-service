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
	"github.com/the-algovn/the-button-service/internal/store"
	"github.com/the-algovn/the-button-service/internal/testutil"
)

func TestProgressWorker_EmitsUserFrame(t *testing.T) {
	ctx := context.Background()
	rdb, err := store.NewRedis(ctx, testutil.StartRedis(t))
	require.NoError(t, err)
	defer rdb.Close()

	brokers := testutil.StartRedpanda(t)
	prod, err := kafka.NewProducer(brokers)
	require.NoError(t, err)
	defer prod.Close()
	adm := kadm.NewClient(prod)
	for _, tp := range []string{kafka.TopicClicks, kafka.TopicSSEUser, kafka.TopicSSELeaderboard} {
		_, err := adm.CreateTopic(ctx, 1, 1, nil, tp)
		require.NoError(t, err)
	}

	now := time.Now()
	ev := clickevent.Click{Sub: "u1", Count: 10, ChallengeID: "ch-1", TsUnix: now.Unix(), DisplayName: "Alice"}
	val, _ := ev.Marshal()
	require.NoError(t, kafka.Produce(ctx, prod, kafka.TopicClicks, ev.Key(), val))

	w := &Worker{RDB: rdb, Prod: prod, Brokers: brokers, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil))}
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = w.Run(wctx) }()

	// unlocks hash gets mvh (>=1) and ten (>=10).
	require.Eventually(t, func() bool {
		return rdb.HExists(ctx, "unlocks:u1", "mvh").Val() && rdb.HExists(ctx, "unlocks:u1", "ten").Val()
	}, 25*time.Second, 200*time.Millisecond)
	// streak started at 1.
	require.Equal(t, "1", rdb.HGet(ctx, "streak:u1", "count").Val())

	// sse.user frame for u1: total 10, rank 1, unlocked has mvh+ten, streak 1, quests present.
	cons, err := kafka.NewConsumer(brokers, "test-user", kafka.TopicSSEUser)
	require.NoError(t, err)
	defer cons.Close()
	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("no sse.user frame for u1")
		default:
		}
		f := cons.PollFetches(ctx)
		for _, rec := range f.Records() {
			var m struct {
				Type        string `json:"type"`
				Sub         string `json:"sub"`
				Total       uint64 `json:"total"`
				AllTimeRank uint32 `json:"allTimeRank"`
				Unlocked    []struct {
					ID string `json:"id"`
				} `json:"unlocked"`
				QuestProgress []map[string]any `json:"questProgress"`
				Streak        struct {
					Count uint32 `json:"count"`
				} `json:"streak"`
			}
			if json.Unmarshal(rec.Value, &m) != nil || m.Type != "user" || m.Sub != "u1" {
				continue
			}
			require.EqualValues(t, 10, m.Total)
			require.EqualValues(t, 1, m.AllTimeRank)
			ids := map[string]bool{}
			for _, u := range m.Unlocked {
				ids[u.ID] = true
			}
			require.True(t, ids["mvh"] && ids["ten"], "unlocked=%v", ids)
			require.EqualValues(t, 1, m.Streak.Count)
			require.NotEmpty(t, m.QuestProgress) // 3 daily + 2 weekly
			return
		}
	}
}
