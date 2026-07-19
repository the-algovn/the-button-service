//go:build integration

package clicks

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/the-algovn/the-button-service/internal/clickevent"
	"github.com/the-algovn/the-button-service/internal/kafka"
	"github.com/the-algovn/the-button-service/internal/pow"
	"github.com/the-algovn/the-button-service/internal/store"
	"github.com/the-algovn/the-button-service/internal/testutil"
)

func TestSubmit_ProducesAndThrottles(t *testing.T) {
	ctx := context.Background()
	rdb, err := store.NewRedis(ctx, testutil.StartRedis(t))
	require.NoError(t, err)
	defer rdb.Close()

	brokers := testutil.StartRedpanda(t)
	prod, err := kafka.NewProducer(brokers)
	require.NoError(t, err)
	defer prod.Close()

	// testutil.StartRedpanda runs with auto_create_topics_enabled=false, so
	// the topic must be created explicitly before producing.
	_, err = kadm.NewClient(prod).CreateTopic(ctx, 1, 1, nil, kafka.TopicClicks)
	require.NoError(t, err)

	p := pow.Payload{ID: "ch-1", Sub: "u1", MinIntervalS: 2}

	// First submit: accepted, produced.
	require.NoError(t, Submit(ctx, rdb, prod, p, 5, time.Now(), "Nyx"))

	// Immediate second submit for the same sub: throttled.
	err = Submit(ctx, rdb, prod, pow.Payload{ID: "ch-2", Sub: "u1", MinIntervalS: 2}, 5, time.Now(), "Nyx")
	require.Equal(t, codes.ResourceExhausted, status.Code(err))

	// The produced event is readable and well-formed.
	cons, err := kafka.NewConsumer(brokers, "test", kafka.TopicClicks)
	require.NoError(t, err)
	defer cons.Close()
	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	recs := cons.PollFetches(cctx).Records()
	require.Len(t, recs, 1)
	got, err := clickevent.Unmarshal(recs[0].Value)
	require.NoError(t, err)
	require.Equal(t, uint32(5), got.Count)
	require.Equal(t, "ch-1", got.ChallengeID)
}
