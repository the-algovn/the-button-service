//go:build integration

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/the-algovn/the-button-service/internal/testutil"
)

func TestProduceThenConsume(t *testing.T) {
	brokers := testutil.StartRedpanda(t)
	prod, err := NewProducer(brokers)
	require.NoError(t, err)
	defer prod.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// testutil.StartRedpanda runs with auto_create_topics_enabled=false, so
	// the topic must be created explicitly before producing.
	_, err = kadm.NewClient(prod).CreateTopic(ctx, 1, 1, nil, "clicks")
	require.NoError(t, err)

	require.NoError(t, Produce(ctx, prod, "clicks", []byte("u1"), []byte(`{"sub":"u1"}`)))

	cons, err := NewConsumer(brokers, "counter", "clicks")
	require.NoError(t, err)
	defer cons.Close()

	f := cons.PollFetches(ctx)
	require.Empty(t, f.Errors())
	recs := f.Records()
	require.Len(t, recs, 1)
	require.Equal(t, []byte("u1"), recs[0].Key)
	require.NoError(t, cons.CommitRecords(ctx, recs...))
}
