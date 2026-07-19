// Package kafka wraps franz-go for the service: an idempotent synchronous
// producer for the hotpath, and consumer-group clients with manual commit
// (offsets committed only after a record's effects apply).
package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

func NewProducer(brokers []string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchMaxBytes(1<<20),
	)
}

// Produce sends one record synchronously and returns the broker error, so the
// hotpath can turn a produce failure into an Unavailable RPC (client retries).
func Produce(ctx context.Context, cl *kgo.Client, topic string, key, val []byte) error {
	rec := &kgo.Record{Topic: topic, Key: key, Value: val}
	return cl.ProduceSync(ctx, rec).FirstErr()
}

// NewConsumer builds a consumer-group client for one topic with auto-commit
// disabled — callers CommitRecords after effects apply, so a crash re-delivers.
func NewConsumer(brokers []string, group, topic string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
	)
}
