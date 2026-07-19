// Package counterworker is the "counter" Kafka consumer group. It applies each
// accepted click to counter:total exactly once (idempotent via idem.FirstSee),
// and tracks the accepted-submit count that the difficulty controller reads.
// Redis is authoritative; offsets are committed only after effects apply, so a
// crash re-delivers rather than drops.
package counterworker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/the-algovn/the-button-service/internal/clickevent"
	"github.com/the-algovn/the-button-service/internal/idem"
	"github.com/the-algovn/the-button-service/internal/kafka"
)

const (
	group   = "counter"
	seenTTL = 10 * time.Minute
)

type Worker struct {
	RDB     *redis.Client
	Prod    *kgo.Client
	Brokers []string
	Logger  *slog.Logger
}

func (w *Worker) Run(ctx context.Context) error {
	cons, err := kafka.NewConsumer(w.Brokers, group, kafka.TopicClicks)
	if err != nil {
		return err
	}
	defer cons.Close()
	w.consume(ctx, cons)
	return nil
}

// consume applies each record's effect then commits offsets for the batch. On
// a Redis error it stops before committing (so the batch re-delivers); a
// malformed record is skipped (a poison record must not wedge the group).
func (w *Worker) consume(ctx context.Context, cons *kgo.Client) {
	for ctx.Err() == nil {
		f := cons.PollFetches(ctx)
		if f.IsClientClosed() {
			return
		}
		if errs := f.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if !errors.Is(e.Err, context.Canceled) {
					w.Logger.Warn("counter fetch", "err", e.Err)
				}
			}
			continue
		}
		recs := f.Records()
		ok := true
		for _, rec := range recs {
			if err := w.apply(ctx, rec); err != nil {
				w.Logger.Warn("counter apply", "err", err)
				ok = false
				break
			}
		}
		if ok && len(recs) > 0 {
			if err := cons.CommitRecords(ctx, recs...); err != nil {
				w.Logger.Warn("counter commit", "err", err)
			}
		}
	}
}

func (w *Worker) apply(ctx context.Context, rec *kgo.Record) error {
	ev, err := clickevent.Unmarshal(rec.Value)
	if err != nil {
		w.Logger.Warn("counter: skip malformed record", "err", err)
		return nil // poison record: skip, never block the group
	}
	first, err := idem.FirstSee(ctx, w.RDB, group, ev.ChallengeID, seenTTL)
	if err != nil {
		return err // Redis down: propagate so we don't commit
	}
	if !first {
		return nil // already applied
	}
	if err := w.RDB.IncrBy(ctx, "counter:total", int64(ev.Count)).Err(); err != nil {
		return err
	}
	return w.RDB.Incr(ctx, "stats:accepted_total").Err()
}
