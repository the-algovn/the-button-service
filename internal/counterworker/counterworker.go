// Package counterworker is the "counter" Kafka consumer group. It applies each
// accepted click to counter:total exactly once (idempotent via idem.FirstSee),
// and tracks the accepted-submit count that the difficulty controller reads.
// Redis is authoritative; offsets are committed only after effects apply, so a
// crash re-delivers rather than drops.
package counterworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/the-algovn/the-button-service/internal/achievements"
	"github.com/the-algovn/the-button-service/internal/clickevent"
	"github.com/the-algovn/the-button-service/internal/idem"
	"github.com/the-algovn/the-button-service/internal/kafka"
	"github.com/the-algovn/the-button-service/internal/leaderboard"
	"github.com/the-algovn/the-button-service/internal/pow"
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
	go w.tick(ctx)
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

const tickInterval = time.Second

// tick reads the authoritative counter, publishes counter/milestone frames to
// sse.counter when the total moves, and runs the PoW difficulty controller
// (EWMA of accepted submits/s -> NextL -> pow:L / pow:min_interval). Ported
// from the old publisher pollLoop, but Redis-sourced and Kafka-sinked.
func (w *Worker) tick(ctx context.Context) {
	l := w.currentL(ctx)
	w.writeDifficulty(ctx, l)
	lastChange := time.Now()
	var ewma float64
	prevStats, _ := w.RDB.Get(ctx, "stats:accepted_total").Int64()
	prevSample := time.Now()
	var lastPublished int64 = -1

	tk := time.NewTicker(tickInterval)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
		}

		total, err := w.RDB.Get(ctx, "counter:total").Uint64()
		if errors.Is(err, redis.Nil) {
			total, err = 0, nil
		}
		if err != nil {
			if ctx.Err() == nil {
				w.Logger.Warn("counter tick: read total", "err", err)
			}
			continue
		}

		if int64(total) != lastPublished {
			frame := map[string]any{"type": "counter", "total": total}
			if users, err := w.RDB.ZCard(ctx, leaderboard.AllTimeKey).Result(); err == nil && users > 0 {
				frame["users"] = users
			}
			w.produceSSE(ctx, frame)
			w.claimMilestones(ctx, total)
			lastPublished = int64(total)
		}

		// difficulty controller: EWMA of accepted submits/s -> NextL
		if s, err := w.RDB.Get(ctx, "stats:accepted_total").Int64(); err == nil || errors.Is(err, redis.Nil) {
			now := time.Now()
			dt := now.Sub(prevSample)
			if dt > 0 {
				ewma = pow.EWMA(ewma, float64(s-prevStats)/dt.Seconds(), dt)
				prevStats, prevSample = s, now
				if next, ts := pow.NextL(l, ewma, lastChange, now); next != l {
					l, lastChange = next, ts
					w.writeDifficulty(ctx, l)
					w.Logger.Info("difficulty changed", "L", l, "ewma_rate", ewma)
				}
			}
		}
	}
}

// claimMilestones SETNX-claims each reached threshold (exactly-once) and
// produces only a won claim (at-most-once announcement) to sse.counter.
func (w *Worker) claimMilestones(ctx context.Context, total uint64) {
	for _, m := range achievements.Milestones {
		if total < m.Threshold {
			return // ascending
		}
		won, err := w.RDB.SetNX(ctx, fmt.Sprintf("milestone:%d", m.Threshold), time.Now().Unix(), 0).Result()
		if err != nil {
			w.Logger.Warn("milestone claim", "threshold", m.Threshold, "err", err)
			return
		}
		if won {
			w.produceSSE(ctx, map[string]any{"type": "milestone", "threshold": m.Threshold, "title": m.Title})
		}
	}
}

func (w *Worker) writeDifficulty(ctx context.Context, l uint32) {
	if err := w.RDB.Set(ctx, "pow:L", l, 0).Err(); err != nil {
		w.Logger.Warn("write pow:L", "err", err)
	}
	if err := w.RDB.Set(ctx, "pow:min_interval", pow.MinInterval(l), 0).Err(); err != nil {
		w.Logger.Warn("write pow:min_interval", "err", err)
	}
}

func (w *Worker) currentL(ctx context.Context) uint32 {
	if v, err := w.RDB.Get(ctx, "pow:L").Int64(); err == nil && v >= pow.MinL && v <= pow.MaxL {
		return uint32(v)
	}
	return pow.MinL
}

// produceSSE marshals v and produces it to sse.counter with a fixed key so
// frames stay ordered on one partition (api-control-plane fans out to all).
func (w *Worker) produceSSE(ctx context.Context, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		w.Logger.Warn("counter sse marshal", "err", err)
		return
	}
	if err := kafka.Produce(ctx, w.Prod, kafka.TopicSSECounter, []byte("counter"), body); err != nil {
		w.Logger.Warn("counter sse produce", "err", err)
	}
}
