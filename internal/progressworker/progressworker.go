// Package progressworker is the "progress" Kafka consumer group. It applies each
// accepted click to the leaderboards + per-user total (idempotent), records the
// per-day/per-week counters the quest engine reads, and produces sse.leaderboard
// frames. Achievements/quests/streak/troll + sse.user are wired in a follow-up.
package progressworker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	_ "time/tzdata"

	"github.com/the-algovn/the-button-service/internal/clickevent"
	"github.com/the-algovn/the-button-service/internal/idem"
	"github.com/the-algovn/the-button-service/internal/kafka"
	"github.com/the-algovn/the-button-service/internal/leaderboard"
)

const (
	group      = "progress"
	seenTTL    = 10 * time.Minute
	lbInterval = 3 * time.Second
	dailyTTL   = 48 * time.Hour
)

var hcm = func() *time.Location {
	loc, err := time.LoadLocation("Asia/Ho_Chi_Minh")
	if err != nil {
		panic(err)
	}
	return loc
}()

// DateHCM is the Asia/Ho_Chi_Minh calendar date (YYYY-MM-DD) for t.
func DateHCM(t time.Time) string { return t.In(hcm).Format("2006-01-02") }

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
	go w.leaderboardTick(ctx)
	w.consume(ctx, cons)
	return nil
}

func (w *Worker) consume(ctx context.Context, cons *kgo.Client) {
	for ctx.Err() == nil {
		f := cons.PollFetches(ctx)
		if f.IsClientClosed() {
			return
		}
		if errs := f.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if !errors.Is(e.Err, context.Canceled) {
					w.Logger.Warn("progress fetch", "err", e.Err)
				}
			}
			continue
		}
		recs := f.Records()
		ok := true
		for _, rec := range recs {
			if err := w.apply(ctx, rec); err != nil {
				w.Logger.Warn("progress apply", "err", err)
				ok = false
				break
			}
		}
		if ok && len(recs) > 0 {
			if err := cons.CommitRecords(ctx, recs...); err != nil {
				w.Logger.Warn("progress commit", "err", err)
			}
		}
	}
}

func (w *Worker) apply(ctx context.Context, rec *kgo.Record) error {
	ev, err := clickevent.Unmarshal(rec.Value)
	if err != nil {
		w.Logger.Warn("progress: skip malformed record", "err", err)
		return nil
	}
	first, err := idem.FirstSee(ctx, w.RDB, group, ev.ChallengeID, seenTTL)
	if err != nil {
		return err
	}
	if !first {
		return nil
	}
	return w.applyEvent(ctx, ev)
}

// applyEvent updates boards, per-user total, profile name, and the day/week
// counters. Called once per distinct accepted click (idempotency gated above).
func (w *Worker) applyEvent(ctx context.Context, ev clickevent.Click) error {
	now := time.Unix(ev.TsUnix, 0)
	weekKey := leaderboard.WeekKey(now)
	date := DateHCM(now)
	weekStart := leaderboard.WeekStartString(now)
	dk := "daily:" + ev.Sub + ":" + date
	wk := "weekdays:" + ev.Sub + ":" + weekStart

	// Raise-only maxbatch: read the current value first (idempotency-gated, so no
	// concurrent duplicate can race this read-then-write).
	curMax, err := w.RDB.HGet(ctx, dk, "maxbatch").Uint64()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}

	// All effects in one atomic pipeline: either the whole event's state applies
	// or the error propagates and the batch is not committed (re-delivered).
	pipe := w.RDB.TxPipeline()
	pipe.ZIncrBy(ctx, leaderboard.AllTimeKey, float64(ev.Count), ev.Sub)
	pipe.ZIncrBy(ctx, weekKey, float64(ev.Count), ev.Sub)
	pipe.Expire(ctx, weekKey, leaderboard.WeekTTL)
	pipe.HSet(ctx, "profile:names", ev.Sub, ev.DisplayName)
	pipe.HIncrBy(ctx, dk, "clicks", int64(ev.Count))
	pipe.HIncrBy(ctx, dk, "batches", 1)
	if uint64(ev.Count) > curMax {
		pipe.HSet(ctx, dk, "maxbatch", ev.Count)
	}
	pipe.Expire(ctx, dk, dailyTTL)
	pipe.SAdd(ctx, wk, date)
	pipe.Expire(ctx, wk, leaderboard.WeekTTL)
	_, err = pipe.Exec(ctx)
	return err
}

func (w *Worker) leaderboardTick(ctx context.Context) {
	tk := time.NewTicker(lbInterval)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
		}
		now := time.Now()
		frame := map[string]any{
			"type":     "leaderboard",
			"allTime":  w.renderTop(ctx, leaderboard.AllTimeKey),
			"thisWeek": w.renderTop(ctx, leaderboard.WeekKey(now)),
		}
		body, err := json.Marshal(frame)
		if err != nil {
			continue
		}
		if err := kafka.Produce(ctx, w.Prod, kafka.TopicSSELeaderboard, []byte("leaderboard"), body); err != nil {
			w.Logger.Warn("progress sse.leaderboard produce", "err", err)
		}
	}
}

func (w *Worker) renderTop(ctx context.Context, key string) []map[string]any {
	top, err := leaderboard.TopN(ctx, w.RDB, key, 20)
	if err != nil || len(top) == 0 {
		return []map[string]any{}
	}
	subs := make([]string, len(top))
	for i, r := range top {
		subs[i] = r.Sub
	}
	names := map[string]string{}
	if vals, err := w.RDB.HMGet(ctx, "profile:names", subs...).Result(); err == nil {
		for i, v := range vals {
			if s, ok := v.(string); ok {
				names[subs[i]] = s
			}
		}
	}
	out := make([]map[string]any, len(top))
	for i, r := range top {
		name := names[r.Sub]
		if name == "" {
			name = "clicker-" + r.Sub[:min(6, len(r.Sub))]
		}
		out[i] = map[string]any{"rank": i + 1, "name": name, "clicks": r.Clicks}
	}
	return out
}
