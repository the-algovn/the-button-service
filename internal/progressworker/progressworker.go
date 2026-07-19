// Package progressworker is the "progress" Kafka consumer group. It applies each
// accepted click to the leaderboards + per-user total (idempotent), records the
// per-day/per-week counters the quest engine reads, produces sse.leaderboard
// frames, and runs the achievements/quests/streak/troll engines to emit a
// per-user sse.user frame.
package progressworker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	_ "time/tzdata"

	"github.com/the-algovn/the-button-service/internal/achievements"
	"github.com/the-algovn/the-button-service/internal/clickevent"
	"github.com/the-algovn/the-button-service/internal/idem"
	"github.com/the-algovn/the-button-service/internal/kafka"
	"github.com/the-algovn/the-button-service/internal/leaderboard"
	"github.com/the-algovn/the-button-service/internal/quests"
	"github.com/the-algovn/the-button-service/internal/streak"
	"github.com/the-algovn/the-button-service/internal/troll"
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
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}

	// --- engine layer: derive per-user progression from the new state and emit
	// a per-user SSE frame. Best-effort (logs on error; never fails the
	// already-committed board effects above).
	newTotal := uint64(w.RDB.ZScore(ctx, leaderboard.AllTimeKey, ev.Sub).Val())
	clicksToday, _ := w.RDB.HGet(ctx, dk, "clicks").Uint64()
	batchesToday, _ := w.RDB.HGet(ctx, dk, "batches").Uint64()
	maxBatchToday, _ := w.RDB.HGet(ctx, dk, "maxbatch").Uint64()
	daysThisWeek, _ := w.RDB.SCard(ctx, wk).Result()
	clicksThisWeek := uint64(w.RDB.ZScore(ctx, weekKey, ev.Sub).Val())
	weeklyRank := rankOf(ctx, w.RDB, weekKey, ev.Sub)
	allTimeRank := rankOf(ctx, w.RDB, leaderboard.AllTimeKey, ev.Sub)

	sig := quests.Signals{
		ClicksToday: clicksToday, BatchesToday: batchesToday, MaxBatchToday: maxBatchToday,
		DaysThisWeek: uint64(daysThisWeek), ClicksThisWeek: clicksThisWeek, WeeklyRank: weeklyRank,
	}

	// achievements + troll unlocks, deduped via the per-user unlocks hash.
	unlockKey := "unlocks:" + ev.Sub
	unlocked := []map[string]any{}
	claim := func(id, title, desc string) {
		if ok, err := w.RDB.HSetNX(ctx, unlockKey, id, now.Unix()).Result(); err == nil && ok {
			unlocked = append(unlocked, map[string]any{"id": id, "title": title, "description": desc})
		}
	}
	for _, a := range achievements.Evaluate(newTotal, ev.Count, now) {
		claim(a.ID, a.Title, a.Description)
	}
	for _, u := range troll.Evaluate(newTotal, now, false) {
		claim(u.ID, u.Title, u.Description)
	}

	// streak
	st, adv := streak.Advance(loadStreak(ctx, w.RDB, ev.Sub), date)
	if adv {
		w.RDB.HSet(ctx, "streak:"+ev.Sub, "count", st.Count, "best", st.Best, "lastday", st.LastDay)
	}

	// quests: current progress + newly-completed
	questProgress := []map[string]any{}
	questsDone := []string{}
	evalSet := func(defs []quests.Def, doneKey string) {
		for _, d := range defs {
			p, done := quests.Progress(d, sig)
			questProgress = append(questProgress, map[string]any{
				"id": d.ID, "title": d.Title, "description": d.Description,
				"kind": kindStr(d.Kind), "target": d.Target, "progress": p, "done": done, "reward": d.Reward,
			})
			if done {
				if n, err := w.RDB.SAdd(ctx, doneKey, d.ID).Result(); err == nil && n == 1 {
					questsDone = append(questsDone, d.ID)
				}
			}
		}
	}
	dailyDoneKey := "quests_done:" + ev.Sub + ":" + date
	weeklyDoneKey := "quests_done:" + ev.Sub + ":" + weekStart
	evalSet(quests.DailyQuests(date), dailyDoneKey)
	evalSet(quests.WeeklyQuests(weekStart), weeklyDoneKey)
	w.RDB.Expire(ctx, dailyDoneKey, dailyTTL)
	w.RDB.Expire(ctx, weeklyDoneKey, leaderboard.WeekTTL)

	frame := map[string]any{
		"type": "user", "sub": ev.Sub, "total": newTotal,
		"allTimeRank": allTimeRank, "weeklyRank": weeklyRank,
		"unlocked": unlocked, "questProgress": questProgress, "questsDone": questsDone,
		"streak": map[string]any{"count": st.Count, "best": st.Best, "lastDay": st.LastDay},
	}
	if body, err := json.Marshal(frame); err == nil {
		if err := kafka.Produce(ctx, w.Prod, kafka.TopicSSEUser, []byte(ev.Sub), body); err != nil {
			w.Logger.Warn("progress sse.user produce", "err", err)
		}
	}
	return nil
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

// rankOf returns the 1-based rank of sub in the ZSET (highest score = rank 1),
// or 0 if unranked.
func rankOf(ctx context.Context, rdb *redis.Client, key, sub string) uint32 {
	r, err := rdb.ZRevRank(ctx, key, sub).Result()
	if err != nil {
		return 0
	}
	return uint32(r) + 1
}

func kindStr(k quests.Kind) string {
	if k == quests.Weekly {
		return "weekly"
	}
	return "daily"
}

func loadStreak(ctx context.Context, rdb *redis.Client, sub string) streak.State {
	vals, err := rdb.HGetAll(ctx, "streak:"+sub).Result()
	if err != nil || len(vals) == 0 {
		return streak.State{}
	}
	count, _ := strconv.ParseUint(vals["count"], 10, 32)
	best, _ := strconv.ParseUint(vals["best"], 10, 32)
	return streak.State{Count: uint32(count), Best: uint32(best), LastDay: vals["lastday"]}
}
