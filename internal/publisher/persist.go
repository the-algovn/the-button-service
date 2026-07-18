package publisher

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/the-algovn/the-button-service/internal/achievements"
	"github.com/the-algovn/the-button-service/internal/db"
	"github.com/the-algovn/the-button-service/internal/leaderboard"
)

const (
	eventsStream        = "clicks:events"
	eventsGroup         = "persist"
	persistFlushEvery   = 2 * time.Second
	persistReconcile    = 60 * time.Second
	persistReadCount    = 500
	persistCallTimeout  = 3 * time.Second
	persistStreamMaxLen = 100000
)

// Persister owns the async write-behind: it consumes clicks:events for
// achievements and batch-flushes absolute ZSET scores to Postgres. Runs in
// the single-replica publisher.
type Persister struct {
	Pool   *pgxpool.Pool
	RDB    *redis.Client
	Logger *slog.Logger

	mu    sync.Mutex
	dirty map[string]struct{}
}

func (p *Persister) markDirty(sub string) {
	p.mu.Lock()
	if p.dirty == nil {
		p.dirty = map[string]struct{}{}
	}
	p.dirty[sub] = struct{}{}
	p.mu.Unlock()
}

// drainDirty returns the accumulated subs and resets the set.
func (p *Persister) drainDirty() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.dirty))
	for s := range p.dirty {
		out = append(out, s)
	}
	p.dirty = map[string]struct{}{}
	return out
}

func (p *Persister) Run(ctx context.Context) {
	// MKSTREAM so the group (and stream) exist before the first click. Start
	// at "0" (not "$"): the stream can already exist with entries by the time
	// this group is first created (XADD auto-vivifies it), and "$" would
	// silently skip those — "0" replays from the beginning, which is safe
	// since achievement/flush processing is idempotent.
	if err := p.RDB.XGroupCreateMkStream(ctx, eventsStream, eventsGroup, "0").Err(); err != nil &&
		!strings.Contains(err.Error(), "BUSYGROUP") {
		p.Logger.Warn("xgroup create", "err", err)
	}
	go p.flushLoop(ctx)
	go p.reconcileLoop(ctx)
	p.consumeLoop(ctx)
}

// consumeLoop drains this consumer's pending entries first (id "0"), then
// blocks for new ones (">"). XACK happens only after a message is fully
// processed, so a crash re-delivers rather than drops.
func (p *Persister) consumeLoop(ctx context.Context) {
	const consumer = "persist-1"
	readID := "0"
	for {
		if ctx.Err() != nil {
			return
		}
		res, err := p.RDB.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: eventsGroup, Consumer: consumer,
			Streams: []string{eventsStream, readID},
			Count:   persistReadCount, Block: time.Second,
		}).Result()
		if errors.Is(err, redis.Nil) {
			readID = ">" // pending drained; switch to new messages
			continue
		}
		if err != nil {
			if ctx.Err() == nil {
				p.Logger.Warn("xreadgroup", "err", err)
			}
			continue
		}
		processed := 0
		for _, st := range res {
			if len(st.Messages) == 0 && readID == "0" {
				readID = ">"
			}
			for _, m := range st.Messages {
				if p.process(ctx, m) {
					p.RDB.XAck(ctx, eventsStream, eventsGroup, m.ID)
					processed++
				}
			}
		}
		if processed > 0 {
			p.RDB.XTrimMaxLenApprox(ctx, eventsStream, persistStreamMaxLen, 0)
		}
	}
}

// process evaluates one accepted-batch event and persists any newly-earned
// achievements. Returns true only when the event is fully handled (safe to
// XACK). It marks the sub dirty for the counter flush regardless.
func (p *Persister) process(ctx context.Context, m redis.XMessage) bool {
	sub, _ := m.Values["sub"].(string)
	if sub == "" {
		return true // malformed — drop
	}
	p.markDirty(sub)
	total, _ := strconv.ParseUint(str(m.Values["total"]), 10, 64)
	count64, _ := strconv.ParseUint(str(m.Values["count"]), 10, 32)
	tsUnix, _ := strconv.ParseInt(str(m.Values["ts"]), 10, 64)
	at := time.Unix(tsUnix, 0)

	q := db.New(p.Pool)
	for _, a := range achievements.Evaluate(total, uint32(count64), at) {
		key := "ach:" + sub + ":" + a.ID
		won, err := p.RDB.SetNX(ctx, key, tsUnix, 0).Result()
		if err != nil {
			p.Logger.Warn("ach setnx", "err", err)
			return false // retry the whole event
		}
		if !won {
			continue // already claimed by an earlier event
		}
		if err := q.InsertUserAchievementAt(ctx, db.InsertUserAchievementAtParams{
			UserSub: sub, AchievementID: a.ID, UnlockedAt: at,
		}); err != nil {
			p.RDB.Del(ctx, key) // roll back the claim so redelivery re-attempts the insert
			p.Logger.Warn("ach insert", "err", err)
			return false // Postgres down — redeliver later
		}
	}
	return true
}

func (p *Persister) flushLoop(ctx context.Context) {
	tick := time.NewTicker(persistFlushEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			p.flush(ctx)
		}
	}
}

// flush writes the absolute ZSET scores of the dirty subs to Postgres in one
// batch per table. On any error the subs are re-marked dirty so the next tick
// retries — Postgres being down never blocks the hot path.
func (p *Persister) flush(ctx context.Context) {
	subs := p.drainDirty()
	if len(subs) == 0 {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, persistCallTimeout)
	defer cancel()
	now := time.Now()
	weekKey := leaderboard.WeekKey(now)

	allScores := p.RDB.ZMScore(cctx, leaderboard.AllTimeKey, subs...).Val()
	weekScores := p.RDB.ZMScore(cctx, weekKey, subs...).Val()
	names := p.RDB.HMGet(cctx, "profile:names", subs...).Val()

	var aSubs, wSubs, nSubs []string
	var aClicks, wClicks []int64
	var nNames []string
	for i, sub := range subs {
		if i < len(allScores) && allScores[i] > 0 {
			aSubs = append(aSubs, sub)
			aClicks = append(aClicks, int64(allScores[i]))
		}
		if i < len(weekScores) && weekScores[i] > 0 {
			wSubs = append(wSubs, sub)
			wClicks = append(wClicks, int64(weekScores[i]))
		}
		if i < len(names) {
			if s, ok := names[i].(string); ok && s != "" {
				nSubs = append(nSubs, sub)
				nNames = append(nNames, s)
			}
		}
	}

	q := db.New(p.Pool)
	failed := false
	if len(aSubs) > 0 {
		if err := q.BatchUpsertUserClicks(cctx, db.BatchUpsertUserClicksParams{Subs: aSubs, Clicks: aClicks}); err != nil {
			p.Logger.Warn("flush user_clicks", "err", err)
			failed = true
		}
	}
	if len(wSubs) > 0 {
		ws := pgtype.Date{Time: leaderboard.WeekStart(now), Valid: true}
		if err := q.BatchUpsertUserWeeklyClicks(cctx, db.BatchUpsertUserWeeklyClicksParams{Subs: wSubs, WeekStart: ws, Clicks: wClicks}); err != nil {
			p.Logger.Warn("flush user_weekly_clicks", "err", err)
			failed = true
		}
	}
	if len(nSubs) > 0 {
		if err := q.BatchUpsertUserProfile(cctx, db.BatchUpsertUserProfileParams{Subs: nSubs, Names: nNames}); err != nil {
			p.Logger.Warn("flush user_profile", "err", err)
			failed = true
		}
	}
	if failed {
		for _, s := range subs {
			p.markDirty(s)
		}
	}
}

func (p *Persister) reconcileLoop(ctx context.Context) {
	tick := time.NewTicker(persistReconcile)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			p.reconcile(ctx)
		}
	}
}

// counterHealScript raises counter:total toward the ZSET-sum truth but NEVER
// lowers it — the atomic hot-path script keeps the two equal in steady state,
// so this only ever recovers a counter left low by a Redis-loss disaster.
var counterHealScript = redis.NewScript(`
local cur = tonumber(redis.call('GET', KEYS[1]) or '0')
local sum = tonumber(ARGV[1])
if sum > cur then redis.call('SET', KEYS[1], sum) end
return 1
`)

// reconcile is the ~60s backstop: it monotonically heals counter:total against
// the lb:alltime score sum (only ever raising it), recovering drift from a
// partial AOF/disaster recovery without ever moving the public counter backward.
func (p *Persister) reconcile(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, persistCallTimeout)
	defer cancel()
	zs, err := p.RDB.ZRangeWithScores(cctx, leaderboard.AllTimeKey, 0, -1).Result()
	if err != nil {
		if ctx.Err() == nil {
			p.Logger.Warn("reconcile: zrange", "err", err)
		}
		return
	}
	var sum int64
	for _, z := range zs {
		sum += int64(z.Score)
	}
	if err := counterHealScript.Run(cctx, p.RDB, []string{"counter:total"}, sum).Err(); err != nil {
		if ctx.Err() == nil {
			p.Logger.Warn("reconcile: counter heal", "err", err)
		}
	}
}

func str(v any) string {
	s, _ := v.(string)
	return s
}
