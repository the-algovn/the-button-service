// Package publisher is the single-replica broadcast loop: it polls the
// Postgres SUM every second and publishes counter/milestone frames to
// RabbitMQ, claims milestones, and runs the shared PoW difficulty
// controller. It replaced the leader-elected ticker (2026-07-17
// api-publisher split): Postgres is the only counter truth, so there is
// nothing to heal and nothing to elect. Brief two-instance overlap during a
// rolling update is safe — duplicate counter frames are cosmetic and
// milestone claims dedupe via SETNX.
package publisher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/the-algovn/the-button-service/internal/achievements"
	"github.com/the-algovn/the-button-service/internal/db"
	"github.com/the-algovn/the-button-service/internal/leaderboard"
	"github.com/the-algovn/the-button-service/internal/pow"
)

const (
	pollInterval       = time.Second
	usersInterval      = 15 * time.Second
	callTimeout        = 3 * time.Second
	counterChannel     = "the-button.counter"
	leaderboardEveryN  = 3
	leaderboardChannel = "the-button.leaderboard"
)

type Publisher struct {
	Pool    *pgxpool.Pool
	RDB     *redis.Client
	Publish func(channel string, body []byte)
	Logger  *slog.Logger

	users     atomic.Uint64
	haveUsers atomic.Bool
}

// Run starts the users loop and the poll loop and blocks until ctx is done.
func (p *Publisher) Run(ctx context.Context) {
	p.warmUp(ctx)
	go (&Persister{Pool: p.Pool, RDB: p.RDB, Logger: p.Logger}).Run(ctx)
	go p.usersLoop(ctx)
	p.pollLoop(ctx)
}

// warmUp restores Redis counter truth from Postgres when counter:total is
// absent (fresh Redis / disaster recovery). Steady state is a no-op.
func (p *Publisher) warmUp(ctx context.Context) {
	if n, err := p.RDB.Exists(ctx, "counter:total").Result(); err == nil && n == 1 {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	q := db.New(p.Pool)
	all, err := q.ListAllUserClicks(cctx)
	if err != nil {
		p.Logger.Warn("warmup: list all clicks", "err", err)
		return
	}
	var sum int64
	if len(all) > 0 {
		zs := make([]redis.Z, len(all))
		for i, r := range all {
			zs[i] = redis.Z{Score: float64(r.Clicks), Member: r.UserSub}
			sum += r.Clicks
		}
		p.RDB.ZAddArgs(cctx, leaderboard.AllTimeKey, redis.ZAddArgs{GT: true, Members: zs})
	}
	now := time.Now()
	week, err := q.ListWeekUserClicks(cctx, pgDate(leaderboard.WeekStart(now)))
	if err == nil && len(week) > 0 {
		zs := make([]redis.Z, len(week))
		for i, r := range week {
			zs[i] = redis.Z{Score: float64(r.Clicks), Member: r.UserSub}
		}
		p.RDB.ZAddArgs(cctx, leaderboard.WeekKey(now), redis.ZAddArgs{GT: true, Members: zs})
		p.RDB.Expire(cctx, leaderboard.WeekKey(now), leaderboard.WeekTTL)
	}
	if len(all) > 0 {
		subs := make([]string, len(all))
		for i, r := range all {
			subs[i] = r.UserSub
		}
		if names, err := q.ListProfileNames(cctx, subs); err == nil && len(names) > 0 {
			pairs := make([]any, 0, len(names)*2)
			for _, n := range names {
				pairs = append(pairs, n.UserSub, n.DisplayName)
			}
			p.RDB.HSet(cctx, "profile:names", pairs...)
		}
	}
	p.RDB.SetNX(cctx, "counter:total", sum, 0) // SetNX: never clobber a live counter
}

func (p *Publisher) pollLoop(ctx context.Context) {
	// initialize controller state and make sure the shared difficulty keys
	// exist — IssueChallenge fails closed without them
	l := p.currentL(ctx)
	p.writeDifficulty(ctx, l)
	lastChange := time.Now()
	var ewma float64
	prevStats, _ := p.RDB.Get(ctx, "stats:accepted_total").Int64()
	prevSample := time.Now()

	// -1 forces one frame on startup so a fresh publisher (or SSE clients
	// that reconnected during a publisher restart) sees the current total
	// without waiting for a click.
	var lastPublished int64 = -1
	var pollN int

	tick := time.NewTicker(pollInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}

		total, err := p.readTotal(ctx)
		if err != nil {
			if ctx.Err() == nil {
				p.Logger.Warn("counter poll failed", "err", err)
			}
			continue
		}
		pollN++
		// Freshness signal: set on every successful poll, changed or not,
		// so an idle counter is never mistaken for a frozen loop.
		lastPollUnixtime.Set(float64(time.Now().Unix()))

		if int64(total) != lastPublished {
			frame := map[string]any{"type": "counter", "total": total}
			if p.haveUsers.Load() {
				frame["users"] = p.users.Load()
			}
			p.publishJSON(counterChannel, frame)
			p.claimMilestones(ctx, total)
			lastPublished = int64(total)
		}

		if pollN%leaderboardEveryN == 0 {
			p.broadcastLeaderboard(ctx)
		}

		// controller: EWMA of accepted submits/s → NextL → shared keys
		if s, err := p.RDB.Get(ctx, "stats:accepted_total").Int64(); err == nil || errors.Is(err, redis.Nil) {
			now := time.Now()
			dt := now.Sub(prevSample)
			ewma = pow.EWMA(ewma, float64(s-prevStats)/dt.Seconds(), dt)
			prevStats, prevSample = s, now
			if next, ts := pow.NextL(l, ewma, lastChange, now); next != l {
				l, lastChange = next, ts
				p.writeDifficulty(ctx, l)
				p.Logger.Info("difficulty changed", "L", l, "ewma_rate", ewma)
			}
		}
	}
}

func (p *Publisher) readTotal(ctx context.Context) (uint64, error) {
	cctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	v, err := p.RDB.Get(cctx, "counter:total").Uint64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return v, err
}

func (p *Publisher) broadcastLeaderboard(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	now := time.Now()
	frame := map[string]any{
		"type":     "leaderboard",
		"allTime":  p.renderTop(cctx, leaderboard.AllTimeKey),
		"thisWeek": p.renderTop(cctx, leaderboard.WeekKey(now)),
	}
	p.publishJSON(leaderboardChannel, frame)
}

func (p *Publisher) renderTop(ctx context.Context, key string) []map[string]any {
	top, err := leaderboard.TopN(ctx, p.RDB, key, 20)
	if err != nil || len(top) == 0 {
		return []map[string]any{}
	}
	subs := make([]string, len(top))
	for i, r := range top {
		subs[i] = r.Sub
	}
	names := map[string]string{}
	if vals, err := p.RDB.HMGet(ctx, "profile:names", subs...).Result(); err == nil {
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

// claimMilestones SETNX-claims every reached threshold and publishes only a
// won claim: exactly-once claim, at-most-once announcement.
func (p *Publisher) claimMilestones(ctx context.Context, total uint64) {
	for _, m := range achievements.Milestones {
		if total < m.Threshold {
			return // Milestones are ascending
		}
		key := fmt.Sprintf("milestone:%d", m.Threshold)
		won, err := p.RDB.SetNX(ctx, key, time.Now().Unix(), 0).Result()
		if err != nil {
			p.Logger.Warn("milestone claim failed", "key", key, "err", err)
			return
		}
		if won {
			p.publishJSON(counterChannel, map[string]any{
				"type": "milestone", "threshold": m.Threshold, "title": m.Title,
			})
		}
	}
}

func (p *Publisher) writeDifficulty(ctx context.Context, l uint32) {
	if err := p.RDB.Set(ctx, "pow:L", l, 0).Err(); err != nil {
		p.Logger.Warn("write pow:L failed", "err", err)
	}
	if err := p.RDB.Set(ctx, "pow:min_interval", pow.MinInterval(l), 0).Err(); err != nil {
		p.Logger.Warn("write pow:min_interval failed", "err", err)
	}
}

// currentL restores the shared level across restarts, defaulting to MinL.
func (p *Publisher) currentL(ctx context.Context) uint32 {
	if v, err := p.RDB.Get(ctx, "pow:L").Int64(); err == nil && v >= pow.MinL && v <= pow.MaxL {
		return uint32(v)
	}
	return pow.MinL
}

func (p *Publisher) usersLoop(ctx context.Context) {
	tick := time.NewTicker(usersInterval)
	defer tick.Stop()
	for {
		p.refreshUsers(ctx)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (p *Publisher) refreshUsers(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	n, err := db.New(p.Pool).CountUsers(cctx)
	if err != nil {
		if ctx.Err() == nil {
			p.Logger.Warn("user count refresh failed", "err", err)
		}
		return
	}
	p.users.Store(uint64(n))
	p.haveUsers.Store(true)
}

func (p *Publisher) publishJSON(channel string, v any) {
	if p.Publish == nil {
		return
	}
	body, _ := json.Marshal(v)
	p.Publish(channel, body)
}

func pgDate(t time.Time) pgtype.Date { return pgtype.Date{Time: t, Valid: true} }
