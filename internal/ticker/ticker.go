// Package ticker runs the per-replica counter cache and the elected tick
// leader (spec §8): 1s counter publishes, milestone claims, the shared
// difficulty controller, and the hourly reconcile.
package ticker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/the-algovn/the-button-service/internal/achievements"
	"github.com/the-algovn/the-button-service/internal/pow"
)

// leaderLockKey identifies cluster-wide tick leadership:
// fnv1a64("the-button.tick") = 0x99a46ea8595c12ce as a signed bigint.
const leaderLockKey int64 = -7375648620393262386

const (
	tickInterval      = time.Second
	candidateInterval = 2 * time.Second // non-leaders poll the lock (spec §8: ~2s)
	demoteAfter       = 5 * time.Second // self-demote when the loop lags (spec §8)
	reconcileEvery    = time.Hour
	counterChannel    = "the-button.counter"
)

type Ticker struct {
	PGURL   string        // dedicated leader connection — never the pool
	Pool    *pgxpool.Pool // SUM fallback + reconcile
	RDB     *redis.Client
	Publish func(channel string, body []byte) // best-effort; nil disables publishing
	Logger  *slog.Logger

	total     atomic.Uint64
	haveTotal atomic.Bool
}

// Total returns the cached global counter and whether a value has been
// loaded yet. Correct from any pod, even with RabbitMQ or Redis down.
func (t *Ticker) Total() (uint64, bool) {
	return t.total.Load(), t.haveTotal.Load()
}

// Run starts the every-replica cache loop and the leader-election loop and
// blocks until ctx is done.
func (t *Ticker) Run(ctx context.Context) {
	go t.cacheLoop(ctx)
	t.leaderLoop(ctx)
}

func (t *Ticker) cacheLoop(ctx context.Context) {
	tick := time.NewTicker(tickInterval)
	defer tick.Stop()
	for {
		if n, err := t.readTotal(ctx); err == nil {
			t.total.Store(n)
			t.haveTotal.Store(true)
		} else if ctx.Err() == nil {
			t.Logger.Warn("counter cache refresh failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// readTotal prefers Redis and falls back to the durable SUM (spec §8).
func (t *Ticker) readTotal(ctx context.Context) (uint64, error) {
	if v, err := t.RDB.Get(ctx, "counter:global").Uint64(); err == nil {
		return v, nil
	}
	var sum int64
	if err := t.Pool.QueryRow(ctx, `SELECT COALESCE(SUM(clicks), 0) FROM user_clicks`).Scan(&sum); err != nil {
		return 0, err
	}
	return uint64(sum), nil
}

// leaderLoop holds ONE dedicated non-pooled connection per attempt;
// leadership == that connection's health. Closing it releases the lock.
func (t *Ticker) leaderLoop(ctx context.Context) {
	for {
		conn, err := pgx.Connect(ctx, t.PGURL)
		if err == nil {
			var got bool
			if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, leaderLockKey).Scan(&got); err == nil && got {
				t.Logger.Info("tick leadership acquired")
				t.lead(ctx, conn)
				t.Logger.Info("tick leadership released")
			}
			_ = conn.Close(context.WithoutCancel(ctx))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(candidateInterval):
		}
	}
}

func (t *Ticker) lead(ctx context.Context, conn *pgx.Conn) {
	// initialize controller state and make sure the shared difficulty keys
	// exist — IssueChallenge fails closed without them
	l := t.currentL(ctx)
	t.writeDifficulty(ctx, l)
	lastChange := time.Now()
	var ewma float64
	prevStats, _ := t.RDB.Get(ctx, "stats:accepted_total").Int64()
	prevSample := time.Now()

	var lastPublished int64 = -1
	lastTick := time.Now()
	nextReconcile := time.Now().Add(reconcileEvery)

	tick := time.NewTicker(tickInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		// self-demote when we could not keep up — closing the conn lets
		// another replica take over within ~2s
		if lag := time.Since(lastTick); lag > demoteAfter {
			t.Logger.Warn("tick loop lagged, demoting", "lag", lag)
			return
		}
		lastTick = time.Now()

		// the lock connection's health IS leadership
		if err := conn.Ping(ctx); err != nil {
			t.Logger.Warn("leader connection lost", "err", err)
			return
		}

		total, err := t.leaderTotal(ctx)
		if err != nil {
			t.Logger.Warn("leader total read failed", "err", err)
			continue
		}
		if int64(total) != lastPublished {
			t.publishJSON(counterChannel, map[string]any{"type": "counter", "total": total})
			t.claimMilestones(ctx, total)
			lastPublished = int64(total)
		}

		// controller: EWMA of accepted submits/s → NextL → shared keys
		if s, err := t.RDB.Get(ctx, "stats:accepted_total").Int64(); err == nil || errors.Is(err, redis.Nil) {
			now := time.Now()
			dt := now.Sub(prevSample)
			ewma = pow.EWMA(ewma, float64(s-prevStats)/dt.Seconds(), dt)
			prevStats, prevSample = s, now
			if next, ts := pow.NextL(l, ewma, lastChange, now); next != l {
				l, lastChange = next, ts
				t.writeDifficulty(ctx, l)
				t.Logger.Info("difficulty changed", "L", l, "ewma_rate", ewma)
			}
		}

		if time.Now().After(nextReconcile) {
			t.reconcile(ctx)
			nextReconcile = time.Now().Add(reconcileEvery)
		}
	}
}

// leaderTotal reads the hot counter, seeding it from the durable SUM when
// missing — with SETNX, never SET, so concurrent INCRBYs survive (spec §8).
func (t *Ticker) leaderTotal(ctx context.Context) (uint64, error) {
	v, err := t.RDB.Get(ctx, "counter:global").Uint64()
	if err == nil {
		return v, nil
	}
	if !errors.Is(err, redis.Nil) {
		return 0, err
	}
	var sum int64
	if err := t.Pool.QueryRow(ctx, `SELECT COALESCE(SUM(clicks), 0) FROM user_clicks`).Scan(&sum); err != nil {
		return 0, err
	}
	if err := t.RDB.SetNX(ctx, "counter:global", sum, 0).Err(); err != nil {
		return 0, err
	}
	return t.RDB.Get(ctx, "counter:global").Uint64()
}

// claimMilestones SETNX-claims every reached threshold and publishes only a
// won claim: exactly-once claim, at-most-once announcement (spec §8).
func (t *Ticker) claimMilestones(ctx context.Context, total uint64) {
	for _, m := range achievements.Milestones {
		if total < m.Threshold {
			return // Milestones are ascending
		}
		key := fmt.Sprintf("milestone:%d", m.Threshold)
		won, err := t.RDB.SetNX(ctx, key, time.Now().Unix(), 0).Result()
		if err != nil {
			t.Logger.Warn("milestone claim failed", "key", key, "err", err)
			return
		}
		if won {
			t.publishJSON(counterChannel, map[string]any{
				"type": "milestone", "threshold": m.Threshold, "title": m.Title,
			})
		}
	}
}

func (t *Ticker) writeDifficulty(ctx context.Context, l uint32) {
	if err := t.RDB.Set(ctx, "pow:L", l, 0).Err(); err != nil {
		t.Logger.Warn("write pow:L failed", "err", err)
	}
	if err := t.RDB.Set(ctx, "pow:min_interval", pow.MinInterval(l), 0).Err(); err != nil {
		t.Logger.Warn("write pow:min_interval failed", "err", err)
	}
}

// currentL restores the shared level across failovers, defaulting to MinL.
func (t *Ticker) currentL(ctx context.Context) uint32 {
	if v, err := t.RDB.Get(ctx, "pow:L").Int64(); err == nil && v >= pow.MinL && v <= pow.MaxL {
		return uint32(v)
	}
	return pow.MinL
}

// reconcile heals counter drift: INCRBY the delta, never SET — a SET would
// clobber concurrent increments (spec §8).
func (t *Ticker) reconcile(ctx context.Context) {
	var sum int64
	if err := t.Pool.QueryRow(ctx, `SELECT COALESCE(SUM(clicks), 0) FROM user_clicks`).Scan(&sum); err != nil {
		t.Logger.Warn("reconcile SUM failed", "err", err)
		return
	}
	cur, err := t.RDB.Get(ctx, "counter:global").Int64()
	if err != nil {
		t.Logger.Warn("reconcile GET failed", "err", err)
		return
	}
	if drift := sum - cur; drift != 0 {
		t.Logger.Warn("counter drift healed", "drift", drift)
		if err := t.RDB.IncrBy(ctx, "counter:global", drift).Err(); err != nil {
			t.Logger.Warn("reconcile INCRBY failed", "err", err)
		}
	}
}

func (t *Ticker) publishJSON(channel string, v any) {
	if t.Publish == nil {
		return
	}
	body, _ := json.Marshal(v)
	t.Publish(channel, body)
}
