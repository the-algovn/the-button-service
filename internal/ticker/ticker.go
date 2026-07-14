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
	keyCounterGlobal  = "counter:global"

	// leaderCallTimeout bounds every call issued on the dedicated leader
	// connection, below the 5s self-demote SLA so a stalled connection is
	// detected, not waited on.
	leaderCallTimeout = 3 * time.Second

	// reconcileSettle is long enough for any in-flight Submit's post-commit
	// INCRBY to land before a drift observation is trusted.
	reconcileSettle = 5 * time.Second
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
		conn, err := t.dialLeaderConn(ctx)
		if err == nil {
			lctx, cancel := context.WithTimeout(ctx, leaderCallTimeout)
			var got bool
			err := conn.QueryRow(lctx, `SELECT pg_try_advisory_lock($1)`, leaderLockKey).Scan(&got)
			cancel()
			if err == nil && got {
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

// dialLeaderConn opens the dedicated (never-pooled) leader connection with a
// server-side statement_timeout below the 5s self-demote SLA, so a stuck
// statement is cancelled by Postgres rather than blocking the tick loop.
func (t *Ticker) dialLeaderConn(ctx context.Context) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(t.PGURL)
	if err != nil {
		return nil, err
	}
	cfg.RuntimeParams["statement_timeout"] = "4000"
	return pgx.ConnectConfig(ctx, cfg)
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

		// the lock connection's health IS leadership; bounded below the
		// self-demote SLA so a stalled connection is detected, not waited on
		pctx, cancel := context.WithTimeout(ctx, leaderCallTimeout)
		err := conn.Ping(pctx)
		cancel()
		if err != nil {
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
			if err := t.reconcile(ctx); err != nil {
				t.Logger.Warn("reconcile failed", "err", err)
			}
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

// redisTotal reads the hot counter, treating a missing key as zero.
func redisTotal(ctx context.Context, rdb *redis.Client) (uint64, error) {
	v, err := rdb.Get(ctx, keyCounterGlobal).Uint64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return v, err
}

// pgTotal reads the durable SUM across all users.
func pgTotal(ctx context.Context, pool *pgxpool.Pool) (uint64, error) {
	var sum int64
	if err := pool.QueryRow(ctx, `SELECT COALESCE(SUM(clicks), 0) FROM user_clicks`).Scan(&sum); err != nil {
		return 0, err
	}
	return uint64(sum), nil
}

func abs64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

// reconcile heals counter drift: INCRBY the delta, never SET — a SET would
// clobber concurrent increments (spec §8).
//
// Read Redis BEFORE Postgres. Submit() commits to PG and only then INCRBYs
// Redis, so with this order any batch that lands between the two reads is
// either (a) already in the Redis value we read — and also in the later SUM,
// so it cancels — or (b) not yet in Redis but present in SUM, making drift
// look positive by exactly that batch, which its own pending INCRBY will
// then apply too. To avoid double-applying (b), we require the drift to be
// stable across a second confirming read before acting on it.
func (t *Ticker) reconcile(ctx context.Context) error {
	before, err := redisTotal(ctx, t.RDB)
	if err != nil {
		return err
	}
	sum, err := pgTotal(ctx, t.Pool)
	if err != nil {
		return err
	}
	drift := int64(sum) - int64(before)
	if drift == 0 {
		return nil
	}
	// Settle window: let any in-flight Submit finish its post-commit INCRBY,
	// then recompute. Only a drift that persists is real (a lost INCRBY from a
	// crash or an ambiguous commit), not an artifact of a batch mid-flight.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(reconcileSettle):
	}
	after, err := redisTotal(ctx, t.RDB)
	if err != nil {
		return err
	}
	sum2, err := pgTotal(ctx, t.Pool)
	if err != nil {
		return err
	}
	drift2 := int64(sum2) - int64(after)
	if drift2 == 0 {
		return nil
	}
	// Apply the SMALLER magnitude of the two observations, and only when both
	// agree in sign — a conservative correction never overshoots.
	apply := drift2
	if (drift > 0) != (drift2 > 0) {
		return nil // unsettled; try again next cycle
	}
	if abs64(drift) < abs64(drift2) {
		apply = drift
	}
	if _, err := t.RDB.IncrBy(ctx, keyCounterGlobal, apply).Result(); err != nil {
		return err
	}
	t.Logger.Info("counter reconciled", "drift", apply, "pg_total", sum2, "redis_total", after)
	return nil
}

func (t *Ticker) publishJSON(channel string, v any) {
	if t.Publish == nil {
		return
	}
	body, _ := json.Marshal(v)
	t.Publish(channel, body)
}
