// Package ticker runs the per-replica counter cache and the elected tick
// leader (spec §8): 1s counter publishes, milestone claims, the shared
// difficulty controller, and the 30s counter-outbox sweeper.
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
	"github.com/the-algovn/the-button-service/internal/store"
)

// leaderLockKey identifies cluster-wide tick leadership:
// fnv1a64("the-button.tick") = 0x99a46ea8595c12ce as a signed bigint.
const leaderLockKey int64 = -7375648620393262386

const (
	tickInterval      = time.Second
	candidateInterval = 2 * time.Second // non-leaders poll the lock (spec §8: ~2s)
	demoteAfter       = 5 * time.Second // self-demote when the loop lags (spec §8)
	counterChannel    = "the-button.counter"

	// leaderCallTimeout bounds every call issued on the dedicated leader
	// connection, below the 5s self-demote SLA so a stalled connection is
	// detected, not waited on.
	leaderCallTimeout = 3 * time.Second

	// sweepInterval is how often the leader sweeps counter_outbox, in its
	// own goroutine — never inline in the 1s tick loop, or a slow sweep
	// would trip the >5s self-demote check above.
	sweepInterval = 30 * time.Second
	sweepLimit    = 500

	// outboxStaleAfter bounds how old an unapplied outbox row may be before
	// the sweeper refuses to touch it: past this age its "applied:<id>"
	// marker may already have expired (store.AppliedMarkerTTLSeconds is 1h),
	// so a re-apply could no longer be guaranteed idempotent. Must sit
	// strictly between the ~30s in-flight window the sweeper query already
	// excludes (below which a row is still expected to be in flight, not
	// orphaned) and store.AppliedMarkerTTLSeconds (above which the marker
	// may be gone) — 30m gives wide margin on both sides of that window
	// while still catching orphaned rows well before their marker expires.
	outboxStaleAfter = 30 * time.Minute

	// metricsInterval is how often the leader refreshes the observation-only
	// divergence/outbox-depth gauges (Fix C) — its own goroutine, same
	// reasoning as sweepLoop: never inline in the 1s tick loop.
	metricsInterval = 60 * time.Second

	// usersRefreshInterval is how often each replica recounts distinct
	// contributors for the display-only session stats — a slow cadence, never
	// in the 1s tick loop.
	usersRefreshInterval = 15 * time.Second
)

type Ticker struct {
	PGURL   string        // dedicated leader connection — never the pool
	Pool    *pgxpool.Pool // SUM fallback + outbox sweeper
	RDB     *redis.Client
	Publish func(channel string, body []byte) // best-effort; nil disables publishing
	Logger  *slog.Logger

	total     atomic.Uint64
	haveTotal atomic.Bool

	users     atomic.Uint64
	haveUsers atomic.Bool
}

// Total returns the cached global counter and whether a value has been
// loaded yet. Correct from any pod, even with RabbitMQ or Redis down.
func (t *Ticker) Total() (uint64, bool) {
	return t.total.Load(), t.haveTotal.Load()
}

// Users returns the cached distinct-contributor count and whether one has been
// loaded yet. Display-only (session stats); never load-bearing for accounting.
func (t *Ticker) Users() (uint64, bool) {
	return t.users.Load(), t.haveUsers.Load()
}

// Run starts the every-replica cache loop and the leader-election loop and
// blocks until ctx is done.
func (t *Ticker) Run(ctx context.Context) {
	go t.cacheLoop(ctx)
	go t.usersLoop(ctx)
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

// usersLoop refreshes the distinct-contributor count on a slow cadence. It is
// display-only, so a failed refresh just keeps the last value.
func (t *Ticker) usersLoop(ctx context.Context) {
	t.refreshUsers(ctx)
	tick := time.NewTicker(usersRefreshInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			t.refreshUsers(ctx)
		}
	}
}

func (t *Ticker) refreshUsers(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, leaderCallTimeout)
	defer cancel()
	var n int64
	if err := t.Pool.QueryRow(cctx, `SELECT COUNT(*) FROM user_clicks`).Scan(&n); err != nil {
		if ctx.Err() == nil {
			t.Logger.Warn("user count refresh failed", "err", err)
		}
		return
	}
	t.users.Store(uint64(n))
	t.haveUsers.Store(true)
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
	// the sweeper runs on its own timer in its own goroutine for as long as
	// this replica holds leadership; cancel it the moment lead() returns
	// (demotion or shutdown) so it never outlives leadership.
	leadCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go t.sweepLoop(leadCtx)
	go t.metricsLoop(leadCtx)

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
		// Freshness signal for the frozen-counter alert: set on every
		// successful tick, not gated on total changing, so an idle counter
		// (no click traffic) is never mistaken for a stuck tick loop.
		lastTickUnixtime.Set(float64(time.Now().Unix()))
		if int64(total) != lastPublished {
			frame := map[string]any{"type": "counter", "total": total}
			if users, ok := t.Users(); ok {
				frame["users"] = users
			}
			t.publishJSON(counterChannel, frame)
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
	}
}

// leaderTotal reads the hot counter, seeding it from the durable SUM when
// missing — with SETNX, never SET, so a concurrent Submit's apply (via the
// Lua script) survives (spec §8). The SUM already reflects every committed
// batch, so a WON seed also purges any outbox rows created at-or-before the
// SUM's own read timestamp: left alone, the sweeper would apply them on top
// of the seed and over-count, since their clicks are already included in it.
func (t *Ticker) leaderTotal(ctx context.Context) (uint64, error) {
	v, err := t.RDB.Get(ctx, "counter:global").Uint64()
	if err == nil {
		return v, nil
	}
	if !errors.Is(err, redis.Nil) {
		return 0, err
	}
	var sum int64
	var pgNow time.Time
	if err := t.Pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(clicks), 0), now() FROM user_clicks`).Scan(&sum, &pgNow); err != nil {
		return 0, err
	}
	won, err := t.RDB.SetNX(ctx, "counter:global", sum, 0).Result()
	if err != nil {
		return 0, err
	}
	if won {
		t.purgeSeededOutbox(ctx, pgNow)
	}
	return t.RDB.Get(ctx, "counter:global").Uint64()
}

// purgeSeededOutbox handles every outbox row already reflected in the seed
// SUM (created at-or-before pgNow). A batch that committed just before pgNow
// may still have its post-commit apply in flight (e.g. go-redis retrying
// across the very Redis restart that triggered this seed): its clicks are
// already inside the SUM, and if its row were simply deleted, the delayed
// apply would later find applied:<id> gone (wiped with Redis) and INCRBY
// again — double-counting with no row left to notice. So each row's
// applied:<id> marker is stamped BEFORE the row is deleted, making that
// delayed apply a no-op. Markers before delete also means a crash mid-purge
// leaves rows the sweeper will still pick up, but idempotently — their
// markers already exist, so its apply is a no-op and it deletes them
// normally on its own pass.
func (t *Ticker) purgeSeededOutbox(ctx context.Context, pgNow time.Time) {
	rows, err := t.Pool.Query(ctx, `SELECT id FROM counter_outbox WHERE created_at <= $1`, pgNow)
	if err != nil {
		t.Logger.Warn("post-seed outbox read failed", "err", err)
		return
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			t.Logger.Warn("post-seed outbox scan failed", "err", err)
			return
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Logger.Warn("post-seed outbox read failed", "err", err)
		return
	}
	rows.Close()
	if len(ids) == 0 {
		return
	}

	markerTTL := time.Duration(store.AppliedMarkerTTLSeconds) * time.Second
	pipe := t.RDB.Pipeline()
	for _, id := range ids {
		pipe.Set(ctx, "applied:"+id, "1", markerTTL)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Logger.Warn("post-seed marker write failed", "err", err)
		return
	}

	if _, err := t.Pool.Exec(ctx,
		`DELETE FROM counter_outbox WHERE created_at <= $1`, pgNow); err != nil {
		t.Logger.Warn("post-seed outbox purge failed", "err", err)
	}
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

// sweepLoop runs the outbox sweeper on its own timer for as long as ctx is
// live (leadership held) — in its own goroutine so a slow sweep can never
// block the 1s tick loop in lead() and trip its >5s self-demote check.
func (t *Ticker) sweepLoop(ctx context.Context) {
	tick := time.NewTicker(sweepInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		if err := t.sweep(ctx); err != nil {
			t.Logger.Warn("outbox sweep failed", "err", err)
		}
	}
}

// sweep applies every counter_outbox row old enough to be outside the
// in-flight window: a batch whose post-commit apply never happened (crash,
// ambiguous commit, Redis blip). Replaces the old diff-based reconcile — no
// diff between Postgres and Redis can distinguish a lost apply from one
// merely in flight, since Redis structurally lags Postgres by that same
// window (commit lands, then the apply); the outbox sidesteps the diff
// entirely by keying every apply to an idempotency marker instead (spec §8).
func (t *Ticker) sweep(ctx context.Context) error {
	rows, err := t.Pool.Query(ctx,
		`SELECT id, clicks, created_at FROM counter_outbox WHERE created_at < now() - interval '30 seconds' ORDER BY created_at LIMIT $1`,
		sweepLimit)
	if err != nil {
		return err
	}
	type outboxRow struct {
		id        string
		clicks    int64
		createdAt time.Time
	}
	var pending []outboxRow
	for rows.Next() {
		var r outboxRow
		if err := rows.Scan(&r.id, &r.clicks, &r.createdAt); err != nil {
			rows.Close()
			return err
		}
		pending = append(pending, r)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()

	staleBefore := time.Now().Add(-outboxStaleAfter)
	applied := 0
	for _, r := range pending {
		// Past the marker TTL, applied:<id> may already have expired, so a
		// re-apply is no longer guaranteed idempotent (spec: never
		// double-count). Skip and surface it via the metric instead.
		if r.createdAt.Before(staleBefore) {
			t.Logger.Warn("outbox row older than applied-marker TTL, skipping to avoid double-count",
				"id", r.id, "created_at", r.createdAt)
			outboxStale.Inc()
			continue
		}
		// Idempotent: a no-op if this id's apply already landed.
		if err := store.ApplyCounter(ctx, t.RDB, r.id, r.clicks); err != nil {
			if errors.Is(err, store.ErrCounterNotSeeded) {
				// Nothing can apply until the tick leader seeds
				// counter:global from Postgres; every remaining row (all
				// newer, same Redis state) would fail the same way, so stop
				// this pass rather than re-check each one.
				t.Logger.Info("outbox sweep stopped: counter not seeded yet", "id", r.id)
				break
			}
			t.Logger.Warn("sweep apply failed", "id", r.id, "err", err)
			continue
		}
		if _, err := t.Pool.Exec(ctx, `DELETE FROM counter_outbox WHERE id = $1`, r.id); err != nil {
			t.Logger.Warn("sweep delete failed", "id", r.id, "err", err)
			continue
		}
		applied++
	}
	if applied > 0 {
		t.Logger.Info("outbox swept", "applied", applied)
	}
	return nil
}

func (t *Ticker) publishJSON(channel string, v any) {
	if t.Publish == nil {
		return
	}
	body, _ := json.Marshal(v)
	t.Publish(channel, body)
}
