package ticker

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/the-algovn/the-button-service/internal/db"
)

// These three metrics are observation-only: refreshing them never writes to
// Redis or Postgres beyond a plain SUM/GET/count read, and nothing in this
// service auto-corrects divergence it observes here — a diff between
// Postgres and Redis can never tell a lost increment from one merely in
// flight (spec §8), so healing is a manual, documented runbook step, not
// automation.
var (
	counterDivergence = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "the_button_counter_divergence",
		Help: "SUM(user_clicks) - GET counter:global, refreshed every 60s by the tick leader. Observation only — never used to correct Redis. Non-zero is expected transiently (in-flight batches); a persistently growing value is the alert signal.",
	})
	outboxDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "the_button_counter_outbox_depth",
		Help: "count(*) FROM counter_outbox, refreshed every 60s by the tick leader. A growing value is the early warning for a stuck sweeper.",
	})
	outboxStale = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "the_button_counter_outbox_stale",
		Help: "Outbox rows the sweeper refused to (re-)apply because they are older than the applied:<id> marker TTL and could no longer be guaranteed idempotent.",
	})

	// lastTickUnixtime is a freshness signal, not observation-only bookkeeping:
	// ButtonServiceDown only fires when every replica is unreachable, so a
	// single replica that scrapes as up but never wins (or loses and never
	// regains) tick leadership — the runbook's documented gap — would
	// otherwise go undetected while the public counter sits frozen. Set once
	// per successful tick in lead() regardless of whether the total actually
	// changed, so a genuinely idle counter (no click traffic) never masquerades
	// as a stuck tick loop.
	lastTickUnixtime = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "the_button_last_tick_unixtime",
		Help: "Unix timestamp of the tick leader's last successful tick, refreshed every ~1s while this replica holds leadership. time() - this growing means the tick loop is stuck.",
	})
)

func init() {
	prometheus.MustRegister(counterDivergence, outboxDepth, outboxStale, lastTickUnixtime)
}

// metricsLoop refreshes the divergence/outbox-depth gauges on its own timer
// for as long as ctx is live (leadership held) — its own goroutine so a slow
// read can never block the 1s tick loop in lead() and trip its self-demote
// check, same reasoning as sweepLoop.
func (t *Ticker) metricsLoop(ctx context.Context) {
	tick := time.NewTicker(metricsInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		t.refreshDivergenceMetrics(ctx)
	}
}

// refreshDivergenceMetrics samples SUM(user_clicks), GET counter:global, and
// count(*) FROM counter_outbox and publishes them as gauges. It is read-only
// against both stores by design (Fix C) — this must never turn into an
// auto-correction path.
func (t *Ticker) refreshDivergenceMetrics(ctx context.Context) {
	sum, err := db.New(t.Pool).SumUserClicks(ctx)
	if err != nil {
		t.Logger.Warn("divergence metric: SUM read failed", "err", err)
		return
	}
	counter, err := t.RDB.Get(ctx, "counter:global").Int64()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Logger.Warn("divergence metric: counter GET failed", "err", err)
		return
	}
	counterDivergence.Set(float64(sum - counter))

	depth, err := db.New(t.Pool).CountOutbox(ctx)
	if err != nil {
		t.Logger.Warn("outbox depth metric: read failed", "err", err)
		return
	}
	outboxDepth.Set(float64(depth))
}
