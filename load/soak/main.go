// Command soak drives the-button's per-batch Postgres transaction shape at a
// fixed target rate against a THROWAWAY database, reporting commit-latency
// percentiles to validate the ~3ms/txn capacity model (spec §12).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/the-algovn/the-button-service/internal/db"
)

func main() {
	dsn := flag.String("dsn", "", "postgres DSN for the throwaway database")
	rate := flag.Int("rate", 1000, "target transactions per second")
	dur := flag.Duration("duration", 60*time.Second, "soak duration")
	users := flag.Int("users", 5000, "distinct user_sub values cycled (contention model)")
	flag.Parse()
	if *dsn == "" {
		log.Fatal("--dsn required")
	}

	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(*dsn)
	if err != nil {
		log.Fatalf("parse dsn: %v", err)
	}
	cfg.MaxConns = 10 // matches the service's per-replica pool (spec §7)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("ping: %v", err) // fails loudly if the DB is absent — the RED check
	}

	// Idempotent schema (single source: internal/db/schema.sql).
	mustExec(ctx, pool, db.Schema)

	var (
		lat   []time.Duration
		latMu sync.Mutex
		ok    int64
		fail  int64
	)
	work := make(chan string, *rate)
	var wg sync.WaitGroup
	workers := *rate / 100
	if workers < 8 {
		workers = 8
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sub := range work {
				t0 := time.Now()
				if err := oneTxn(ctx, pool, sub); err != nil {
					atomic.AddInt64(&fail, 1)
					continue
				}
				d := time.Since(t0)
				atomic.AddInt64(&ok, 1)
				latMu.Lock()
				lat = append(lat, d)
				latMu.Unlock()
			}
		}()
	}

	// Rate pacing: emit one job every 1s/rate.
	tick := time.NewTicker(time.Second / time.Duration(*rate))
	defer tick.Stop()
	deadline := time.After(*dur)
	i := 0
loop:
	for {
		select {
		case <-deadline:
			break loop
		case <-tick.C:
			select {
			case work <- fmt.Sprintf("soak-%d", rand.Intn(*users)):
			default: // backpressure: pool saturated, drop the tick (records a shortfall)
			}
			i++
		}
	}
	close(work)
	wg.Wait()

	latMu.Lock()
	sort.Slice(lat, func(a, b int) bool { return lat[a] < lat[b] })
	latMu.Unlock()
	fmt.Printf("target_rate=%d duration=%s ok=%d fail=%d achieved_rate=%.0f/s\n",
		*rate, *dur, ok, fail, float64(ok)/dur.Seconds())
	if len(lat) > 0 {
		fmt.Printf("commit_latency p50=%s p95=%s p99=%s max=%s\n",
			pct(lat, 50), pct(lat, 95), pct(lat, 99), lat[len(lat)-1])
	}
	if fail > 0 {
		os.Exit(1)
	}
}

func oneTxn(ctx context.Context, pool *pgxpool.Pool, sub string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	q := db.New(tx)
	if _, err := q.UpsertUserClicks(ctx, db.UpsertUserClicksParams{UserSub: sub, Clicks: 100}); err != nil {
		return err
	}
	if _, err := q.InsertUserAchievement(ctx, db.InsertUserAchievementParams{UserSub: sub, AchievementID: "mvh"}); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	return tx.Commit(ctx)
}

func mustExec(ctx context.Context, pool *pgxpool.Pool, sql string) {
	if _, err := pool.Exec(ctx, sql); err != nil {
		log.Fatalf("exec %q: %v", sql, err)
	}
}

func pct(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := p * len(sorted) / 100
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}
