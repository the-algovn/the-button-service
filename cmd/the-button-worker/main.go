// the-button-worker runs the two Kafka consumer groups (counter + progress) and
// the Redis→Postgres snapshot loop. It consumes the `clicks` topic, maintains the
// Redis-authoritative game state, produces the sse.* frames, and restores the last
// snapshot into an empty Redis on cold start. See the-algovn/specs ARCHITECTURE.md.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/the-algovn/the-button-service/internal/config"
	"github.com/the-algovn/the-button-service/internal/counterworker"
	"github.com/the-algovn/the-button-service/internal/kafka"
	"github.com/the-algovn/the-button-service/internal/progressworker"
	"github.com/the-algovn/the-button-service/internal/snapshot"
	"github.com/the-algovn/the-button-service/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.LoadWorker()
	if err != nil {
		logger.Error("config", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := store.NewPGFlush(ctx, cfg.PGURL)
	if err != nil {
		logger.Error("postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	rdb, err := store.NewRedis(ctx, cfg.RedisURL)
	if err != nil {
		logger.Error("redis", "err", err)
		os.Exit(1)
	}
	defer rdb.Close()

	prod, err := kafka.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		logger.Error("kafka producer", "err", err)
		os.Exit(1)
	}
	defer prod.Close()

	snap := &snapshot.Snapshotter{Pool: pool, RDB: rdb, Logger: logger}
	// Cold start: restore the last snapshot BEFORE the consumers apply new events.
	// (No-op when Redis already holds state — see Snapshotter.Restore.)
	if err := snap.Restore(ctx); err != nil {
		logger.Error("snapshot restore", "err", err)
		os.Exit(1)
	}

	cw := &counterworker.Worker{RDB: rdb, Prod: prod, Brokers: cfg.KafkaBrokers, Logger: logger}
	pw := &progressworker.Worker{RDB: rdb, Prod: prod, Brokers: cfg.KafkaBrokers, Logger: logger}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	go func() { _ = (&http.Server{Addr: cfg.MetricsAddr, Handler: mux}).ListenAndServe() }()

	errc := make(chan error, 3)
	go func() { errc <- cw.Run(ctx) }()
	go func() { errc <- pw.Run(ctx) }()
	go func() { errc <- snap.Run(ctx) }()

	logger.Info("the-button-worker running", "metrics_addr", cfg.MetricsAddr)
	select {
	case <-ctx.Done():
		logger.Info("the-button-worker shutting down")
	case err := <-errc:
		// A worker returning before shutdown is a startup failure (e.g. cannot
		// reach Kafka) — surface it and tear down the rest.
		logger.Error("worker exited unexpectedly", "err", err)
		stop()
	}
}
