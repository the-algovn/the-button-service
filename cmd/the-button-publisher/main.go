// the-button-publisher: single-replica broadcast loop for The Button —
// polls the Postgres SUM and publishes counter/milestone frames to
// RabbitMQ, and runs the shared PoW difficulty controller. See
// the-algovn/specs ARCHITECTURE.md.
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
	"github.com/the-algovn/the-button-service/internal/publisher"
	"github.com/the-algovn/the-button-service/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.LoadPublisher()
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

	pub := &publisher.Publisher{
		Pool: pool, RDB: rdb,
		Publish: publisher.NewAMQPPublisher(ctx, cfg.AMQPURL, logger),
		Logger:  logger,
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	go func() {
		_ = (&http.Server{Addr: cfg.MetricsAddr, Handler: mux}).ListenAndServe()
	}()

	logger.Info("the-button-publisher running", "metrics_addr", cfg.MetricsAddr)
	pub.Run(ctx) // blocks until SIGINT/SIGTERM
}
