// the-button-service: PoW-gated global click counter. See the-algovn/specs ARCHITECTURE.md.
package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	buttonv1 "github.com/the-algovn/protos/gen/go/algovn/button/v1"
	"github.com/the-algovn/the-button-service/internal/config"
	"github.com/the-algovn/the-button-service/internal/countercache"
	"github.com/the-algovn/the-button-service/internal/server"
	"github.com/the-algovn/the-button-service/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		logger.Error("config", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := store.NewPG(ctx, cfg.PGURL)
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

	cache := &countercache.Cache{Pool: pool, Logger: logger}
	go cache.Run(ctx)

	keys := [][]byte{cfg.PowSecret}
	if cfg.PowSecretPrev != nil {
		keys = append(keys, cfg.PowSecretPrev)
	}
	srv := &server.Server{
		Pool: pool, RDB: rdb, Tick: cache, Logger: logger,
		W0: cfg.PowW0, Keys: keys,
	}

	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		logger.Error("listen failed", "err", err)
		os.Exit(1)
	}
	gs := grpc.NewServer()
	buttonv1.RegisterButtonServiceServer(gs, srv)
	healthpb.RegisterHealthServer(gs, health.NewServer())
	reflection.Register(gs)

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		_ = (&http.Server{Addr: cfg.MetricsAddr, Handler: mux}).ListenAndServe()
	}()
	go func() {
		<-ctx.Done()
		gs.GracefulStop()
	}()
	logger.Info("the-button-service listening", "addr", cfg.ListenAddr)
	if err := gs.Serve(lis); err != nil {
		logger.Error("serve failed", "err", err)
		os.Exit(1)
	}
}
