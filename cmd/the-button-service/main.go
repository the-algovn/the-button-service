// the-button-service: PoW-gated global click counter. See docs/superpowers/specs.
package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	buttonv1 "github.com/the-algovn/protos/gen/go/algovn/button/v1"
	"github.com/the-algovn/the-button-service/internal/config"
	"github.com/the-algovn/the-button-service/internal/server"
	"github.com/the-algovn/the-button-service/internal/store"
	"github.com/the-algovn/the-button-service/internal/ticker"
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

	var publish func(string, []byte)
	if cfg.AMQPURL != "" {
		publish = newPublisher(ctx, cfg.AMQPURL, logger)
	} else {
		logger.Warn("AMQP_URL not set; counter events will not publish")
	}

	tick := &ticker.Ticker{
		PGURL: cfg.PGURL, Pool: pool, RDB: rdb,
		Publish: publish, Logger: logger,
	}
	go tick.Run(ctx)

	keys := [][]byte{cfg.PowSecret}
	if cfg.PowSecretPrev != nil {
		keys = append(keys, cfg.PowSecretPrev)
	}
	srv := &server.Server{
		Pool: pool, RDB: rdb, Tick: tick, Logger: logger,
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

// newPublisher returns a fire-and-forget AMQP publish func; failures are
// logged, never fatal — events are best-effort by design. (Pattern copied
// from api-control-plane's demo-service.)
func newPublisher(ctx context.Context, url string, logger *slog.Logger) func(string, []byte) {
	type conn struct {
		ch *amqp.Channel
		c  *amqp.Connection
	}
	var mu sync.Mutex // ticker + future callers may publish concurrently
	var cur *conn
	dial := func() *conn {
		// Bounded dial: a hung broker must not stall the leader's tick loop.
		c, err := amqp.DialConfig(url, amqp.Config{Dial: amqp.DefaultDial(5 * time.Second)})
		if err != nil {
			logger.Warn("amqp dial failed", "err", err)
			return nil
		}
		ch, err := c.Channel()
		if err != nil {
			_ = c.Close()
			return nil
		}
		if err := ch.ExchangeDeclare("events", "topic", true, false, false, false, nil); err != nil {
			_ = c.Close()
			return nil
		}
		return &conn{ch: ch, c: c}
	}
	return func(channel string, body []byte) {
		mu.Lock()
		defer mu.Unlock()
		if cur == nil || cur.c.IsClosed() || cur.ch.IsClosed() {
			cur = dial()
			if cur == nil {
				return
			}
		}
		pubCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		err := cur.ch.PublishWithContext(pubCtx, "events", channel, false, false,
			amqp.Publishing{ContentType: "application/json", Body: body})
		if err != nil {
			logger.Warn("publish failed", "channel", channel, "err", err)
			cur = nil
		}
	}
}
