// the-button-migrate applies the embedded goose migrations to PG_URL and exits.
// It runs as an Argo PreSync hook Job — never in the service process, never by
// hand. See the-algovn/specs ARCHITECTURE.md (Data → Schema) and the
// 2026-07-17 sqlc+goose migrations spec.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/the-algovn/the-button-service/internal/migrate"
)

func main() {
	down := flag.Bool("down", false, "reverse the most recently applied migration instead of applying pending ones")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	url := os.Getenv("PG_URL")
	if url == "" {
		logger.Error("config", "err", "PG_URL is required")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if *down {
		reversed, err := migrate.Down(ctx, url)
		if err != nil {
			logger.Error("migrate down failed", "err", err)
			os.Exit(1)
		}
		logger.Info("migration reversed", "detail", reversed)
		return
	}

	applied, err := migrate.Up(ctx, url)
	if err != nil {
		logger.Error("migrate failed", "err", err)
		os.Exit(1)
	}
	if len(applied) == 0 {
		logger.Info("no pending migrations")
		return
	}
	for _, line := range applied {
		logger.Info("migration applied", "detail", line)
	}
}
