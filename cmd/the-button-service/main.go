// the-button-service: PoW-gated global click counter. See docs/superpowers/specs.
package main

import (
	"log/slog"
	"os"

	"github.com/the-algovn/the-button-service/internal/config"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("config", "err", err)
		os.Exit(1)
	}
	// Full wiring lands with the gRPC assembly task.
	logger.Info("scaffold ok", "listen", cfg.ListenAddr)
}
