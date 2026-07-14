// Package config loads service configuration from the environment.
package config

import (
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	PGURL         string // PG_URL (required)
	RedisURL      string // REDIS_URL (required)
	AMQPURL       string // AMQP_URL (optional; counter events are best-effort)
	PowSecret     []byte // POW_SECRET (required, std-base64 of 32 raw bytes)
	PowSecretPrev []byte // POW_SECRET_PREV (optional, rotation window)
	PowW0         uint64 // POW_W0 (default 16384 = 2^14 expected hashes/click)
	ListenAddr    string // LISTEN_ADDR (default :9090)
	MetricsAddr   string // METRICS_ADDR (default :9091)
}

func Load() (*Config, error) {
	c := &Config{
		PGURL:       os.Getenv("PG_URL"),
		RedisURL:    os.Getenv("REDIS_URL"),
		AMQPURL:     os.Getenv("AMQP_URL"),
		ListenAddr:  env("LISTEN_ADDR", ":9090"),
		MetricsAddr: env("METRICS_ADDR", ":9091"),
	}
	if c.PGURL == "" {
		return nil, fmt.Errorf("PG_URL is required")
	}
	if c.RedisURL == "" {
		return nil, fmt.Errorf("REDIS_URL is required")
	}
	secret := os.Getenv("POW_SECRET")
	if secret == "" {
		return nil, fmt.Errorf("POW_SECRET is required")
	}
	key, err := base64.StdEncoding.DecodeString(secret)
	if err != nil {
		return nil, fmt.Errorf("POW_SECRET: %w", err)
	}
	c.PowSecret = key
	if prev := os.Getenv("POW_SECRET_PREV"); prev != "" {
		k, err := base64.StdEncoding.DecodeString(prev)
		if err != nil {
			return nil, fmt.Errorf("POW_SECRET_PREV: %w", err)
		}
		c.PowSecretPrev = k
	}
	w0, err := strconv.ParseUint(env("POW_W0", "16384"), 10, 64)
	if err != nil || w0 == 0 {
		return nil, fmt.Errorf("POW_W0 must be a positive integer")
	}
	c.PowW0 = w0
	return c, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
