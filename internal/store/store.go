// Package store wires Postgres (durable truth — and since the outbox
// removal, the ONLY counter truth) and Redis (hot control state: PoW,
// throttle, difficulty, milestones). Redis maxmemory-policy MUST be
// noeviction: the PoW burn keys (pow:<id>) are what prevent challenge
// replay/double-credit, so evicting one under memory pressure is a
// correctness bug, not just a cache miss.
package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// NewPG opens a pgx pool (MaxConns 10, statement_timeout 2s — spec §7).
// Schema is NOT applied here: migrations run in the PreSync Job
// (cmd/the-button-migrate) — see the 2026-07-17 sqlc+goose migrations spec.
func NewPG(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse PG_URL: %w", err)
	}
	cfg.MaxConns = 10
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "2000"
	return pgxpool.NewWithConfig(ctx, cfg)
}

// NewPGFlush is the worker's pool for the periodic snapshot dump. Same as NewPG
// but with synchronous_commit=off: the durable snapshot may lag Redis by a commit
// or two — the design's ~1-2s loss budget accepts that in exchange for far higher
// batched-write throughput. Redis stays authoritative.
func NewPGFlush(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse PG_URL: %w", err)
	}
	cfg.MaxConns = 10
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "2000"
	cfg.ConnConfig.RuntimeParams["synchronous_commit"] = "off"
	return pgxpool.NewWithConfig(ctx, cfg)
}

// NewRedis parses REDIS_URL and verifies connectivity with a PING.
func NewRedis(ctx context.Context, url string) (*redis.Client, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse REDIS_URL: %w", err)
	}
	rdb := redis.NewClient(opt)
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return rdb, nil
}
