// Package store wires Postgres (durable truth — and since the outbox
// removal, the ONLY counter truth) and Redis (hot control state: PoW,
// throttle, difficulty, milestones).
package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/the-algovn/the-button-service/internal/db"
)

// NewPG opens a pgx pool (MaxConns 10, statement_timeout 2s — spec §7) and
// applies the idempotent schema.
func NewPG(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse PG_URL: %w", err)
	}
	cfg.MaxConns = 10
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "2000"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	// Schema application is serialized with a transaction-scoped advisory lock:
	// CREATE TABLE IF NOT EXISTS races on the catalog when replicas start
	// simultaneously against a fresh database, and the loser would fail fatally.
	if err := applySchema(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return pool, nil
}

// schemaLockKey is an arbitrary constant shared by all replicas.
const schemaLockKey int64 = 7238410394821017561

// applySchema applies the idempotent schema within a transaction-scoped advisory lock.
// This serializes concurrent DDL attempts across replicas starting simultaneously
// against a fresh database, preventing catalog conflicts.
func applySchema(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", schemaLockKey); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, db.Schema); err != nil {
		return err
	}
	return tx.Commit(ctx)
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
