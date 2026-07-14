// Package store wires Postgres (durable personal truth) and Redis (hot
// control state) per the design spec §7.
package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// Schema is the full DDL (spec §7), idempotent so every replica applies it
// at startup. No migration framework by design.
const Schema = `
CREATE TABLE IF NOT EXISTS user_clicks (user_sub text PRIMARY KEY, clicks bigint NOT NULL);
CREATE TABLE IF NOT EXISTS user_achievements (
  user_sub text NOT NULL, achievement_id text NOT NULL,
  unlocked_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (user_sub, achievement_id));
CREATE TABLE IF NOT EXISTS counter_outbox (
  id         uuid PRIMARY KEY,
  clicks     bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS counter_outbox_created_at_idx ON counter_outbox (created_at);
`

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
	if _, err := tx.Exec(ctx, Schema); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ApplyCounterScript idempotently applies a batch's clicks to counter:global,
// keyed by the batch's outbox id (its PoW challenge id, spec §6/§8). A diff
// between Postgres and Redis can never tell a lost increment from one merely
// in flight, so the counter is no longer healed by diffing: this script is
// safe to run any number of times for the same id — from the write path
// right after commit, or later from the outbox sweeper — because the
// "applied:<id>" marker only ever transitions once.
const ApplyCounterScript = `
if redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[2]) then
  return redis.call('INCRBY', KEYS[2], ARGV[1])
end
return redis.call('GET', KEYS[2])
`

// appliedMarkerTTLSeconds bounds the "applied:<id>" marker: long enough that
// the sweeper, which only looks at rows outside the in-flight window, can
// never re-apply an already-applied batch.
const appliedMarkerTTLSeconds = 86400 // 24h

// Scripter is the subset of the Redis client ApplyCounter needs.
type Scripter interface {
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
}

// ApplyCounter runs ApplyCounterScript for outbox row id against
// counter:global.
func ApplyCounter(ctx context.Context, rdb Scripter, id string, clicks int64) error {
	return rdb.Eval(ctx, ApplyCounterScript,
		[]string{"applied:" + id, "counter:global"}, clicks, appliedMarkerTTLSeconds).Err()
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
