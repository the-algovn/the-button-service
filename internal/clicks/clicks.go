// Package clicks implements the SubmitClicks core flow (spec §6 steps 2-4):
// burn → throttle → durable txn → compensation → counter bump.
package clicks

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/the-algovn/the-button-service/internal/achievements"
	"github.com/the-algovn/the-button-service/internal/pow"
)

// Rediser is the slice of go-redis used by Submit (satisfied by *redis.Client).
type Rediser interface {
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	IncrBy(ctx context.Context, key string, value int64) *redis.IntCmd
	Incr(ctx context.Context, key string) *redis.IntCmd
}

// Unlock is a newly earned achievement with its database timestamp.
type Unlock struct {
	Achievement achievements.Achievement
	UnlockedAt  time.Time
}

// Result is the outcome of an accepted batch.
type Result struct {
	UserTotal uint64
	Unlocked  []Unlock
}

// Submit executes spec §6 steps 2-4 for an already-verified challenge
// payload. Returned errors are gRPC status errors:
//   - AlreadyExists     — challenge replay (burn key present)
//   - ResourceExhausted — per-user min-interval hit (token un-burned, stays valid)
//   - Unavailable       — Redis or Postgres unreachable (clicks fail closed)
func Submit(ctx context.Context, rdb Rediser, pool *pgxpool.Pool, logger *slog.Logger, p pow.Payload, count uint32, now time.Time) (*Result, error) {
	powKey := "pow:" + p.ID
	throttleKey := "throttle:" + p.Sub
	bg := context.WithoutCancel(ctx) // compensation must survive deadline expiry

	// Step 2a: burn the challenge. Two sequential commands — the throttle
	// branches on the burn result, so no pipelining.
	burned, err := rdb.SetNX(ctx, powKey, 1, pow.BurnTTL).Result()
	if err != nil {
		return nil, status.Error(codes.Unavailable, "redis unavailable")
	}
	if !burned {
		return nil, status.Error(codes.AlreadyExists, "challenge already redeemed")
	}

	// Step 2b: hard per-user rate floor.
	ok, err := rdb.SetNX(ctx, throttleKey, 1, time.Duration(p.MinIntervalS)*time.Second).Result()
	if err != nil {
		if derr := rdb.Del(bg, powKey).Err(); derr != nil {
			logger.Warn("un-burn DEL failed", "err", derr)
		}
		return nil, status.Error(codes.Unavailable, "redis unavailable")
	}
	if !ok {
		// un-burn: the token stays valid, the client backs off
		if derr := rdb.Del(bg, powKey).Err(); derr != nil {
			logger.Warn("un-burn DEL failed", "err", derr)
		}
		return nil, status.Error(codes.ResourceExhausted, "min interval not elapsed")
	}

	// Step 3: durable personal truth.
	res, err := applyBatch(ctx, pool, p.Sub, count, now)
	if err != nil {
		logger.Warn("batch txn failed", "sub", p.Sub, "err", err)
		// best-effort compensation; if this DEL fails the client re-solves
		// one challenge — accepted (spec §13)
		if derr := rdb.Del(bg, powKey, throttleKey).Err(); derr != nil {
			logger.Warn("compensation DEL failed", "err", derr)
		}
		return nil, status.Error(codes.Unavailable, "postgres unavailable")
	}

	// Step 4: hot counter + controller signal — drift healed by reconcile.
	if err := rdb.IncrBy(bg, "counter:global", int64(count)).Err(); err != nil {
		logger.Warn("counter INCRBY failed", "err", err)
	}
	if err := rdb.Incr(bg, "stats:accepted_total").Err(); err != nil {
		logger.Warn("stats INCR failed", "err", err)
	}
	return res, nil
}

func applyBatch(ctx context.Context, pool *pgxpool.Pool, sub string, count uint32, now time.Time) (*Result, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck // no-op after commit

	var total int64
	err = tx.QueryRow(ctx,
		`INSERT INTO user_clicks AS u (user_sub, clicks) VALUES ($1, $2)
		 ON CONFLICT (user_sub) DO UPDATE SET clicks = u.clicks + $2
		 RETURNING clicks`, sub, int64(count)).Scan(&total)
	if err != nil {
		return nil, err
	}

	res := &Result{UserTotal: uint64(total)}
	for _, a := range achievements.Evaluate(uint64(total), count, now) {
		var at time.Time
		err := tx.QueryRow(ctx,
			`INSERT INTO user_achievements (user_sub, achievement_id) VALUES ($1, $2)
			 ON CONFLICT DO NOTHING RETURNING unlocked_at`, sub, a.ID).Scan(&at)
		if errors.Is(err, pgx.ErrNoRows) {
			continue // unlocked in an earlier batch
		}
		if err != nil {
			return nil, err
		}
		res.Unlocked = append(res.Unlocked, Unlock{Achievement: a, UnlockedAt: at})
	}
	return res, tx.Commit(ctx)
}
