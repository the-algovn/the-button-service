// Package clicks implements the SubmitClicks core flow (spec §6 steps 2-4):
// burn → throttle → durable txn → compensation → counter bump.
package clicks

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/the-algovn/the-button-service/internal/achievements"
	"github.com/the-algovn/the-button-service/internal/pow"
	"github.com/the-algovn/the-button-service/internal/store"
)

// Rediser is the slice of go-redis used by Submit (satisfied by *redis.Client).
type Rediser interface {
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Incr(ctx context.Context, key string) *redis.IntCmd
	redis.Scripter
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
	res, err := applyBatch(ctx, pool, p.ID, p.Sub, count, now)
	if err != nil {
		logger.Warn("batch txn failed", "sub", p.Sub, "err", err)
		// Pre-commit failures rolled back cleanly — release the burn and the
		// throttle so the client can retry the same token immediately.
		// An ambiguous commit may have landed: keep the burn (the token is
		// spent) so a retry cannot double-credit the batch. The client
		// re-issues a challenge; at worst one PoW solve is wasted.
		if errors.Is(err, errCommitAmbiguous) {
			logger.Warn("commit ambiguous; keeping PoW burn to prevent double-credit", "sub", p.Sub)
		} else if derr := rdb.Del(bg, powKey, throttleKey).Err(); derr != nil {
			// best-effort compensation; if this DEL fails the client re-solves
			// one challenge — accepted (spec §13)
			logger.Warn("compensation DEL failed", "err", derr)
		}
		return nil, status.Error(codes.Unavailable, "postgres unavailable")
	}

	// Step 4: idempotent counter apply + controller signal. The outbox row
	// inserted inside the txn above lets the sweeper (internal/ticker) apply
	// this batch later if the process crashes or Redis blips before this
	// call — a diff between Postgres and Redis can never distinguish a lost
	// apply from one merely in flight, so the counter is no longer healed by
	// diffing (spec §6/§8).
	if err := store.ApplyCounter(bg, rdb, p.ID, int64(count)); err != nil {
		if errors.Is(err, store.ErrCounterNotSeeded) {
			// counter:global doesn't exist yet (Redis lost its data and the
			// tick leader hasn't re-seeded from Postgres). The batch is
			// already committed and correct — this is not a failed click.
			// Leave the outbox row in place; the sweeper applies it once
			// the seed lands.
			logger.Info("counter apply deferred: counter not seeded yet", "id", p.ID)
		} else {
			logger.Warn("counter apply failed", "err", err)
		}
	} else if _, err := pool.Exec(bg, `DELETE FROM counter_outbox WHERE id = $1`, p.ID); err != nil {
		// best-effort: if this fails the sweeper will re-apply idempotently
		// (a no-op, since applied:<id> is already set) and delete later
		logger.Warn("outbox delete failed", "id", p.ID, "err", err)
	}
	if err := rdb.Incr(bg, "stats:accepted_total").Err(); err != nil {
		logger.Warn("stats INCR failed", "err", err)
	}
	return res, nil
}

// errCommitAmbiguous marks a commit whose outcome is unknown (deadline expiry,
// connection drop): Postgres may have durably committed the batch. The PoW burn
// must NOT be released in that case — replaying the token would credit the same
// clicks twice, and there is no batch-level idempotency key to catch it.
var errCommitAmbiguous = errors.New("commit outcome ambiguous")

func applyBatch(ctx context.Context, pool *pgxpool.Pool, id, sub string, count uint32, now time.Time) (*Result, error) {
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

	// Outbox row in the SAME txn as the upsert: an ambiguous-but-landed
	// commit (below) leaves this row for the sweeper to apply, closing the
	// under-count that an ambiguous commit would otherwise leave.
	if _, err := tx.Exec(ctx,
		`INSERT INTO counter_outbox (id, clicks) VALUES ($1, $2)`, id, int64(count)); err != nil {
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
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("%w: %w", errCommitAmbiguous, err)
	}
	return res, nil
}
