// Package clicks implements the accepted-submit hot path (spec §4): one
// atomic Redis script does burn -> throttle -> counter increments -> event
// hand-off. Postgres is not on this path; the publisher's persist worker
// mirrors Redis to Postgres asynchronously.
package clicks

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/the-algovn/the-button-service/internal/pow"
)

// Result is the outcome of an accepted batch.
type Result struct {
	UserTotal uint64
}

// Submit runs the hot-path script for an already-verified challenge payload.
// Returned errors are gRPC status errors:
//   - AlreadyExists     — challenge replay (burn key present)
//   - ResourceExhausted — per-user min-interval hit (token un-burned, stays valid)
//   - Unavailable       — Redis unreachable (clicks fail closed)
func Submit(ctx context.Context, rdb redis.Scripter, p pow.Payload, count uint32, now time.Time, displayName string) (*Result, error) {
	res, err := runHotPath(ctx, rdb, p, count, now, displayName)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "redis unavailable")
	}
	switch res.Status {
	case "replay":
		return nil, status.Error(codes.AlreadyExists, "challenge already redeemed")
	case "throttled":
		return nil, status.Error(codes.ResourceExhausted, "min interval not elapsed")
	case "ok":
		return &Result{UserTotal: res.UserTotal}, nil
	default:
		return nil, status.Error(codes.Internal, "unexpected hot-path status")
	}
}
