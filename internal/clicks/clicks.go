// Package clicks is the pure-ack hot path: verify already happened in the
// server; here we throttle (Redis) and produce the click event to Kafka. No
// counter/board mutation — the worker groups own that.
package clicks

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/the-algovn/the-button-service/internal/clickevent"
	"github.com/the-algovn/the-button-service/internal/kafka"
	"github.com/the-algovn/the-button-service/internal/pow"
)

// Submit throttles the sub in Redis and, if allowed, produces the click event
// to Kafka. Returned errors are gRPC status errors:
//   - ResourceExhausted — per-user min-interval hit
//   - Unavailable       — Redis or Kafka produce failure
func Submit(ctx context.Context, rdb redis.Cmdable, prod *kgo.Client, p pow.Payload, count uint32, now time.Time, displayName string) error {
	ok, err := rdb.SetNX(ctx, "throttle:"+p.Sub, "1", time.Duration(p.MinIntervalS)*time.Second).Result()
	if err != nil {
		return status.Error(codes.Unavailable, "redis unavailable")
	}
	if !ok {
		return status.Error(codes.ResourceExhausted, "min interval not elapsed")
	}
	ev := clickevent.Click{Sub: p.Sub, Count: count, ChallengeID: p.ID, TsUnix: now.Unix(), DisplayName: displayName}
	val, err := ev.Marshal()
	if err != nil {
		return status.Error(codes.Internal, "marshal click")
	}
	if perr := kafka.Produce(ctx, prod, kafka.TopicClicks, ev.Key(), val); perr != nil {
		// Best-effort un-throttle so the client's retry isn't locked out.
		_ = rdb.Del(ctx, "throttle:"+p.Sub).Err()
		return status.Error(codes.Unavailable, "enqueue failed")
	}
	return nil
}
