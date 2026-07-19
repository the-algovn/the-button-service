// Package idem guards worker effects against Kafka's at-least-once delivery
// and client retries: the first time a (group, challengeID) is seen the effect
// runs; repeats no-op. Each consumer group tracks its own seen-set, so the two
// worker groups apply the same event independently exactly once each.
package idem

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// FirstSee reports whether (group, challengeID) is being seen for the first
// time, using SET NX seen:<group>:<challengeID> EX ttl. It returns true the
// first time and false on repeats, so callers can gate a side effect on the
// result.
func FirstSee(ctx context.Context, rdb redis.Cmdable, group, challengeID string, ttl time.Duration) (bool, error) {
	return rdb.SetNX(ctx, "seen:"+group+":"+challengeID, "1", ttl).Result()
}
