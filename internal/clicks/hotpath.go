package clicks

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/the-algovn/the-button-service/internal/leaderboard"
	"github.com/the-algovn/the-button-service/internal/pow"
)

// hotPathScript is the entire Redis interaction of an accepted submit, run
// atomically in one round-trip. Burn + throttle + the counter increments are
// one operation, so there is no ambiguous-commit window: either the whole
// batch applied exactly once, or it did not apply at all.
//
// KEYS: 1 powKey  2 throttleKey  3 lb:alltime  4 weekKey  5 counter:total
//       6 profile:names  7 clicks:events  8 stats:accepted_total
// ARGV: 1 count  2 minIntervalS  3 weekTTLs  4 sub  5 burnTTLs  6 displayName  7 tsUnix
var hotPathScript = redis.NewScript(`
local count = tonumber(ARGV[1])
if redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[5]) == false then
  return {'replay'}
end
if redis.call('SET', KEYS[2], '1', 'NX', 'EX', ARGV[2]) == false then
  redis.call('DEL', KEYS[1])
  return {'throttled'}
end
local userTotal = redis.call('ZINCRBY', KEYS[3], count, ARGV[4])
redis.call('ZINCRBY', KEYS[4], count, ARGV[4])
redis.call('EXPIRE', KEYS[4], ARGV[3])
redis.call('INCRBY', KEYS[5], count)
redis.call('HSET', KEYS[6], ARGV[4], ARGV[6])
redis.call('XADD', KEYS[7], '*', 'sub', ARGV[4], 'count', ARGV[1], 'total', userTotal, 'ts', ARGV[7])
redis.call('INCR', KEYS[8])
return {'ok', tonumber(userTotal)}
`)

// HotResult is the parsed script reply. Status is "ok", "replay", or "throttled".
type HotResult struct {
	Status    string
	UserTotal uint64
}

func runHotPath(ctx context.Context, rdb redis.Scripter, p pow.Payload, count uint32, now time.Time, displayName string) (HotResult, error) {
	keys := []string{
		"pow:" + p.ID,
		"throttle:" + p.Sub,
		leaderboard.AllTimeKey,
		leaderboard.WeekKey(now),
		"counter:total",
		"profile:names",
		"clicks:events",
		"stats:accepted_total",
	}
	args := []any{
		count,
		p.MinIntervalS,
		int64(leaderboard.WeekTTL.Seconds()),
		p.Sub,
		int64(pow.BurnTTL.Seconds()),
		displayName,
		now.Unix(),
	}
	raw, err := hotPathScript.Run(ctx, rdb, keys, args...).Result()
	if err != nil {
		return HotResult{}, err
	}
	arr, ok := raw.([]any)
	if !ok || len(arr) == 0 {
		return HotResult{}, fmt.Errorf("hot-path: unexpected reply %v", raw)
	}
	status, _ := arr[0].(string)
	switch status {
	case "replay", "throttled":
		return HotResult{Status: status}, nil
	case "ok":
		total, _ := arr[1].(int64)
		return HotResult{Status: "ok", UserTotal: uint64(total)}, nil
	default:
		return HotResult{}, fmt.Errorf("hot-path: unknown status %q", status)
	}
}
