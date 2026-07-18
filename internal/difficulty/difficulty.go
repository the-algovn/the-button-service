// Package difficulty keeps a per-replica cache of the shared PoW difficulty
// keys (pow:L, pow:min_interval). The publisher moves them at most once per
// 30s, so IssueChallenge / the piggybacked next-challenge read them from here
// instead of hitting Redis on every submit.
package difficulty

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	refreshInterval = time.Second
	callTimeout     = 2 * time.Second
)

type Cache struct {
	RDB    *redis.Client
	Logger *slog.Logger

	l           atomic.Uint64
	minInterval atomic.Uint64
	have        atomic.Bool
}

// Get returns the cached difficulty and whether a value has loaded yet.
func (c *Cache) Get() (l, minInterval uint64, ok bool) {
	return c.l.Load(), c.minInterval.Load(), c.have.Load()
}

// Run refreshes the cache every second and blocks until ctx is done.
func (c *Cache) Run(ctx context.Context) {
	tick := time.NewTicker(refreshInterval)
	defer tick.Stop()
	for {
		c.refresh(ctx)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (c *Cache) refresh(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	l, err := c.RDB.Get(cctx, "pow:L").Uint64()
	if err != nil {
		if ctx.Err() == nil {
			c.Logger.Warn("difficulty refresh: pow:L", "err", err)
		}
		return
	}
	mi, err := c.RDB.Get(cctx, "pow:min_interval").Uint64()
	if err != nil {
		if ctx.Err() == nil {
			c.Logger.Warn("difficulty refresh: pow:min_interval", "err", err)
		}
		return
	}
	c.l.Store(l)
	c.minInterval.Store(mi)
	c.have.Store(true)
}
