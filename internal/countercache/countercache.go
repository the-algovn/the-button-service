// Package countercache keeps a per-replica cache of the public counter and
// distinct-contributor count, polled straight from Redis — the only
// counter truth since the outbox removal (see the 2026-07-17 api-publisher
// split spec). It satisfies server.Totaler.
package countercache

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/the-algovn/the-button-service/internal/leaderboard"
)

const (
	totalInterval = time.Second
	usersInterval = 15 * time.Second
	callTimeout   = 3 * time.Second
)

type Cache struct {
	RDB    *redis.Client
	Logger *slog.Logger

	total     atomic.Uint64
	haveTotal atomic.Bool
	users     atomic.Uint64
	haveUsers atomic.Bool
}

// Total returns the cached global counter and whether a value has been
// loaded yet.
func (c *Cache) Total() (uint64, bool) { return c.total.Load(), c.haveTotal.Load() }

// Users returns the cached distinct-contributor count and whether one has
// been loaded yet. Display-only; never load-bearing for accounting.
func (c *Cache) Users() (uint64, bool) { return c.users.Load(), c.haveUsers.Load() }

// Run starts both refresh loops and blocks until ctx is done.
func (c *Cache) Run(ctx context.Context) {
	go c.usersLoop(ctx)
	c.totalLoop(ctx)
}

func (c *Cache) totalLoop(ctx context.Context) {
	tick := time.NewTicker(totalInterval)
	defer tick.Stop()
	for {
		c.refreshTotal(ctx)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (c *Cache) refreshTotal(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	v, err := c.RDB.Get(cctx, "counter:total").Uint64()
	if errors.Is(err, redis.Nil) {
		v = 0
	} else if err != nil {
		if ctx.Err() == nil {
			c.Logger.Warn("counter cache refresh failed", "err", err)
		}
		return
	}
	c.total.Store(v)
	c.haveTotal.Store(true)
}

func (c *Cache) usersLoop(ctx context.Context) {
	tick := time.NewTicker(usersInterval)
	defer tick.Stop()
	for {
		c.refreshUsers(ctx)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (c *Cache) refreshUsers(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	n, err := c.RDB.ZCard(cctx, leaderboard.AllTimeKey).Result()
	if err != nil {
		if ctx.Err() == nil {
			c.Logger.Warn("user count refresh failed", "err", err)
		}
		return
	}
	c.users.Store(uint64(n))
	c.haveUsers.Store(true)
}
