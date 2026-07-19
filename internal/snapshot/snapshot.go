// Package snapshot periodically persists the Redis-authoritative game state to
// Postgres (a durable backup, not a write-behind log) and restores it into Redis
// on a cold start. Redis stays authoritative; Postgres only guards against a Redis
// loss zeroing the global counter, leaderboards, unlocks, and streaks.
package snapshot

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/the-algovn/the-button-service/internal/db"
	"github.com/the-algovn/the-button-service/internal/leaderboard"
)

const snapInterval = 30 * time.Second

type Snapshotter struct {
	Pool   *pgxpool.Pool
	RDB    *redis.Client
	Logger *slog.Logger
}

// Run snapshots every snapInterval until ctx is done.
func (s *Snapshotter) Run(ctx context.Context) error {
	tk := time.NewTicker(snapInterval)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tk.C:
			if err := s.Snapshot(ctx); err != nil {
				s.Logger.Warn("snapshot", "err", err)
			}
		}
	}
}

// Snapshot writes the current Redis state to Postgres. Idempotent (absolute
// upserts + first-write-wins unlock times), so re-running never double-counts.
func (s *Snapshotter) Snapshot(ctx context.Context) error {
	q := db.New(s.Pool)
	now := time.Now()

	if total, err := s.RDB.Get(ctx, "counter:total").Int64(); err == nil {
		if err := q.UpsertCounterState(ctx, total); err != nil {
			return err
		}
	} else if !errors.Is(err, redis.Nil) {
		return err
	}

	allSubs, allClicks, err := zsetToSlices(ctx, s.RDB, leaderboard.AllTimeKey)
	if err != nil {
		return err
	}
	if len(allSubs) > 0 {
		if err := q.BatchUpsertUserClicks(ctx, db.BatchUpsertUserClicksParams{Subs: allSubs, Clicks: allClicks}); err != nil {
			return err
		}
	}

	weekSubs, weekClicks, err := zsetToSlices(ctx, s.RDB, leaderboard.WeekKey(now))
	if err != nil {
		return err
	}
	if len(weekSubs) > 0 {
		if err := q.BatchUpsertUserWeeklyClicks(ctx, db.BatchUpsertUserWeeklyClicksParams{
			Subs: weekSubs, WeekStart: pgtype.Date{Time: leaderboard.WeekStart(now), Valid: true}, Clicks: weekClicks,
		}); err != nil {
			return err
		}
	}

	if names, err := s.RDB.HGetAll(ctx, "profile:names").Result(); err == nil && len(names) > 0 {
		subs := make([]string, 0, len(names))
		disp := make([]string, 0, len(names))
		for k, v := range names {
			subs = append(subs, k)
			disp = append(disp, v)
		}
		if err := q.BatchUpsertUserProfile(ctx, db.BatchUpsertUserProfileParams{Subs: subs, Names: disp}); err != nil {
			return err
		}
	}

	if err := s.snapshotUnlocks(ctx, q, allSubs); err != nil {
		return err
	}
	return s.snapshotStreaks(ctx, q, allSubs)
}

func (s *Snapshotter) snapshotUnlocks(ctx context.Context, q *db.Queries, subs []string) error {
	if len(subs) == 0 {
		return nil
	}
	pipe := s.RDB.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(subs))
	for i, sub := range subs {
		cmds[i] = pipe.HGetAll(ctx, "unlocks:"+sub)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	var uSubs, uIDs []string
	var uAt []time.Time
	for i, sub := range subs {
		m, err := cmds[i].Result()
		if err != nil {
			continue
		}
		for id, ts := range m {
			sec, _ := strconv.ParseInt(ts, 10, 64)
			uSubs = append(uSubs, sub)
			uIDs = append(uIDs, id)
			uAt = append(uAt, time.Unix(sec, 0))
		}
	}
	if len(uSubs) == 0 {
		return nil
	}
	return q.BatchUpsertUserAchievements(ctx, db.BatchUpsertUserAchievementsParams{Subs: uSubs, Ids: uIDs, UnlockedAts: uAt})
}

func (s *Snapshotter) snapshotStreaks(ctx context.Context, q *db.Queries, subs []string) error {
	if len(subs) == 0 {
		return nil
	}
	pipe := s.RDB.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(subs))
	for i, sub := range subs {
		cmds[i] = pipe.HGetAll(ctx, "streak:"+sub)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	var sSubs, lastDays []string
	var cur, best []int32
	for i, sub := range subs {
		m, err := cmds[i].Result()
		if err != nil || len(m) == 0 {
			continue
		}
		c, _ := strconv.ParseInt(m["count"], 10, 32)
		b, _ := strconv.ParseInt(m["best"], 10, 32)
		sSubs = append(sSubs, sub)
		cur = append(cur, int32(c))
		best = append(best, int32(b))
		lastDays = append(lastDays, m["lastday"])
	}
	if len(sSubs) == 0 {
		return nil
	}
	return q.BatchUpsertUserStreak(ctx, db.BatchUpsertUserStreakParams{Subs: sSubs, CurDays: cur, BestDays: best, LastDays: lastDays})
}

// Restore loads the latest Postgres snapshot back into Redis, but ONLY when Redis
// is empty (no counter and no all-time board). A non-empty Redis is authoritative
// and left untouched.
func (s *Snapshotter) Restore(ctx context.Context) error {
	exists, err := s.RDB.Exists(ctx, "counter:total").Result()
	if err != nil {
		return err
	}
	card, err := s.RDB.ZCard(ctx, leaderboard.AllTimeKey).Result()
	if err != nil {
		return err
	}
	if exists > 0 || card > 0 {
		s.Logger.Info("snapshot restore skipped: redis not empty")
		return nil
	}
	q := db.New(s.Pool)
	now := time.Now()

	if total, err := q.GetCounterState(ctx); err == nil {
		if err := s.RDB.Set(ctx, "counter:total", total, 0).Err(); err != nil {
			return err
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}

	rows, err := q.ListAllUserClicks(ctx)
	if err != nil {
		return err
	}
	if len(rows) > 0 {
		zs := make([]redis.Z, len(rows))
		for i, r := range rows {
			zs[i] = redis.Z{Member: r.UserSub, Score: float64(r.Clicks)}
		}
		if err := s.RDB.ZAdd(ctx, leaderboard.AllTimeKey, zs...).Err(); err != nil {
			return err
		}
	}

	week := pgtype.Date{Time: leaderboard.WeekStart(now), Valid: true}
	wrows, err := q.ListWeekUserClicks(ctx, week)
	if err != nil {
		return err
	}
	if len(wrows) > 0 {
		weekKey := leaderboard.WeekKey(now)
		zs := make([]redis.Z, len(wrows))
		for i, r := range wrows {
			zs[i] = redis.Z{Member: r.UserSub, Score: float64(r.Clicks)}
		}
		if err := s.RDB.ZAdd(ctx, weekKey, zs...).Err(); err != nil {
			return err
		}
		s.RDB.Expire(ctx, weekKey, leaderboard.WeekTTL)
	}

	profs, err := q.ListAllProfiles(ctx)
	if err != nil {
		return err
	}
	if len(profs) > 0 {
		vals := make([]any, 0, len(profs)*2)
		for _, p := range profs {
			vals = append(vals, p.UserSub, p.DisplayName)
		}
		if err := s.RDB.HSet(ctx, "profile:names", vals...).Err(); err != nil {
			return err
		}
	}

	achs, err := q.ListAllUserAchievements(ctx)
	if err != nil {
		return err
	}
	if len(achs) > 0 {
		pipe := s.RDB.Pipeline()
		for _, a := range achs {
			pipe.HSetNX(ctx, "unlocks:"+a.UserSub, a.AchievementID, a.UnlockedAt.Unix())
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
	}

	streaks, err := q.ListAllUserStreak(ctx)
	if err != nil {
		return err
	}
	if len(streaks) > 0 {
		pipe := s.RDB.Pipeline()
		for _, st := range streaks {
			pipe.HSet(ctx, "streak:"+st.UserSub, "count", st.CurDays, "best", st.BestDays, "lastday", st.LastDay)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
	}
	return nil
}

func zsetToSlices(ctx context.Context, rdb *redis.Client, key string) ([]string, []int64, error) {
	zs, err := rdb.ZRangeWithScores(ctx, key, 0, -1).Result()
	if err != nil {
		return nil, nil, err
	}
	subs := make([]string, len(zs))
	clicks := make([]int64, len(zs))
	for i, z := range zs {
		subs[i], _ = z.Member.(string)
		clicks[i] = int64(z.Score)
	}
	return subs, clicks, nil
}
