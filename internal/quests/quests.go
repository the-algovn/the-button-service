// Package quests holds the daily/weekly quest pools, deterministic per-day/week
// selection (seeded by the Asia/Ho_Chi_Minh date so everyone shares the set),
// and pure progress evaluation from server-observable signals only. It knows
// nothing about Redis or the proto — the progress worker gathers Signals and
// maps Def+progress to the wire type.
package quests

import "hash/fnv"

type Kind int

const (
	Daily Kind = iota
	Weekly
)

// Metric selects which server-observable signal a quest tracks. Never combo.
type Metric int

const (
	MetricClicksToday Metric = iota
	MetricBatchesToday
	MetricMaxBatchToday
	MetricDaysThisWeek
	MetricClicksThisWeek
	MetricWeeklyRankAtMost // done when 0 < WeeklyRank <= Target
)

type Def struct {
	ID          string
	Title       string
	Description string
	Kind        Kind
	Metric      Metric
	Target      uint64
	Reward      string
}

// Signals are the server-observable facts the progress worker gathers for the
// caller's current HCM day/week.
type Signals struct {
	ClicksToday    uint64
	BatchesToday   uint64
	MaxBatchToday  uint64
	DaysThisWeek   uint64
	ClicksThisWeek uint64
	WeeklyRank     uint32 // 0 = unranked
}

var dailyPool = []Def{
	{"warmup", "Warm-up", "Click 100 times today.", Daily, MetricClicksToday, 100, "+50 XP"},
	{"marathon", "Marathon", "Click 1,000 times today.", Daily, MetricClicksToday, 1_000, "+200 XP"},
	{"grinder", "The Grinder", "Click 5,000 times today. Your wrist filed a complaint.", Daily, MetricClicksToday, 5_000, "+500 XP"},
	{"assembly", "Assembly Line", "Submit 10 batches today.", Daily, MetricBatchesToday, 10, "+100 XP"},
	{"go_big", "Go Big", "Land a single 500-click batch today.", Daily, MetricMaxBatchToday, 500, "+150 XP"},
	{"overachiever", "Overachiever", "Submit 50 batches today. Touch grass afterward.", Daily, MetricBatchesToday, 50, "+300 XP"},
}

var weeklyPool = []Def{
	{"everyday", "Every Day This Week", "Contribute on all 7 days.", Weekly, MetricDaysThisWeek, 7, "+1000 XP"},
	{"weekly_grind", "Weekly Grind", "Click 50,000 times this week.", Weekly, MetricClicksThisWeek, 50_000, "+800 XP"},
	{"top100", "Crack the Top 100", "Reach the weekly top 100.", Weekly, MetricWeeklyRankAtMost, 100, "badge"},
}

// DailyQuests returns 3 deterministic daily quests for the HCM date.
func DailyQuests(dateHCM string) []Def { return pick(dailyPool, dateHCM, 3) }

// WeeklyQuests returns 2 deterministic weekly quests for the HCM week start.
func WeeklyQuests(weekStartHCM string) []Def { return pick(weeklyPool, weekStartHCM, 2) }

// pick selects n consecutive (wrapping) entries starting at a seed-hashed
// offset. Same seed -> same set; different seeds rotate. Stable across runs and
// Go versions (no math/rand internals).
func pick(pool []Def, seed string, n int) []Def {
	if n > len(pool) {
		n = len(pool)
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(seed))
	start := int(h.Sum64() % uint64(len(pool)))
	out := make([]Def, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, pool[(start+i)%len(pool)])
	}
	return out
}

// Progress returns the caller's progress toward a quest and whether it's done.
// Count metrics cap progress at Target; the rank metric is done when the caller
// is ranked at or better than Target.
func Progress(d Def, s Signals) (uint64, bool) {
	if d.Metric == MetricWeeklyRankAtMost {
		done := s.WeeklyRank > 0 && uint64(s.WeeklyRank) <= d.Target
		if done {
			return d.Target, true
		}
		return 0, false
	}
	v := s.value(d.Metric)
	if v >= d.Target {
		return d.Target, true
	}
	return v, false
}

func (s Signals) value(m Metric) uint64 {
	switch m {
	case MetricClicksToday:
		return s.ClicksToday
	case MetricBatchesToday:
		return s.BatchesToday
	case MetricMaxBatchToday:
		return s.MaxBatchToday
	case MetricDaysThisWeek:
		return s.DaysThisWeek
	case MetricClicksThisWeek:
		return s.ClicksThisWeek
	default:
		return 0
	}
}
