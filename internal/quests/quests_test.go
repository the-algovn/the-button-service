package quests

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDailyQuests_DeterministicAndRotates(t *testing.T) {
	a := DailyQuests("2026-07-19")
	b := DailyQuests("2026-07-19")
	require.Len(t, a, 3)
	require.Equal(t, a, b, "same date -> same quests")
	for _, q := range a {
		require.Equal(t, Daily, q.Kind)
	}
	// A different date generally rotates the set (ids differ somewhere).
	c := DailyQuests("2026-07-20")
	require.NotEqual(t, ids(a), ids(c), "different date -> different set")
}

func TestWeeklyQuests_Deterministic(t *testing.T) {
	a := WeeklyQuests("2026-07-13")
	require.Len(t, a, 2)
	require.Equal(t, a, WeeklyQuests("2026-07-13"))
	for _, q := range a {
		require.Equal(t, Weekly, q.Kind)
	}
}

func TestProgress_CountMetric(t *testing.T) {
	d := Def{ID: "warmup", Kind: Daily, Metric: MetricClicksToday, Target: 100}
	p, done := Progress(d, Signals{ClicksToday: 40})
	require.EqualValues(t, 40, p)
	require.False(t, done)
	p, done = Progress(d, Signals{ClicksToday: 100})
	require.EqualValues(t, 100, p)
	require.True(t, done)
	// progress caps at target (does not exceed)
	p, _ = Progress(d, Signals{ClicksToday: 250})
	require.EqualValues(t, 100, p)
}

func TestProgress_RankMetric(t *testing.T) {
	d := Def{ID: "top100", Kind: Weekly, Metric: MetricWeeklyRankAtMost, Target: 100}
	_, done := Progress(d, Signals{WeeklyRank: 0}) // unranked
	require.False(t, done)
	_, done = Progress(d, Signals{WeeklyRank: 250}) // worse than 100
	require.False(t, done)
	_, done = Progress(d, Signals{WeeklyRank: 50}) // 50 <= 100 -> done
	require.True(t, done)
}

func ids(ds []Def) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = d.ID
	}
	return out
}
