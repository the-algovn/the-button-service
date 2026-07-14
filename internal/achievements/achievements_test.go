package achievements

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func ids(as []Achievement) []string {
	out := make([]string, len(as))
	for i, a := range as {
		out[i] = a.ID
	}
	return out
}

// neutral is 22:00 ICT — triggers no time-of-day rule.
var neutral = time.Date(2026, 7, 14, 15, 0, 0, 0, time.UTC)

func TestEvaluate_ThresholdsUseCrosses(t *testing.T) {
	require.Equal(t, []string{"mvh"}, ids(Evaluate(1, 1, neutral)))
	// old=60 → only the 69 crossing fires; no re-award of mvh/ten
	require.Equal(t, []string{"nice"}, ids(Evaluate(70, 10, neutral)))
	// boundary: old=69 does NOT re-cross 69
	require.Empty(t, Evaluate(70, 1, neutral))
	// crossing includes the exact value: old=68, new=69
	require.Equal(t, []string{"nice"}, ids(Evaluate(69, 1, neutral)))
	// old=99,995 crosses only 100,000
	require.Equal(t, []string{"stretch"}, ids(Evaluate(100_000, 5, neutral)))
}

func TestEvaluate_BatchRules(t *testing.T) {
	require.Equal(t, []string{"bigbatch"}, ids(Evaluate(200_500, 500, neutral)))
	require.Equal(t, []string{"bigbatch", "maxbatch"}, ids(Evaluate(210_000, 10_000, neutral)))
	require.Empty(t, Evaluate(200_999, 499, neutral))
}

func TestEvaluate_TimeRulesHoChiMinh(t *testing.T) {
	// 20:30 UTC = 03:30 ICT (+07, no DST)
	night := time.Date(2026, 7, 13, 20, 30, 0, 0, time.UTC)
	require.Equal(t, []string{"night"}, ids(Evaluate(50, 1, night)))
	// 05:15 UTC = 12:15 ICT
	lunch := time.Date(2026, 7, 14, 5, 15, 0, 0, time.UTC)
	require.Equal(t, []string{"lunch"}, ids(Evaluate(50, 1, lunch)))
	// 03:30 UTC = 10:30 ICT — NOT night even though it is 3am UTC
	require.Empty(t, Evaluate(50, 1, time.Date(2026, 7, 14, 3, 30, 0, 0, time.UTC)))
}

func TestEvaluate_FreshWhale(t *testing.T) {
	// first-ever batch of 10,000: every threshold ≤10k, both crossings, both batch rules
	require.Equal(t,
		[]string{"mvh", "ten", "nice", "century", "blaze", "comma", "carpal", "bigbatch", "maxbatch"},
		ids(Evaluate(10_000, 10_000, neutral)))
}

func TestCatalogAndMilestones(t *testing.T) {
	require.Len(t, Catalog, 12)
	for _, a := range Catalog {
		require.NotEmpty(t, a.ID)
		require.NotEmpty(t, a.Title)
		require.NotEmpty(t, a.Description)
	}
	a, ok := ByID("nice")
	require.True(t, ok)
	require.Equal(t, "Nice.", a.Title)

	require.Len(t, Milestones, 5)
	require.Equal(t, uint64(1_000), Milestones[0].Threshold)
	require.Equal(t, uint64(1_000_000_000), Milestones[4].Threshold)
	for i := 1; i < len(Milestones); i++ {
		require.Greater(t, Milestones[i].Threshold, Milestones[i-1].Threshold)
	}
}
