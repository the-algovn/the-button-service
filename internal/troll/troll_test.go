package troll

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func has(us []Unlock, id string) bool {
	for _, u := range us {
		if u.ID == id {
			return true
		}
	}
	return false
}

func TestEvaluate_Numbers(t *testing.T) {
	noon := time.Date(2026, 7, 19, 5, 0, 0, 0, time.UTC) // HCM 12:00, no clock trigger
	require.True(t, has(Evaluate(12321, noon, false), "palindrome"))
	require.False(t, has(Evaluate(12345, noon, false), "palindrome"))
	require.True(t, has(Evaluate(1024, noon, false), "power_of_two"))
	require.False(t, has(Evaluate(1000, noon, false), "power_of_two"))
	require.False(t, has(Evaluate(512, noon, false), "power_of_two")) // below the 1024 floor
	require.True(t, has(Evaluate(5555, noon, false), "repdigit"))
	require.False(t, has(Evaluate(5556, noon, false), "repdigit"))
}

func TestEvaluate_Clock420(t *testing.T) {
	// 21:20 UTC == 04:20 HCM (UTC+7)
	at420 := time.Date(2026, 7, 18, 21, 20, 0, 0, time.UTC)
	require.True(t, has(Evaluate(50, at420, false), "clock_420"))
	at421 := time.Date(2026, 7, 18, 21, 21, 0, 0, time.UTC)
	require.False(t, has(Evaluate(50, at421, false), "clock_420"))
}

func TestEvaluate_MilestoneSecond(t *testing.T) {
	noon := time.Date(2026, 7, 19, 5, 0, 0, 0, time.UTC)
	require.True(t, has(Evaluate(1_000_000, noon, true), "milestone_second"))
	require.False(t, has(Evaluate(1_000_000, noon, false), "milestone_second"))
}
