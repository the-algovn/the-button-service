package leaderboard

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWeekStart_ICTMonday(t *testing.T) {
	hcm, err := time.LoadLocation("Asia/Ho_Chi_Minh")
	require.NoError(t, err)

	// Wed 2026-07-15 10:00 ICT -> week's Monday is 2026-07-13.
	wed := time.Date(2026, 7, 15, 10, 0, 0, 0, hcm)
	require.Equal(t, "2026-07-13", WeekStartString(wed))

	// Sun 2026-07-19 23:59 ICT is still the 2026-07-13 week (Mon-anchored).
	sun := time.Date(2026, 7, 19, 23, 59, 0, 0, hcm)
	require.Equal(t, "2026-07-13", WeekStartString(sun))

	// Mon 2026-07-20 00:00 ICT rolls to the new week.
	mon := time.Date(2026, 7, 20, 0, 0, 0, 0, hcm)
	require.Equal(t, "2026-07-20", WeekStartString(mon))

	// Boundary is ICT, not UTC: Sun 2026-07-19 18:00 UTC == Mon 01:00 ICT ->
	// already the new (2026-07-20) week.
	utcLateSun := time.Date(2026, 7, 19, 18, 0, 0, 0, time.UTC)
	require.Equal(t, "2026-07-20", WeekStartString(utcLateSun))
}

func TestWeekKey(t *testing.T) {
	hcm, _ := time.LoadLocation("Asia/Ho_Chi_Minh")
	require.Equal(t, "lb:week:2026-07-13", WeekKey(time.Date(2026, 7, 15, 10, 0, 0, 0, hcm)))
	require.Equal(t, "lb:alltime", AllTimeKey)
}
