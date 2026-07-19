package streak

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAdvance(t *testing.T) {
	// first ever contribution
	s, adv := Advance(State{}, "2026-07-19")
	require.True(t, adv)
	require.Equal(t, State{Count: 1, Best: 1, LastDay: "2026-07-19"}, s)

	// same day again: no-op
	s2, adv := Advance(s, "2026-07-19")
	require.False(t, adv)
	require.Equal(t, s, s2)

	// next day: increment, best follows
	s3, adv := Advance(s, "2026-07-20")
	require.True(t, adv)
	require.Equal(t, State{Count: 2, Best: 2, LastDay: "2026-07-20"}, s3)

	// next day that does NOT beat an existing best
	s4, adv := Advance(State{Count: 3, Best: 9, LastDay: "2026-07-20"}, "2026-07-21")
	require.True(t, adv)
	require.Equal(t, State{Count: 4, Best: 9, LastDay: "2026-07-21"}, s4)

	// a skipped day resets the count but keeps best
	s5, adv := Advance(State{Count: 5, Best: 9, LastDay: "2026-07-19"}, "2026-07-21")
	require.True(t, adv)
	require.Equal(t, State{Count: 1, Best: 9, LastDay: "2026-07-21"}, s5)

	// month boundary is still "next day"
	s6, adv := Advance(State{Count: 2, Best: 2, LastDay: "2026-07-31"}, "2026-08-01")
	require.True(t, adv)
	require.Equal(t, State{Count: 3, Best: 3, LastDay: "2026-08-01"}, s6)
}
