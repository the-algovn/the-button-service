package pow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNextL(t *testing.T) {
	t0 := time.Unix(1_000_000, 0)
	after := t0.Add(SlewInterval) // slew window open

	tests := []struct {
		name      string
		l         uint32
		rate      float64
		now       time.Time
		wantL     uint32
		wantMoved bool
	}{
		{"in band no change", 4, 300, after, 4, false},
		{"raise above 440", 4, 441, after, 5, true},
		{"hysteresis: 400..440 holds", 4, 439, after, 4, false},
		{"lower below 140", 4, 139, after, 3, true},
		{"hysteresis: 140..200 holds", 4, 141, after, 4, false},
		{"clamp at MaxL", 16, 10_000, after, 16, false},
		{"clamp at MinL", 1, 0, after, 1, false},
		{"slew: too soon", 4, 10_000, t0.Add(29 * time.Second), 4, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotL, gotChange := NextL(tc.l, tc.rate, t0, tc.now)
			require.Equal(t, tc.wantL, gotL)
			if tc.wantMoved {
				require.Equal(t, tc.now, gotChange)
			} else {
				require.Equal(t, t0, gotChange)
			}
		})
	}
}

func TestNextL_SlewOneStepPer30s(t *testing.T) {
	t0 := time.Unix(1_000_000, 0)
	l, change := NextL(4, 10_000, t0.Add(-time.Hour), t0)
	require.EqualValues(t, 5, l)
	// 10s later, still storming: no second step yet
	l2, _ := NextL(l, 10_000, change, t0.Add(10*time.Second))
	require.EqualValues(t, 5, l2)
	// 30s after the change the next step is allowed
	l3, _ := NextL(l, 10_000, change, t0.Add(30*time.Second))
	require.EqualValues(t, 6, l3)
}

func TestMinInterval_Ladder(t *testing.T) {
	require.EqualValues(t, 2, MinInterval(1))
	require.EqualValues(t, 2, MinInterval(5))
	require.EqualValues(t, 5, MinInterval(6))
	require.EqualValues(t, 5, MinInterval(11))
	require.EqualValues(t, 10, MinInterval(12))
	require.EqualValues(t, 10, MinInterval(16))
}

func TestEWMA(t *testing.T) {
	// after exactly one half-life the estimate moves halfway to the sample
	require.InDelta(t, 50, EWMA(0, 100, EWMAHalfLife), 0.001)
	// no time elapsed → unchanged
	require.Equal(t, 42.0, EWMA(42, 100, 0))
}
