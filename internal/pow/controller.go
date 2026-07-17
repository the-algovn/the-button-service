package pow

import (
	"math"
	"time"
)

// Difficulty controller constants (spec §5): the publisher keeps the
// accepted-submit rate inside [BandLow, BandHigh] by moving L one step at
// a time; min_interval is the hard valve, L the cost valve.
const (
	MinL     = 1
	MaxL     = 16
	BandLow  = 200.0 // accepted submits/s
	BandHigh = 400.0
	// Hysteresis: raise only above 110% of the high edge, lower only
	// below 70% of the low edge.
	raiseAbove = BandHigh * 1.10 // 440/s
	lowerBelow = BandLow * 0.70  // 140/s
	// SlewInterval limits difficulty movement to one step per 30s.
	SlewInterval = 30 * time.Second
	// EWMAHalfLife smooths the sampled rate (spec §6 step 4: ~30s).
	EWMAHalfLife = 30 * time.Second
)

// NextL applies hysteresis and slew to move currentL toward the band.
// It returns the new L and the lastChange timestamp to carry forward.
func NextL(currentL uint32, ewmaRate float64, lastChange, now time.Time) (uint32, time.Time) {
	if now.Sub(lastChange) < SlewInterval {
		return currentL, lastChange
	}
	switch {
	case ewmaRate > raiseAbove && currentL < MaxL:
		return currentL + 1, now
	case ewmaRate < lowerBelow && currentL > MinL:
		return currentL - 1, now
	}
	return currentL, lastChange
}

// MinInterval maps L to the hard per-user interval ladder 2s → 5s → 10s.
func MinInterval(l uint32) uint32 {
	switch {
	case l <= 5:
		return 2
	case l <= 11:
		return 5
	default:
		return 10
	}
}

// EWMA folds a rate sample observed over dt into prev with half-life
// EWMAHalfLife.
func EWMA(prev, sample float64, dt time.Duration) float64 {
	if dt <= 0 {
		return prev
	}
	alpha := 1 - math.Exp2(-dt.Seconds()/EWMAHalfLife.Seconds())
	return prev + alpha*(sample-prev)
}
