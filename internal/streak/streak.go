// Package streak tracks a per-user daily contribution streak keyed on the
// Asia/Ho_Chi_Minh calendar date. Pure: the progress worker supplies today's
// HCM date and persists the returned State in Redis.
package streak

import "time"

const dateFmt = "2006-01-02"

type State struct {
	Count   uint32
	Best    uint32
	LastDay string // HCM YYYY-MM-DD; empty if the user has never contributed
}

// Advance records a contribution on todayHCM (a YYYY-MM-DD HCM date). It returns
// the new state and whether it advanced. Advancing a second time on the same day
// is a no-op (returns the input unchanged, false). A contribution on the day
// immediately after LastDay extends the streak; any gap resets Count to 1. Best
// is the high-water mark and never decreases.
func Advance(cur State, todayHCM string) (State, bool) {
	if cur.LastDay == todayHCM {
		return cur, false
	}
	next := cur
	if cur.LastDay != "" && isNextDay(cur.LastDay, todayHCM) {
		next.Count = cur.Count + 1
	} else {
		next.Count = 1 // first ever, or a gap
	}
	next.LastDay = todayHCM
	if next.Count > next.Best {
		next.Best = next.Count
	}
	return next, true
}

// isNextDay reports whether today is exactly one calendar day after last.
func isNextDay(last, today string) bool {
	lt, err1 := time.Parse(dateFmt, last)
	tt, err2 := time.Parse(dateFmt, today)
	if err1 != nil || err2 != nil {
		return false
	}
	return lt.AddDate(0, 0, 1).Equal(tt)
}
