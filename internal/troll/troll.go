// Package troll holds the hidden one-off / secret unlocks — absurd triggers a
// user stumbles into by doing weird things. Pure and server-observable only
// (total, HCM clock, whether a global milestone landed this same second); never
// the client-cosmetic combo. The progress worker calls Evaluate per accepted
// batch and announces any new unlocks.
package troll

import (
	"strconv"
	"time"
	_ "time/tzdata" // clock rules are in Asia/Ho_Chi_Minh; embed tzdata
)

type Unlock struct {
	ID          string
	Title       string
	Description string
}

var hcm = mustLoad("Asia/Ho_Chi_Minh")

func mustLoad(name string) *time.Location {
	loc, err := time.LoadLocation(name)
	if err != nil {
		panic(err) // tzdata is linked in; cannot happen
	}
	return loc
}

// Evaluate returns every hidden unlock the accepted batch just triggered.
func Evaluate(total uint64, atHCM time.Time, milestoneHitThisSecond bool) []Unlock {
	var out []Unlock
	add := func(id, title, desc string) { out = append(out, Unlock{id, title, desc}) }

	if total >= 100 && isPalindrome(total) {
		add("palindrome", "Palindrome", "Your total reads the same both ways. The button is unsettled.")
	}
	if total >= 1024 && isPowerOfTwo(total) {
		add("power_of_two", "Powers That Be", "Your total landed on a power of two. The machines approve.")
	}
	if total >= 100 && isRepdigit(total) {
		add("repdigit", "All the Same", "Your total is all one digit. Immaculate.")
	}

	local := atHCM.In(hcm)
	if local.Minute() == 20 && (local.Hour() == 4 || local.Hour() == 16) {
		add("clock_420", "Blaze It (Clock Edition)", "A batch at 4:20, HCMC time. We're not saying anything.")
	}
	if milestoneHitThisSecond {
		add("milestone_second", "Right Place, Right Time", "You contributed the very second a global milestone landed.")
	}
	return out
}

func isPalindrome(n uint64) bool {
	s := strconv.FormatUint(n, 10)
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		if s[i] != s[j] {
			return false
		}
	}
	return true
}

func isPowerOfTwo(n uint64) bool { return n > 0 && n&(n-1) == 0 }

func isRepdigit(n uint64) bool {
	s := strconv.FormatUint(n, 10)
	for i := 1; i < len(s); i++ {
		if s[i] != s[0] {
			return false
		}
	}
	return true
}
