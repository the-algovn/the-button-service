// Package leaderboard holds the week-boundary math and Redis key names shared
// by the click path, the publisher, and the GetLeaderboard read.
package leaderboard

import (
	"time"
	_ "time/tzdata" // week boundary is Asia/Ho_Chi_Minh; embed tzdata
)

const (
	AllTimeKey = "lb:alltime"
	WeekTTL    = 14 * 24 * time.Hour
)

var hcm = func() *time.Location {
	loc, err := time.LoadLocation("Asia/Ho_Chi_Minh")
	if err != nil {
		panic(err) // tzdata linked in; cannot happen
	}
	return loc
}()

// WeekStart returns the Monday 00:00 ICT that opens now's week, as a midnight
// time anchored to that calendar date (its Format("2006-01-02") is the DATE
// stored in user_weekly_clicks.week_start).
func WeekStart(now time.Time) time.Time {
	t := now.In(hcm)
	// Go's Weekday: Sunday=0..Saturday=6. Days since Monday:
	offset := (int(t.Weekday()) + 6) % 7
	y, m, d := t.Date()
	monday := time.Date(y, m, d-offset, 0, 0, 0, 0, hcm)
	return monday
}

// WeekStartString is WeekStart formatted as an ISO date.
func WeekStartString(now time.Time) string {
	return WeekStart(now).Format("2006-01-02")
}

// WeekKey is the Redis sorted-set key for now's week.
func WeekKey(now time.Time) string {
	return "lb:week:" + WeekStartString(now)
}
