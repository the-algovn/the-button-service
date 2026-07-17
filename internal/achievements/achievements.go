// Package achievements holds the catalog (spec §9) and the pure evaluation
// rules. Every rule is evaluable from (new total, batch size, server time).
package achievements

import (
	"time"
	_ "time/tzdata" // time rules are defined in Asia/Ho_Chi_Minh; embed tzdata
)

type Achievement struct {
	ID          string
	Title       string
	Description string
}

type Milestone struct {
	Threshold uint64
	Title     string
}

// Catalog is the full personal catalog (spec §9), in presentation order.
var Catalog = []Achievement{
	{"mvh", "Minimum Viable Human", "You clicked the button once. Truly the least you could do."},
	{"ten", "Double Digits", "Ten clicks. Your dedication is now measurable. Barely."},
	{"century", "Century of Defiance", "One hundred clicks against the void."},
	{"comma", "The Comma Club", "1,000 clicks. You've earned punctuation."},
	{"carpal", "Carpal Diem", "10,000 clicks. Seize the wrist brace."},
	{"stretch", "Please Stretch", "100,000 clicks. This is a wellness intervention."},
	{"nice", "Nice.", "Your total crossed 69. You know what you did."},
	{"blaze", "Botanical Enthusiast", "Your total crossed 420. Purely coincidental, we're sure."},
	{"bigbatch", "Mass Production", "500 clicks in a single batch. Industrial-grade defiance."},
	{"maxbatch", "One Batch to Rule Them All", "A perfect 10,000-click batch. The machines are impressed."},
	{"night", "3am Rebellion", "Clicking at 3am. The button appreciates your insomnia."},
	{"lunch", "Lunch Break Rebel", "Clicked between noon and one. The sandwich can wait."},
}

// Milestones are the global thresholds announced by the publisher
// (spec §9), ascending.
var Milestones = []Milestone{
	{1_000, "A Thousand Tiny Rebellions"},
	{100_000, "Six Figures of Defiance"},
	{1_000_000, "One Million. Together We Did… This."},
	{10_000_000, "Ten Million Clicks Nobody Asked For"},
	{1_000_000_000, "The Billion"},
}

var hcm = mustLoad("Asia/Ho_Chi_Minh")

func mustLoad(name string) *time.Location {
	loc, err := time.LoadLocation(name)
	if err != nil {
		panic(err) // tzdata is linked in; cannot happen
	}
	return loc
}

var byID = func() map[string]Achievement {
	m := make(map[string]Achievement, len(Catalog))
	for _, a := range Catalog {
		m[a.ID] = a
	}
	return m
}()

// ByID returns the catalog entry for id.
func ByID(id string) (Achievement, bool) {
	a, ok := byID[id]
	return a, ok
}

// thresholds evaluated with crosses semantics, ascending.
var thresholds = []struct {
	x  uint64
	id string
}{
	{1, "mvh"}, {10, "ten"}, {69, "nice"}, {100, "century"},
	{420, "blaze"}, {1_000, "comma"}, {10_000, "carpal"}, {100_000, "stretch"},
}

// crosses reports old_total < x ≤ new_total with old_total = total - batch
// (spec §9).
func crosses(total uint64, batch uint32, x uint64) bool {
	old := uint64(0)
	if total > uint64(batch) {
		old = total - uint64(batch)
	}
	return old < x && x <= total
}

// Evaluate returns every achievement earned by a batch that brought the
// user to total at now. Threshold rules use crosses semantics so already-
// earned rows are never re-proposed; batch/time rules rely on the
// ON CONFLICT DO NOTHING insert to dedupe.
func Evaluate(total uint64, batch uint32, now time.Time) []Achievement {
	var out []Achievement
	add := func(id string) {
		a, _ := ByID(id)
		out = append(out, a)
	}
	for _, th := range thresholds {
		if crosses(total, batch, th.x) {
			add(th.id)
		}
	}
	if batch >= 500 {
		add("bigbatch")
	}
	if batch == 10_000 {
		add("maxbatch")
	}
	switch now.In(hcm).Hour() {
	case 3:
		add("night")
	case 12:
		add("lunch")
	}
	return out
}
