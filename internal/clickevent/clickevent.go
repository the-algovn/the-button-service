// Package clickevent is the typed, JSON-encoded event on the Kafka `clicks`
// topic. Internal to the service (producer = API, consumers = the two worker
// groups); JSON keeps it dependency-free.
package clickevent

import "encoding/json"

type Click struct {
	Sub         string `json:"sub"`
	Count       uint32 `json:"count"`
	ChallengeID string `json:"challenge_id"`
	TsUnix      int64  `json:"ts_unix"`
	DisplayName string `json:"display_name"`
}

func (c Click) Marshal() ([]byte, error) { return json.Marshal(c) }

func Unmarshal(b []byte) (Click, error) {
	var c Click
	err := json.Unmarshal(b, &c)
	return c, err
}

// Key is the Kafka partition key: the user's sub, so one user's events stay
// ordered on one partition (streak/quest/dedup correctness).
func (c Click) Key() []byte { return []byte(c.Sub) }
