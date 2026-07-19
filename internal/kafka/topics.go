package kafka

// Kafka topic names. `clicks` is the click log (key = user sub); the two worker
// groups consume it. The sse.* topics carry SSE frames api-control-plane fans out.
const (
	TopicClicks         = "clicks"
	TopicSSECounter     = "sse.counter"
	TopicSSELeaderboard = "sse.leaderboard"
	TopicSSEUser        = "sse.user"
)
