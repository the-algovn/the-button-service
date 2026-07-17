package publisher

import "github.com/prometheus/client_golang/prometheus"

var (
	// lastPollUnixtime is the freshness signal behind ButtonTickFrozen: set
	// on every successful poll of SUM(user_clicks), whether or not the total
	// changed, so a genuinely idle counter never masquerades as a stuck loop.
	// The publisher pod is the ONLY exporter of this metric — the alert expr
	// pairs it with absent() to catch the pod not existing at all.
	lastPollUnixtime = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "the_button_last_poll_unixtime",
		Help: "Unix timestamp of the publisher's last successful counter poll, refreshed every ~1s. time() - this growing means the counter broadcast is frozen.",
	})
	publishFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "the_button_publish_failures_total",
		Help: "AMQP publishes that failed (dial or publish error). Rising while polls stay fresh means RabbitMQ trouble: SSE frames stall, GetCounter stays correct.",
	})
)

func init() {
	prometheus.MustRegister(lastPollUnixtime, publishFailures)
}
