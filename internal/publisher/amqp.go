package publisher

import (
	"context"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// NewAMQPPublisher returns a fire-and-forget AMQP publish func; failures are
// logged and counted, never fatal — events are best-effort by design.
func NewAMQPPublisher(ctx context.Context, url string, logger *slog.Logger) func(string, []byte) {
	type conn struct {
		ch *amqp.Channel
		c  *amqp.Connection
	}
	var mu sync.Mutex
	var cur *conn
	dial := func() *conn {
		// Bounded dial: a hung broker must not stall the poll loop.
		c, err := amqp.DialConfig(url, amqp.Config{Dial: amqp.DefaultDial(5 * time.Second)})
		if err != nil {
			logger.Warn("amqp dial failed", "err", err)
			return nil
		}
		ch, err := c.Channel()
		if err != nil {
			_ = c.Close()
			return nil
		}
		if err := ch.ExchangeDeclare("events", "topic", true, false, false, false, nil); err != nil {
			_ = c.Close()
			return nil
		}
		return &conn{ch: ch, c: c}
	}
	return func(channel string, body []byte) {
		mu.Lock()
		defer mu.Unlock()
		if cur == nil || cur.c.IsClosed() || cur.ch.IsClosed() {
			cur = dial()
			if cur == nil {
				publishFailures.Inc()
				return
			}
		}
		pubCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		err := cur.ch.PublishWithContext(pubCtx, "events", channel, false, false,
			amqp.Publishing{ContentType: "application/json", Body: body})
		if err != nil {
			logger.Warn("publish failed", "channel", channel, "err", err)
			publishFailures.Inc()
			cur = nil
		}
	}
}
