package amqp091

import (
	"fmt"

	k6common "github.com/grafana/xk6-amqp/amqp/common"
	rmqamqp "github.com/rabbitmq/amqp091-go"
)

// ListenOptions defines options for consuming messages from a queue.
type ListenOptions struct {
	QueueName string                `json:"queueName,omitempty"`
	Consumer  string                `json:"consumer,omitempty"`
	AutoAck   bool                  `json:"autoAck,omitempty"`
	Exclusive bool                  `json:"exclusive,omitempty"`
	NoLocal   bool                  `json:"noLocal,omitempty"`
	NoWait    bool                  `json:"noWait,omitempty"`
	Args      rmqamqp.Table         `json:"args,omitempty"`
	Listener  k6common.ListenerType `js:"-"` // set from JS; not JSON-serialized
}

// Name satisfies k6common.ConsumingOptions.
func (o *ListenOptions) Name() string { return Name }

// Listen binds to a queue and dispatches incoming messages to the Listener
// callback in a background goroutine. The goroutine exits when the VU context
// is cancelled or the channel is closed.
//
// Each call opens a dedicated AMQP channel so the consumer does not share a
// channel with the publisher (amqp091-go channels are not goroutine-safe for
// concurrent operations).
//
// The JS listener is invoked via vu.RegisterCallback() so it always runs on
// the VU's event loop thread (during sleep() or between iterations). Calling
// JS from a plain goroutine is not safe in sobek.
func (c *Client) Listen(opts *ListenOptions) error {
	if err := c.connect(c.connectionOptions); err != nil {
		return err
	}

	// Dedicated channel for this consumer.
	ch, err := c.amqpClient.Channel()
	if err != nil {
		return err
	}

	msgs, err := ch.ConsumeWithContext(
		c.consumerCtx,
		opts.QueueName,
		opts.Consumer,
		opts.AutoAck,
		opts.Exclusive,
		opts.NoLocal,
		opts.NoWait,
		opts.Args,
	)
	if err != nil {
		ch.Close() //nolint:errcheck
		return err
	}

	vu := c.vu
	listener := opts.Listener
	go func() {
		defer ch.Close() //nolint:errcheck
		for d := range msgs {
			body := string(d.Body)
			// RegisterCallback schedules JS execution on the VU's event loop.
			// The returned function MUST be called exactly once.
			schedule := vu.RegisterCallback()
			schedule(func() error {
				return listener(body)
			})
		}
		fmt.Println("[DEBUG amqp091] consumer goroutine exited")
	}()

	return nil
}
