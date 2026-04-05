package amqp10

import (
	"context"

	k6common "github.com/grafana/xk6-amqp/amqp/common"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

// ListenOptions defines options for consuming messages from a queue via AMQP 1.0.
type ListenOptions struct {
	QueueName      string                `json:"queueName"`
	Consumer       string                `json:"consumer,omitempty"`       // receiver link name
	AutoAck        bool                  `json:"autoAck,omitempty"`
	InitialCredits int32                 `json:"initialCredits,omitempty"` // flow control; defaults to 256
	Listener       k6common.ListenerType `js:"-"`                          // set from JS; not JSON-serialized
}

// Name satisfies k6common.ConsumingOptions.
func (o *ListenOptions) Name() string { return Name }

// Listen binds to a queue and dispatches incoming messages to the Listener
// callback in a background goroutine. The goroutine exits when the VU context
// is cancelled or the consumer connection is closed.
func (c *Client) Listen(opts *ListenOptions) error {
	if err := c.connect(); err != nil {
		return err
	}

	credits := opts.InitialCredits
	if credits == 0 {
		credits = 256
	}

	consumer, err := c.amqpConnection.NewConsumer(c.vu.Context(), opts.QueueName, &rmq.ConsumerOptions{
		ReceiverLinkName: opts.Consumer,
		InitialCredits:   credits,
	})
	if err != nil {
		return err
	}

	vu := c.vu
	listener := opts.Listener
	autoAck := opts.AutoAck

	go func() {
		defer consumer.Close(context.Background()) //nolint:errcheck

		for {
			delivery, err := consumer.Receive(c.vu.Context())
			if err != nil {
				// Context cancelled or connection closed — exit cleanly.
				return
			}

			body := string(delivery.Message().GetData())
			// RegisterCallback schedules JS execution on the VU's event loop.
			// The returned function MUST be called exactly once.
			schedule := vu.RegisterCallback()
			schedule(func() error {
				if cbErr := listener(body); cbErr != nil {
					_ = delivery.Discard(context.Background(), nil)
					return cbErr
				}
				if autoAck {
					_ = delivery.Accept(context.Background())
				}
				return nil
			})
		}
	}()

	return nil
}
