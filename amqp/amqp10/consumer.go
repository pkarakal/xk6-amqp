package amqp10

import (
	"context"

	k6common "github.com/pkarakal/xk6-amqp/amqp/common"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

// ListenOptions defines options for consuming messages from a queue via AMQP 1.0.
type ListenOptions struct {
	QueueName      string                `json:"queueName"`
	Consumer       string                `json:"consumer,omitempty"` // receiver link name
	AutoAck        bool                  `json:"autoAck,omitempty"`
	InitialCredits int32                 `json:"initialCredits,omitempty"` // flow control; defaults to 256
	Listener       k6common.ListenerType `js:"-"`                          // set from JS; not JSON-serialized
}

// Name satisfies k6common.ConsumingOptions.
func (o *ListenOptions) Name() string { return Name }

// Listen binds to a queue and dispatches incoming messages to the Listener
// callback in a background goroutine. The goroutine exits when the VU context
// is cancelled or the consumer connection is closed.
//
// The JS listener is invoked via vu.RegisterCallback() so it always runs on
// the VU's event loop thread. Calling JS from a plain goroutine is not safe
// in sobek.
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
			delivery, err := consumer.Receive(c.consumerCtx)
			if err != nil {
				// Context cancelled or connection closed — exit cleanly.
				return
			}

			rawMsg := delivery.Message()
			msg := &k6common.Message{
				Body:    string(rawMsg.GetData()),
				Headers: rawMsg.ApplicationProperties,
				Accept:  func() error { return delivery.Accept(context.Background()) },
				// AMQP 1.0 does not support requeueing; the requeue parameter is ignored.
				Discard: func(_ bool) error {
					return delivery.Discard(context.Background(), nil)
				},
			}
			if rawMsg.Properties != nil {
				if rawMsg.Properties.Subject != nil {
					msg.RoutingKey = *rawMsg.Properties.Subject
				}
				if rawMsg.Properties.ContentType != nil {
					msg.ContentType = *rawMsg.Properties.ContentType
				}
				if id, ok := rawMsg.Properties.CorrelationID.(string); ok {
					msg.CorrelationID = id
				}
				if id, ok := rawMsg.Properties.MessageID.(string); ok {
					msg.MessageID = id
				}
			}

			// RegisterCallback schedules JS execution on the VU's event loop.
			// The returned function MUST be called exactly once.
			schedule := vu.RegisterCallback()
			schedule(func() error {
				cbErr := listener(msg)
				if autoAck {
					if cbErr != nil {
						_ = msg.Discard(false)
						return cbErr
					}
					_ = msg.Accept()
				}
				return cbErr
			})
		}
	}()

	return nil
}
