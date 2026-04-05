package amqp10

import (
	"errors"

	goamqp "github.com/Azure/go-amqp"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

const (
	// Name identifies this protocol implementation.
	Name = "amqp/1.0"
)

// PublishOptions defines a message payload and its delivery options for AMQP 1.0.
type PublishOptions struct {
	// QueueName publishes directly to a queue. Mutually exclusive with Exchange.
	QueueName string `json:"queueName,omitempty" js:"queueName"`
	// Exchange publishes to an exchange with an optional RoutingKey.
	Exchange   string `json:"exchange,omitempty"   js:"exchange"`
	RoutingKey string `json:"routingKey,omitempty" js:"routingKey"`

	Body        []byte `json:"body,omitempty"        js:"body"`
	ContentType string `json:"contentType,omitempty" js:"contentType"`
	// Persistent sets the Durable flag on the AMQP message header.
	Persistent    bool   `json:"persistent,omitempty"    js:"persistent"`
	Subject       string `json:"subject,omitempty"       js:"subject"`       // AMQP 1.0 Properties.Subject
	CorrelationID string `json:"correlationId,omitempty" js:"correlationId"` // AMQP 1.0 Properties.CorrelationID
	MessageID     string `json:"messageId,omitempty"     js:"messageId"`     // AMQP 1.0 Properties.MessageID
}

// Name satisfies k6common.PublishingOptions.
func (o *PublishOptions) Name() string { return Name }

// Publish delivers a message synchronously to the configured target.
// Either QueueName or Exchange must be set.
//
// A new publisher is created per call. For high-throughput scenarios, consider
// reusing a publisher via caching (future enhancement).
func (c *Client) Publish(opts *PublishOptions) error {
	if err := c.connect(); err != nil {
		return err
	}

	var target rmq.ITargetAddress
	switch {
	case opts.Exchange != "":
		target = &rmq.ExchangeAddress{Exchange: opts.Exchange, Key: opts.RoutingKey}
	case opts.QueueName != "":
		target = &rmq.QueueAddress{Queue: opts.QueueName}
	default:
		return errors.New("publish: either queueName or exchange must be specified")
	}

	publisher, err := c.amqpConnection.NewPublisher(c.vu.Context(), target, nil)
	if err != nil {
		return err
	}
	defer publisher.Close(c.vu.Context()) //nolint:errcheck

	msg := rmq.NewMessage(opts.Body)

	// Set message header for persistence.
	if opts.Persistent {
		msg.Header = &goamqp.MessageHeader{Durable: true}
	}

	// Set message properties if any are provided.
	if opts.ContentType != "" || opts.CorrelationID != "" || opts.MessageID != "" || opts.Subject != "" {
		msg.Properties = &goamqp.MessageProperties{}
		if opts.ContentType != "" {
			ct := opts.ContentType
			msg.Properties.ContentType = &ct
		}
		if opts.CorrelationID != "" {
			msg.Properties.CorrelationID = opts.CorrelationID
		}
		if opts.MessageID != "" {
			msg.Properties.MessageID = opts.MessageID
		}
		if opts.Subject != "" {
			msg.Properties.Subject = &opts.Subject
		}
	}

	result, err := publisher.Publish(c.vu.Context(), msg)
	if err != nil {
		return err
	}
	if _, rejected := result.Outcome.(*rmq.StateRejected); rejected {
		return errors.New("publish: message rejected by broker")
	}

	return nil
}
