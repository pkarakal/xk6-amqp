package amqp091

import (
	"encoding/json"
	"time"

	"github.com/grafana/sobek"
	rmqamqp "github.com/rabbitmq/amqp091-go"
	"github.com/vmihailenco/msgpack/v5"
	"go.k6.io/k6/js/promises"
)

const (
	// Name identifies this protocol implementation.
	Name = "amqp/0.9.1"

	contentTypeMsgpack = "application/x-msgpack"
)

// PublishOptions defines a message payload and its delivery options.
type PublishOptions struct {
	QueueName     string        `json:"queueName,omitempty"`
	Body          []byte        `json:"body,omitempty"`
	Headers       rmqamqp.Table `json:"headers,omitempty"`
	Exchange      string        `json:"exchange,omitempty"`
	ContentType   string        `json:"contentType,omitempty"`
	Mandatory     bool          `json:"mandatory,omitempty"`
	Immediate     bool          `json:"immediate,omitempty"`
	Persistent    bool          `json:"persistent,omitempty"`
	CorrelationID string        `json:"correlationId,omitempty"`
	ReplyTo       string        `json:"replyTo,omitempty"`
	Expiration    string        `json:"expiration,omitempty"`
	MessageID     string        `json:"messageId,omitempty"`
	Timestamp     int64         `json:"timestamp,omitempty"` // unix epoch seconds
	Type          string        `json:"type,omitempty"`
	UserID        string        `json:"userId,omitempty"`
	AppID         string        `json:"appId,omitempty"`
	RoutingKey    string        `json:"routingKey,omitempty"`
}

// Name satisfies k6common.PublishingOptions.
func (o *PublishOptions) Name() string { return Name }

// toAMQPPublishing converts PublishOptions to the amqp091 Publishing type.
func (o *PublishOptions) toAMQPPublishing() rmqamqp.Publishing {
	p := rmqamqp.Publishing{
		Headers:       o.Headers,
		ContentType:   o.ContentType,
		Body:          o.Body,
		CorrelationId: o.CorrelationID,
		ReplyTo:       o.ReplyTo,
		Expiration:    o.Expiration,
		MessageId:     o.MessageID,
		Type:          o.Type,
		UserId:        o.UserID,
		AppId:         o.AppID,
	}
	if o.Persistent {
		p.DeliveryMode = rmqamqp.Persistent
	}
	if o.Timestamp != 0 {
		p.Timestamp = time.Unix(o.Timestamp, 0)
	}
	return p
}

// Publish delivers a message synchronously. Supports application/x-msgpack
// content type by JSON-decoding the body and re-encoding as msgpack.
func (c *Client) Publish(opts *PublishOptions) error {
	if err := c.connect(c.connectionOptions); err != nil {
		return err
	}

	publishing := opts.toAMQPPublishing()

	if opts.ContentType == contentTypeMsgpack {
		var parsed any
		if err := json.Unmarshal(opts.Body, &parsed); err != nil {
			return err
		}
		encoded, err := msgpack.Marshal(parsed)
		if err != nil {
			return err
		}
		publishing.Body = encoded
	}

	return c.amqpChannel.PublishWithContext(
		c.vu.Context(),
		opts.Exchange,
		opts.RoutingKey,
		opts.Mandatory,
		opts.Immediate,
		publishing,
	)
}

// PublishAsync delivers a message asynchronously and returns a Promise.
// It opens a fresh channel per call to avoid concurrent-access issues.
func (c *Client) PublishAsync(opts *PublishOptions) *sobek.Promise {
	promise, resolve, reject := promises.New(c.vu)

	if err := c.connect(c.connectionOptions); err != nil {
		reject(err)
		return promise
	}

	ch, err := c.amqpClient.Channel()
	if err != nil {
		reject(err)
		return promise
	}

	publishing := opts.toAMQPPublishing()

	go func() {
		defer ch.Close() //nolint:errcheck
		if err := ch.PublishWithContext(
			c.vu.Context(),
			opts.Exchange,
			opts.RoutingKey,
			opts.Mandatory,
			opts.Immediate,
			publishing,
		); err != nil {
			reject(err)
			return
		}
		resolve(nil)
	}()

	return promise
}
