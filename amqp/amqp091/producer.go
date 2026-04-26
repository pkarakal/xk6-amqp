package amqp091

import (
	"encoding/json"
	"errors"
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
	QueueName     string        `json:"queueName,omitempty"     js:"queueName"`
	Body          []byte        `json:"body,omitempty"          js:"body"`
	Headers       rmqamqp.Table `json:"headers,omitempty"       js:"headers"`
	Exchange      string        `json:"exchange,omitempty"      js:"exchange"`
	ContentType   string        `json:"contentType,omitempty"   js:"contentType"`
	Mandatory     bool          `json:"mandatory,omitempty"     js:"mandatory"`
	Immediate     bool          `json:"immediate,omitempty"     js:"immediate"`
	Persistent    bool          `json:"persistent,omitempty"    js:"persistent"`
	CorrelationID string        `json:"correlationId,omitempty" js:"correlationId"`
	ReplyTo       string        `json:"replyTo,omitempty"       js:"replyTo"`
	Expiration    string        `json:"expiration,omitempty"    js:"expiration"`
	MessageID     string        `json:"messageId,omitempty"     js:"messageId"`
	Timestamp     int64         `json:"timestamp,omitempty"     js:"timestamp"` // unix epoch seconds
	Type          string        `json:"type,omitempty"          js:"type"`
	UserID        string        `json:"userId,omitempty"        js:"userId"`
	AppID         string        `json:"appId,omitempty"         js:"appId"`
	RoutingKey    string        `json:"routingKey,omitempty"    js:"routingKey"`
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
// It opens a dedicated channel with publisher confirms enabled; the promise
// resolves only when the broker sends a positive acknowledgement (Ack).
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

	// Enable publisher confirms on this dedicated channel.
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		reject(err)
		return promise
	}

	notifyPublish := ch.NotifyPublish(make(chan rmqamqp.Confirmation, 1))
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

		select {
		case confirm := <-notifyPublish:
			if confirm.Ack {
				resolve(nil)
			} else {
				reject(errors.New("publishAsync: broker nacked the message"))
			}
		case <-c.vu.Context().Done():
			reject(c.vu.Context().Err())
		}
	}()

	return promise
}
