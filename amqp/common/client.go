// Package k6common defines shared interfaces and types used by both the
// AMQP 0.9.1 and AMQP 1.0 client implementations.
package k6common

import "io"

// ExchangeKind is a typed enum for AMQP exchange types.
type ExchangeKind string

const (
	ExchangeKindDirect  ExchangeKind = "direct"
	ExchangeKindTopic   ExchangeKind = "topic"
	ExchangeKindFanout  ExchangeKind = "fanout"
	ExchangeKindHeaders ExchangeKind = "headers"
)

// QueueType is a typed enum for RabbitMQ queue variants.
// Quorum and Stream types are only supported on AMQP 1.0.
type QueueType string

const (
	QueueTypeClassic QueueType = "classic"
	QueueTypeQuorum  QueueType = "quorum"
	QueueTypeStream  QueueType = "stream"
)

// ConnectionOptions is implemented by both amqp091.ConnectionOptions and
// amqp10.ConnectionOptions, enabling protocol-agnostic connection setup.
type ConnectionOptions interface {
	ToConnectionString() string
}

// AMQPClient is the interface implemented by both *amqp091.Client and
// *amqp10.Client, covering management operations and lifecycle. It enables
// protocol-agnostic usage and compile-time swapping.
//
// Publish and Listen are intentionally excluded: they accept protocol-specific
// concrete option types (*amqp091.PublishOptions / *amqp10.PublishOptions) so
// that sobek can reflect them directly from JS without a manual wrapper.
type AMQPClient interface {
	QueueManager
	ExchangeManager
	io.Closer
}

// QueueInfo is the normalised queue metadata returned by InspectQueue.
type QueueInfo struct {
	Name      string `json:"name"`
	Messages  int    `json:"messages"`
	Consumers int    `json:"consumers"`
}

// QueueManager groups queue lifecycle operations.
// The opts parameter accepts the protocol-specific options struct pointer
// (e.g. *amqp091.DeclareQueueOptions or *amqp10.DeclareQueueOptions).
type QueueManager interface {
	DeclareQueue(opts any) error
	DeleteQueue(name string) error
	BindQueue(opts any) (string, error)
	UnbindQueue(bindingPath string) error
	PurgeQueue(name string) (int, error)
	InspectQueue(name string) (*QueueInfo, error)
}

// ExchangeManager groups exchange lifecycle operations.
// The opts parameter accepts the protocol-specific options struct pointer.
type ExchangeManager interface {
	DeclareExchange(opts any) error
	DeleteExchange(name string) error
	BindExchange(opts any) (string, error)
	UnbindExchange(bindingPath string) error
}
