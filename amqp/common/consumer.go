package k6common

// Message is passed to the listener callback for each received message.
// Accept and Discard are only meaningful when autoAck is false; when autoAck
// is true they are no-ops (AMQP 0.9.1) or called automatically by the
// extension after the listener returns (AMQP 1.0).
//
// Note: for AMQP 1.0 the requeue parameter of Discard is ignored — the
// protocol does not support requeueing via the consumer link.
type Message struct {
	Body          string                 `json:"body"                    js:"body"`
	RoutingKey    string                 `json:"routingKey,omitempty"    js:"routingKey"`
	ContentType   string                 `json:"contentType,omitempty"   js:"contentType"`
	CorrelationID string                 `json:"correlationId,omitempty" js:"correlationId"`
	Headers       map[string]any         `json:"headers,omitempty"       js:"headers"`
	MessageID     string                 `json:"messageId,omitempty"     js:"messageId"`

	// Accept acknowledges the message. Only meaningful when autoAck is false.
	Accept func() error `js:"accept"`
	// Discard rejects the message. requeue is honoured by AMQP 0.9.1 only.
	Discard func(requeue bool) error `js:"discard"`
}

// ListenerType is the message handler implemented in JavaScript.
// It receives the full Message envelope and returns an error.
type ListenerType func(*Message) error

// ConsumingOptions is implemented by both amqp091.ListenOptions and
// amqp10.ListenOptions.
type ConsumingOptions interface {
	Name() string
}

// Consumer is implemented by both protocol clients for message consumption.
type Consumer interface {
	Listen(opts ConsumingOptions) error
}
