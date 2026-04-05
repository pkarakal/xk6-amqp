package k6common

// ListenerType is the message handler implemented within JavaScript.
// It receives the message body as a string and returns an error.
type ListenerType func(string) error

// ConsumingOptions is implemented by both amqp091.ListenOptions and
// amqp10.ListenOptions.
type ConsumingOptions interface {
	Name() string
}

// Consumer is implemented by both protocol clients for message consumption.
type Consumer interface {
	Listen(opts ConsumingOptions) error
}
