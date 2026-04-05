package k6common

import "github.com/grafana/sobek"

// PublishingOptions is implemented by both amqp091.PublishOptions and
// amqp10.PublishOptions.
type PublishingOptions interface {
	Name() string
}

// Publisher is implemented by both protocol clients for synchronous publishing.
type Publisher interface {
	Publish(opts PublishingOptions) error
}

// PublisherAsync is implemented by both protocol clients for async publishing.
type PublisherAsync interface {
	PublishAsync(opts PublishingOptions) *sobek.Promise
}
