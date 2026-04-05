package amqp

import "github.com/grafana/sobek"

type PublishingOptions interface {
	Name() interface{}
	ToProtocolOptions() interface{}
}

type Publisher interface {
	Publish(options PublishingOptions) error
}

type PublisherAsync interface {
	PublishAsync(options PublishingOptions) *sobek.Promise
}
