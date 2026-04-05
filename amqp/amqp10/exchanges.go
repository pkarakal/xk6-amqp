package amqp10

import (
	k6common "github.com/grafana/xk6-amqp/amqp/common"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

// DeclareExchangeOptions holds parameters for declaring an exchange via AMQP 1.0 management.
type DeclareExchangeOptions struct {
	Name       string               `json:"name"`
	Kind       k6common.ExchangeKind `json:"kind"` // "direct" (default), "topic", "fanout", "headers"
	AutoDelete bool                 `json:"autoDelete,omitempty"`
	Arguments  map[string]any       `json:"args,omitempty"`
}

// BindExchangeOptions holds parameters for binding one exchange to another.
type BindExchangeOptions struct {
	SourceExchange      string         `json:"sourceExchange"`
	DestinationExchange string         `json:"destinationExchange"`
	BindingKey          string         `json:"bindingKey,omitempty"`
	Arguments           map[string]any `json:"args,omitempty"`
}

// DeclareExchange declares an exchange on the broker. Satisfies k6common.ExchangeManager.
func (c *Client) DeclareExchange(opts any) error {
	o, err := convertOpts[DeclareExchangeOptions]("DeclareExchange", opts)
	if err != nil {
		return err
	}
	if err := c.connect(); err != nil {
		return err
	}

	var spec rmq.IExchangeSpecification
	switch o.Kind {
	case k6common.ExchangeKindTopic:
		spec = &rmq.TopicExchangeSpecification{Name: o.Name, IsAutoDelete: o.AutoDelete}
	case k6common.ExchangeKindFanout:
		spec = &rmq.FanOutExchangeSpecification{Name: o.Name, IsAutoDelete: o.AutoDelete}
	case k6common.ExchangeKindHeaders:
		spec = &rmq.HeadersExchangeSpecification{Name: o.Name, IsAutoDelete: o.AutoDelete}
	default: // "direct" or empty
		spec = &rmq.DirectExchangeSpecification{Name: o.Name, IsAutoDelete: o.AutoDelete}
	}

	_, declErr := c.amqpConnection.Management().DeclareExchange(c.vu.Context(), spec)
	return declErr
}

// DeleteExchange removes an exchange from the broker.
func (c *Client) DeleteExchange(name string) error {
	if err := c.connect(); err != nil {
		return err
	}
	return c.amqpConnection.Management().DeleteExchange(c.vu.Context(), name)
}

// BindExchange binds a destination exchange to a source exchange and returns the binding path.
// The returned path is required by UnbindExchange.
func (c *Client) BindExchange(opts any) (string, error) {
	o, err := convertOpts[BindExchangeOptions]("BindExchange", opts)
	if err != nil {
		return "", err
	}
	if err := c.connect(); err != nil {
		return "", err
	}
	return c.amqpConnection.Management().Bind(c.vu.Context(), &rmq.ExchangeToExchangeBindingSpecification{
		SourceExchange:      o.SourceExchange,
		DestinationExchange: o.DestinationExchange,
		BindingKey:          o.BindingKey,
		Arguments:           o.Arguments,
	})
}

// UnbindExchange removes an exchange binding identified by its path (as returned by BindExchange).
func (c *Client) UnbindExchange(bindingPath string) error {
	if err := c.connect(); err != nil {
		return err
	}
	return c.amqpConnection.Management().Unbind(c.vu.Context(), bindingPath)
}
