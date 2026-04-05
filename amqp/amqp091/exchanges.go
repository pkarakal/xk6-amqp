package amqp091

import (
	k6common "github.com/grafana/xk6-amqp/amqp/common"
	rmqamqp "github.com/rabbitmq/amqp091-go"
)

// DeclareExchangeOptions holds parameters for declaring an AMQP exchange.
type DeclareExchangeOptions struct {
	Name       string               `json:"name"`
	Kind       k6common.ExchangeKind `json:"kind"`
	Durable    bool                 `json:"durable,omitempty"`
	AutoDelete bool                 `json:"autoDelete,omitempty"`
	Internal   bool                 `json:"internal,omitempty"`
	NoWait     bool                 `json:"noWait,omitempty"`
	Args       rmqamqp.Table        `json:"args,omitempty"`
}

// DeleteExchangeOptions holds parameters for deleting an AMQP exchange.
type DeleteExchangeOptions struct {
	IfUnused bool `json:"ifUnused,omitempty"`
	NoWait   bool `json:"noWait,omitempty"`
}

// BindExchangeOptions holds parameters for binding one exchange to another.
type BindExchangeOptions struct {
	Source      string        `json:"source"`
	Destination string        `json:"destination"`
	RoutingKey  string        `json:"routingKey,omitempty"`
	NoWait      bool          `json:"noWait,omitempty"`
	Args        rmqamqp.Table `json:"args,omitempty"`
}

// UnbindExchangeOptions holds parameters for removing an exchange binding.
type UnbindExchangeOptions struct {
	Source      string        `json:"source"`
	Destination string        `json:"destination"`
	RoutingKey  string        `json:"routingKey,omitempty"`
	Args        rmqamqp.Table `json:"args,omitempty"`
}

// DeclareExchange declares an exchange on the broker. Satisfies k6common.ExchangeManager.
func (c *Client) DeclareExchange(opts any) error {
	o, err := convertOpts[DeclareExchangeOptions]("DeclareExchange", opts)
	if err != nil {
		return err
	}
	if err := c.connect(c.connectionOptions); err != nil {
		return err
	}
	ch, err := c.amqpClient.Channel()
	if err != nil {
		return err
	}
	defer ch.Close() //nolint:errcheck

	return ch.ExchangeDeclare(o.Name, string(o.Kind), o.Durable, o.AutoDelete, o.Internal, o.NoWait, o.Args)
}

// DeleteExchange removes an exchange from the broker.
func (c *Client) DeleteExchange(name string) error {
	if err := c.connect(c.connectionOptions); err != nil {
		return err
	}
	ch, err := c.amqpClient.Channel()
	if err != nil {
		return err
	}
	defer ch.Close() //nolint:errcheck

	return ch.ExchangeDelete(name, false, false)
}

// BindExchange binds a destination exchange to a source exchange. Returns an
// empty string for interface compatibility with the AMQP 1.0 path-based model.
func (c *Client) BindExchange(opts any) (string, error) {
	o, err := convertOpts[BindExchangeOptions]("BindExchange", opts)
	if err != nil {
		return "", err
	}
	if err := c.connect(c.connectionOptions); err != nil {
		return "", err
	}
	ch, err := c.amqpClient.Channel()
	if err != nil {
		return "", err
	}
	defer ch.Close() //nolint:errcheck

	return "", ch.ExchangeBind(o.Destination, o.RoutingKey, o.Source, o.NoWait, o.Args)
}

// UnbindExchange removes an exchange binding. The bindingPath parameter is
// ignored for AMQP 0.9.1; use UnbindExchangeWithOptions instead.
func (c *Client) UnbindExchange(bindingPath string) error {
	_ = bindingPath
	return errNotSupported("UnbindExchange via binding path is not supported for AMQP 0.9.1; use UnbindExchangeWithOptions")
}

// UnbindExchangeWithOptions removes an exchange binding using explicit parameters.
func (c *Client) UnbindExchangeWithOptions(opts *UnbindExchangeOptions) error {
	if err := c.connect(c.connectionOptions); err != nil {
		return err
	}
	ch, err := c.amqpClient.Channel()
	if err != nil {
		return err
	}
	defer ch.Close() //nolint:errcheck

	return ch.ExchangeUnbind(opts.Destination, opts.RoutingKey, opts.Source, false, opts.Args)
}
