package amqp091

import (
	rmqamqp "github.com/rabbitmq/amqp091-go"
)

// DeclareQueueOptions holds parameters for declaring an AMQP queue.
type DeclareQueueOptions struct {
	Name             string        `json:"name"`
	Durable          bool          `json:"durable,omitempty"`
	DeleteWhenUnused bool          `json:"deleteWhenUnused,omitempty"`
	Exclusive        bool          `json:"exclusive,omitempty"`
	NoWait           bool          `json:"noWait,omitempty"`
	Args             rmqamqp.Table `json:"args,omitempty"`
}

// DeleteQueueOptions holds parameters for deleting an AMQP queue.
type DeleteQueueOptions struct {
	IfUnused bool `json:"ifUnused,omitempty"`
	IfEmpty  bool `json:"ifEmpty,omitempty"`
	NoWait   bool `json:"noWait,omitempty"`
}

// BindQueueOptions holds parameters for binding a queue to an exchange.
type BindQueueOptions struct {
	QueueName    string        `json:"queueName"`
	ExchangeName string        `json:"exchangeName"`
	RoutingKey   string        `json:"routingKey,omitempty"`
	NoWait       bool          `json:"noWait,omitempty"`
	Args         rmqamqp.Table `json:"args,omitempty"`
}

// UnbindQueueOptions holds parameters for removing a queue binding.
type UnbindQueueOptions struct {
	QueueName    string        `json:"queueName"`
	ExchangeName string        `json:"exchangeName"`
	RoutingKey   string        `json:"routingKey,omitempty"`
	Args         rmqamqp.Table `json:"args,omitempty"`
}

// DeclareQueue declares a queue on the broker. Satisfies k6common.QueueManager.
func (c *Client) DeclareQueue(opts any) error {
	o, err := convertOpts[DeclareQueueOptions]("DeclareQueue", opts)
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

	_, err = ch.QueueDeclare(o.Name, o.Durable, o.DeleteWhenUnused, o.Exclusive, o.NoWait, o.Args)
	return err
}

// DeleteQueue removes a queue from the broker.
func (c *Client) DeleteQueue(name string) error {
	if err := c.connect(c.connectionOptions); err != nil {
		return err
	}
	ch, err := c.amqpClient.Channel()
	if err != nil {
		return err
	}
	defer ch.Close() //nolint:errcheck

	_, err = ch.QueueDelete(name, false, false, false)
	return err
}

// BindQueue binds a queue to an exchange. Returns an empty string for interface
// compatibility with the AMQP 1.0 path-based binding model.
func (c *Client) BindQueue(opts any) (string, error) {
	o, err := convertOpts[BindQueueOptions]("BindQueue", opts)
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

	return "", ch.QueueBind(o.QueueName, o.RoutingKey, o.ExchangeName, o.NoWait, o.Args)
}

// UnbindQueue removes a queue binding. The bindingPath parameter is ignored for
// AMQP 0.9.1; pass a JSON-encoded UnbindQueueOptions instead.
//
// Note: for interface compatibility the parameter is a string (matching the
// AMQP 1.0 path-based model). AMQP 0.9.1 users should use UnbindQueueWithOptions.
func (c *Client) UnbindQueue(bindingPath string) error {
	_ = bindingPath
	return errNotSupported("UnbindQueue via binding path is not supported for AMQP 0.9.1; use UnbindQueueWithOptions")
}

// UnbindQueueWithOptions removes a queue binding using explicit parameters.
func (c *Client) UnbindQueueWithOptions(opts *UnbindQueueOptions) error {
	if err := c.connect(c.connectionOptions); err != nil {
		return err
	}
	ch, err := c.amqpClient.Channel()
	if err != nil {
		return err
	}
	defer ch.Close() //nolint:errcheck

	return ch.QueueUnbind(opts.QueueName, opts.RoutingKey, opts.ExchangeName, opts.Args)
}

// PurgeQueue removes all messages from a queue and returns the message count.
func (c *Client) PurgeQueue(name string) (int, error) {
	if err := c.connect(c.connectionOptions); err != nil {
		return 0, err
	}
	ch, err := c.amqpClient.Channel()
	if err != nil {
		return 0, err
	}
	defer ch.Close() //nolint:errcheck

	return ch.QueuePurge(name, false)
}

// InspectQueue returns metadata about a queue without modifying it.
func (c *Client) InspectQueue(name string) (any, error) {
	if err := c.connect(c.connectionOptions); err != nil {
		return nil, err
	}
	ch, err := c.amqpClient.Channel()
	if err != nil {
		return nil, err
	}
	defer ch.Close() //nolint:errcheck

	// QueueDeclarePassive performs a passive declare: it returns queue metadata
	// without creating or modifying the queue (equivalent to the deprecated QueueInspect).
	q, err := ch.QueueDeclarePassive(name, false, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	return q, nil
}
