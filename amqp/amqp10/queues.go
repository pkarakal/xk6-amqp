package amqp10

import (
	k6common "github.com/pkarakal/xk6-amqp/amqp/common"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

// DeclareQueueOptions holds parameters for declaring a queue via AMQP 1.0 management.
type DeclareQueueOptions struct {
	Name       string             `json:"name"`
	QueueType  k6common.QueueType `json:"queueType,omitempty"` // "classic" (default), "quorum", "stream"
	AutoDelete bool               `json:"autoDelete,omitempty"`
	Exclusive  bool               `json:"exclusive,omitempty"`
	Arguments  map[string]any     `json:"args,omitempty"`
}

// BindQueueOptions holds parameters for binding a queue to an exchange via AMQP 1.0.
type BindQueueOptions struct {
	SourceExchange   string         `json:"sourceExchange"`
	DestinationQueue string         `json:"destinationQueue"`
	BindingKey       string         `json:"bindingKey,omitempty"`
	Arguments        map[string]any `json:"args,omitempty"`
}

// DeclareQueue declares a queue on the broker. Satisfies k6common.QueueManager.
func (c *Client) DeclareQueue(opts any) error {
	o, err := convertOpts[DeclareQueueOptions]("DeclareQueue", opts)
	if err != nil {
		return err
	}
	if err := c.connect(); err != nil {
		return err
	}

	var spec rmq.IQueueSpecification
	switch o.QueueType {
	case k6common.QueueTypeQuorum:
		spec = &rmq.QuorumQueueSpecification{Name: o.Name}
	case k6common.QueueTypeStream:
		spec = &rmq.StreamQueueSpecification{Name: o.Name, Arguments: o.Arguments}
	default: // "classic" or empty
		spec = &rmq.ClassicQueueSpecification{
			Name:         o.Name,
			IsAutoDelete: o.AutoDelete,
			IsExclusive:  o.Exclusive,
		}
	}

	_, declErr := c.amqpConnection.Management().DeclareQueue(c.vu.Context(), spec)
	return declErr
}

// DeleteQueue removes a queue from the broker.
func (c *Client) DeleteQueue(name string) error {
	if err := c.connect(); err != nil {
		return err
	}
	return c.amqpConnection.Management().DeleteQueue(c.vu.Context(), name)
}

// BindQueue binds a queue to an exchange and returns the binding path.
// The returned path is required by UnbindQueue.
func (c *Client) BindQueue(opts any) (string, error) {
	o, err := convertOpts[BindQueueOptions]("BindQueue", opts)
	if err != nil {
		return "", err
	}
	if err := c.connect(); err != nil {
		return "", err
	}
	return c.amqpConnection.Management().Bind(c.vu.Context(), &rmq.ExchangeToQueueBindingSpecification{
		SourceExchange:   o.SourceExchange,
		DestinationQueue: o.DestinationQueue,
		BindingKey:       o.BindingKey,
		Arguments:        o.Arguments,
	})
}

// UnbindQueue removes a queue binding identified by its path (as returned by BindQueue).
func (c *Client) UnbindQueue(bindingPath string) error {
	if err := c.connect(); err != nil {
		return err
	}
	return c.amqpConnection.Management().Unbind(c.vu.Context(), bindingPath)
}

// PurgeQueue removes all messages from a queue and returns the purged count.
func (c *Client) PurgeQueue(name string) (int, error) {
	if err := c.connect(); err != nil {
		return 0, err
	}
	return c.amqpConnection.Management().PurgeQueue(c.vu.Context(), name)
}

// InspectQueue returns metadata about a queue.
func (c *Client) InspectQueue(name string) (any, error) {
	if err := c.connect(); err != nil {
		return nil, err
	}
	info, err := c.amqpConnection.Management().QueueInfo(c.vu.Context(), name)
	if err != nil {
		return nil, err
	}
	return info, nil
}
