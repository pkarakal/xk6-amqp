package amqp10

import (
	"fmt"

	k6common "github.com/grafana/xk6-amqp/amqp/common"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	k6jscommon "go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
)

// Compile-time assertion that *Client satisfies the AMQPClient interface.
var _ k6common.AMQPClient = (*Client)(nil)

// ConnectionOptions holds the parameters needed to connect to an AMQP 1.0 broker.
type ConnectionOptions struct {
	Host     string `json:"host,omitempty"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	Port     int    `json:"port,omitempty"`
}

// ToConnectionString formats the options as an amqp:// URL.
func (o *ConnectionOptions) ToConnectionString() string {
	if o == nil {
		return ""
	}
	return fmt.Sprintf("amqp://%s:%s@%s:%d", o.Username, o.Password, o.Host, o.Port)
}

// Client holds an AMQP 1.0 connection for a single VU.
type Client struct {
	vu                modules.VU
	amqpOptions       *rmq.AmqpConnOptions
	amqpConnection    *rmq.AmqpConnection
	connectionOptions *ConnectionOptions

}

// NewClient creates a new AMQP 1.0 client for the given VU.
func NewClient(vu modules.VU, opts *rmq.AmqpConnOptions, connOpts *ConnectionOptions) *Client {
	return &Client{vu: vu, amqpOptions: opts, connectionOptions: connOpts}
}

// connect establishes the client's connection to the AMQP 1.0 broker.
// It is a no-op if the connection is already established.
func (c *Client) connect() error {
	// A nil VU state indicates we are in the init context.
	// k6 must not perform IO in the init context.
	vuState := c.vu.State()
	if vuState == nil {
		return k6jscommon.NewInitContextError("connecting to an amqp server in the init context is not supported")
	}

	if c.amqpConnection != nil {
		return nil
	}

	if c.amqpOptions == nil {
		c.amqpOptions = &rmq.AmqpConnOptions{}
	}

	// Merge k6 TLS configuration. The AMQP 1.0 library does not expose a
	// custom dial hook, so only TLS config can be integrated (not netext.Dialer).
	if vuState.TLSConfig != nil {
		if c.amqpOptions.TLSConfig == nil {
			c.amqpOptions.TLSConfig = vuState.TLSConfig.Clone()
		} else {
			tlsCfg := c.amqpOptions.TLSConfig
			tlsCfg.InsecureSkipVerify = vuState.TLSConfig.InsecureSkipVerify
			tlsCfg.CipherSuites = vuState.TLSConfig.CipherSuites
			tlsCfg.MinVersion = vuState.TLSConfig.MinVersion
			tlsCfg.MaxVersion = vuState.TLSConfig.MaxVersion
			tlsCfg.Renegotiation = vuState.TLSConfig.Renegotiation
			tlsCfg.KeyLogWriter = vuState.TLSConfig.KeyLogWriter
			tlsCfg.Certificates = append(tlsCfg.Certificates, vuState.TLSConfig.Certificates...)
		}
	}

	conn, err := rmq.Dial(c.vu.Context(), c.connectionOptions.ToConnectionString(), c.amqpOptions)
	if err != nil {
		return err
	}
	c.amqpConnection = conn

	return nil
}

// Close closes the AMQP 1.0 connection.
func (c *Client) Close() error {
	if c.amqpConnection == nil {
		return nil
	}
	return c.amqpConnection.Close(c.vu.Context())
}
