package amqp091

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"

	k6common "github.com/pkarakal/xk6-amqp/amqp/common"
	rmqamqp "github.com/rabbitmq/amqp091-go"
	k6jscommon "go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/lib"
)

// Compile-time assertion that *Client satisfies the AMQPClient interface.
var _ k6common.AMQPClient = (*Client)(nil)

// ConnectionOptions holds the parameters needed to connect to an AMQP broker.
type ConnectionOptions = k6common.BaseConnectionOptions

// Client holds an AMQP 0.9.1 connection and channel for a single VU.
type Client struct {
	vu                modules.VU
	amqpOptions       *rmqamqp.Config
	amqpClient        *rmqamqp.Connection
	amqpChannel       *rmqamqp.Channel
	connectionOptions *ConnectionOptions

	// consumerCtx/consumerCancel live for the client lifetime (not a single
	// iteration). Canceled in Close() so all consumer goroutines exit cleanly.
	consumerCtx    context.Context
	consumerCancel context.CancelFunc
}

// NewClient creates a new AMQP 0.9.1 client for the given VU.
func NewClient(vu modules.VU, options *rmqamqp.Config, connOpts *ConnectionOptions) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		vu:                vu,
		amqpOptions:       options,
		connectionOptions: connOpts,
		consumerCtx:       ctx,
		consumerCancel:    cancel,
	}
}

// connect establishes the client's connection to the AMQP broker.
// It is a no-op if the connection is already established.
func (c *Client) connect(options *ConnectionOptions) error {
	// A nil VU state indicates we are in the init context.
	// k6 must not perform IO in the init context.
	vuState := c.vu.State()
	if vuState == nil {
		return k6jscommon.NewInitContextError("connecting to an amqp server in the init context is not supported")
	}

	if c.amqpClient != nil {
		return nil
	}

	if c.amqpOptions == nil {
		c.amqpOptions = &rmqamqp.Config{}
	}

	tlsCfg := c.amqpOptions.TLSClientConfig
	if tlsCfg != nil && vuState.TLSConfig != nil {
		tlsCfg.InsecureSkipVerify = vuState.TLSConfig.InsecureSkipVerify
		tlsCfg.CipherSuites = vuState.TLSConfig.CipherSuites
		tlsCfg.MinVersion = vuState.TLSConfig.MinVersion
		tlsCfg.MaxVersion = vuState.TLSConfig.MaxVersion
		tlsCfg.Renegotiation = vuState.TLSConfig.Renegotiation
		tlsCfg.KeyLogWriter = vuState.TLSConfig.KeyLogWriter
		tlsCfg.Certificates = append(tlsCfg.Certificates, vuState.TLSConfig.Certificates...)
		c.amqpOptions.Dial = c.upgradeDialerToTLS(vuState.Dialer, tlsCfg)
	} else {
		c.amqpOptions.Dial = c.plainTCPDialer(vuState.Dialer)
	}

	client, err := rmqamqp.DialConfig(options.ToConnectionString(), *c.amqpOptions)
	if err != nil {
		return err
	}
	c.amqpClient = client

	ch, err := c.amqpClient.Channel()
	if err != nil {
		return err
	}
	c.amqpChannel = ch

	return nil
}

// dialContextFunc is a function used to dial a TCP connection.
type dialContextFunc func(network, addr string) (net.Conn, error)

// upgradeDialerToTLS returns a dial function that uses k6's netext.Dialer
// and then upgrades the connection to TLS.
func (c *Client) upgradeDialerToTLS(dialer lib.DialContexter, config *tls.Config) dialContextFunc {
	ctx := context.Background()
	return func(network string, addr string) (net.Conn, error) {
		rawConn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}

		tlsConn := tls.Client(rawConn, config)
		if err = tlsConn.HandshakeContext(ctx); err != nil {
			if closeErr := rawConn.Close(); closeErr != nil {
				return nil, fmt.Errorf("failed to close connection after TLS handshake error: %w", closeErr)
			}
			return nil, err
		}
		return tlsConn, nil
	}
}

// plainTCPDialer returns a dial function backed by k6's netext.Dialer.
func (c *Client) plainTCPDialer(dialer lib.DialContexter) dialContextFunc {
	ctx := context.Background()
	return func(network string, addr string) (net.Conn, error) {
		return dialer.DialContext(ctx, network, addr)
	}
}

// Close stops all consumer goroutines and closes the AMQP channel and connection.
func (c *Client) Close() error {
	c.consumerCancel()
	var err1, err2 error
	if c.amqpChannel != nil {
		err1 = c.amqpChannel.Close()
	}
	if c.amqpClient != nil {
		err2 = c.amqpClient.Close()
	}
	return errors.Join(err1, err2)
}
