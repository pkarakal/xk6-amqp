package amqp091_test

import (
	"context"
	"testing"
	"time"

	"github.com/pkarakal/xk6-amqp/amqp/amqp091"
	k6common "github.com/pkarakal/xk6-amqp/amqp/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tclog "github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"go.k6.io/k6/js/modulestest"
)

// setupRabbitMQ starts a RabbitMQ container and returns a connected Client.
// The container is terminated via t.Cleanup when the test ends.
//
// The returned client's VU uses a synchronous RegisterCallback so that consumer
// goroutines can deliver messages without a running event loop in tests.
func setupRabbitMQ(t *testing.T) *amqp091.Client {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ctr, err := rabbitmq.Run(ctx,
		"rabbitmq:4.2.2-management",
		testcontainers.WithLogger(tclog.TestLogger(t)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ctr.Terminate(ctx)) })

	host, err := ctr.Host(ctx)
	require.NoError(t, err)
	mappedPort, err := ctr.MappedPort(ctx, "5672")
	require.NoError(t, err)

	testRT := modulestest.NewRuntime(t)
	// Execute callbacks synchronously so consumer goroutines deliver messages
	// without needing a running event loop in tests.
	testRT.VU.RegisterCallbackField = func() func(f func() error) {
		return func(f func() error) { _ = f() }
	}
	newVUState(t, testRT)

	connOpts := &amqp091.ConnectionOptions{
		Host:     host,
		Port:     mappedPort.Int(),
		Username: "guest",
		Password: "guest",
	}
	return amqp091.NewClient(testRT.VU, nil, connOpts)
}

func TestIntegration_FullRoundTrip(t *testing.T) {
	client := setupRabbitMQ(t)
	t.Cleanup(func() { _ = client.Close() })

	const (
		exchangeName = "test-exchange"
		queueName    = "test-queue"
		routingKey   = "test-key"
		payload      = "hello-integration"
	)

	require.NoError(t, client.DeclareExchange(&amqp091.DeclareExchangeOptions{
		Name: exchangeName, Kind: "direct", Durable: false,
	}))
	require.NoError(t, client.DeclareQueue(&amqp091.DeclareQueueOptions{
		Name: queueName, Durable: false,
	}))
	_, err := client.BindQueue(&amqp091.BindQueueOptions{
		QueueName: queueName, ExchangeName: exchangeName, RoutingKey: routingKey,
	})
	require.NoError(t, err)

	received := make(chan string, 1)
	require.NoError(t, client.Listen(&amqp091.ListenOptions{
		QueueName: queueName,
		AutoAck:   true,
		Listener: func(msg *k6common.Message) error {
			received <- msg.Body
			return nil
		},
	}))

	require.NoError(t, client.Publish(&amqp091.PublishOptions{
		Exchange:   exchangeName,
		RoutingKey: routingKey,
		Body:       []byte(payload),
	}))

	select {
	case msg := <-received:
		assert.Equal(t, payload, msg)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestIntegration_QueueOps(t *testing.T) {
	client := setupRabbitMQ(t)
	t.Cleanup(func() { _ = client.Close() })

	const queueName = "ops-queue"

	require.NoError(t, client.DeclareQueue(&amqp091.DeclareQueueOptions{Name: queueName}))

	info, err := client.InspectQueue(queueName)
	require.NoError(t, err)
	require.NotNil(t, info)

	// Publish 3 messages via the default exchange (routing key = queue name).
	for range 3 {
		require.NoError(t, client.Publish(&amqp091.PublishOptions{
			RoutingKey: queueName,
			Body:       []byte("msg"),
		}))
	}

	// Give the broker a moment to receive all messages.
	time.Sleep(200 * time.Millisecond)

	count, err := client.PurgeQueue(queueName)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	require.NoError(t, client.DeleteQueue(queueName))
}

func TestIntegration_ExchangeOps(t *testing.T) {
	client := setupRabbitMQ(t)
	t.Cleanup(func() { _ = client.Close() })

	const (
		srcExchange = "src-exchange"
		dstExchange = "dst-exchange"
		routingKey  = "rk"
	)

	require.NoError(t, client.DeclareExchange(&amqp091.DeclareExchangeOptions{
		Name: srcExchange, Kind: "direct",
	}))
	require.NoError(t, client.DeclareExchange(&amqp091.DeclareExchangeOptions{
		Name: dstExchange, Kind: "direct",
	}))

	_, err := client.BindExchange(&amqp091.BindExchangeOptions{
		Source: srcExchange, Destination: dstExchange, RoutingKey: routingKey,
	})
	require.NoError(t, err)

	require.NoError(t, client.UnbindExchangeWithOptions(&amqp091.UnbindExchangeOptions{
		Source: srcExchange, Destination: dstExchange, RoutingKey: routingKey,
	}))

	require.NoError(t, client.DeleteExchange(srcExchange))
	require.NoError(t, client.DeleteExchange(dstExchange))
}

func TestIntegration_BindUnbindQueue(t *testing.T) {
	client := setupRabbitMQ(t)
	t.Cleanup(func() { _ = client.Close() })

	const (
		exchangeName = "bind-test-exchange"
		queueName    = "bind-test-queue"
		routingKey   = "rk"
	)

	require.NoError(t, client.DeclareExchange(&amqp091.DeclareExchangeOptions{
		Name: exchangeName, Kind: "direct",
	}))
	require.NoError(t, client.DeclareQueue(&amqp091.DeclareQueueOptions{Name: queueName}))

	_, err := client.BindQueue(&amqp091.BindQueueOptions{
		QueueName: queueName, ExchangeName: exchangeName, RoutingKey: routingKey,
	})
	require.NoError(t, err)

	require.NoError(t, client.UnbindQueueWithOptions(&amqp091.UnbindQueueOptions{
		QueueName:    queueName,
		ExchangeName: exchangeName,
		RoutingKey:   routingKey,
	}))

	// UnbindQueue(path) is not supported for AMQP 0.9.1 — must return errNotSupported.
	err = client.UnbindQueue("some-path")
	require.Error(t, err)

	require.NoError(t, client.DeleteQueue(queueName))
	require.NoError(t, client.DeleteExchange(exchangeName))
}

func TestIntegration_Close(t *testing.T) {
	client := setupRabbitMQ(t)

	// Establish connection.
	require.NoError(t, client.DeclareQueue(&amqp091.DeclareQueueOptions{Name: "close-test-q"}))
	require.NoError(t, client.Close())
}
