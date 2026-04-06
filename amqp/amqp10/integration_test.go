package amqp10_test

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/xk6-amqp/amqp/amqp10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tclog "github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"go.k6.io/k6/js/modulestest"
)

// setupRabbitMQ starts a RabbitMQ container and returns a connected AMQP 1.0 Client.
// The container is terminated via t.Cleanup when the test ends.
//
// The returned client's VU uses a synchronous RegisterCallback so that consumer
// goroutines can deliver messages without a running event loop in tests.
func setupRabbitMQ(t *testing.T) *amqp10.Client {
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

	connOpts := &amqp10.ConnectionOptions{
		Host:     host,
		Port:     mappedPort.Int(),
		Username: "guest",
		Password: "guest",
	}
	return amqp10.NewClient(testRT.VU, nil, connOpts)
}

func TestIntegration10_FullRoundTrip(t *testing.T) {
	client := setupRabbitMQ(t)
	t.Cleanup(func() { _ = client.Close() })

	const (
		exchangeName = "test10-exchange"
		queueName    = "test10-queue"
		routingKey   = "test10-key"
		payload      = "hello-amqp10"
	)

	require.NoError(t, client.DeclareExchange(&amqp10.DeclareExchangeOptions{
		Name: exchangeName, Kind: "direct",
	}))
	require.NoError(t, client.DeclareQueue(&amqp10.DeclareQueueOptions{
		Name: queueName,
	}))
	_, err := client.BindQueue(&amqp10.BindQueueOptions{
		SourceExchange:   exchangeName,
		DestinationQueue: queueName,
		BindingKey:       routingKey,
	})
	require.NoError(t, err)

	received := make(chan string, 1)
	require.NoError(t, client.Listen(&amqp10.ListenOptions{
		QueueName: queueName,
		AutoAck:   true,
		Listener: func(msg string) error {
			received <- msg
			return nil
		},
	}))

	require.NoError(t, client.Publish(&amqp10.PublishOptions{
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

func TestIntegration10_QuorumQueue(t *testing.T) {
	client := setupRabbitMQ(t)
	t.Cleanup(func() { _ = client.Close() })

	const queueName = "quorum-queue"

	require.NoError(t, client.DeclareQueue(&amqp10.DeclareQueueOptions{
		Name:      queueName,
		QueueType: "quorum",
	}))

	received := make(chan string, 1)
	require.NoError(t, client.Listen(&amqp10.ListenOptions{
		QueueName: queueName,
		AutoAck:   true,
		Listener: func(msg string) error {
			received <- msg
			return nil
		},
	}))

	require.NoError(t, client.Publish(&amqp10.PublishOptions{
		QueueName: queueName,
		Body:      []byte("quorum-msg"),
	}))

	select {
	case msg := <-received:
		assert.Equal(t, "quorum-msg", msg)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	require.NoError(t, client.DeleteQueue(queueName))
}

func TestIntegration10_ExchangeOps(t *testing.T) {
	client := setupRabbitMQ(t)
	t.Cleanup(func() { _ = client.Close() })

	const (
		srcExchange = "src10-exchange"
		dstExchange = "dst10-exchange"
		routingKey  = "rk"
	)

	require.NoError(t, client.DeclareExchange(&amqp10.DeclareExchangeOptions{
		Name: srcExchange, Kind: "direct",
	}))
	require.NoError(t, client.DeclareExchange(&amqp10.DeclareExchangeOptions{
		Name: dstExchange, Kind: "direct",
	}))

	bindingPath, err := client.BindExchange(&amqp10.BindExchangeOptions{
		SourceExchange:      srcExchange,
		DestinationExchange: dstExchange,
		BindingKey:          routingKey,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, bindingPath)

	require.NoError(t, client.UnbindExchange(bindingPath))

	require.NoError(t, client.DeleteExchange(srcExchange))
	require.NoError(t, client.DeleteExchange(dstExchange))
}

func TestIntegration10_PurgeQueue(t *testing.T) {
	client := setupRabbitMQ(t)
	t.Cleanup(func() { _ = client.Close() })

	const queueName = "purge10-queue"

	require.NoError(t, client.DeclareQueue(&amqp10.DeclareQueueOptions{Name: queueName}))

	for range 3 {
		require.NoError(t, client.Publish(&amqp10.PublishOptions{
			QueueName: queueName,
			Body:      []byte("msg"),
		}))
	}

	time.Sleep(200 * time.Millisecond)

	count, err := client.PurgeQueue(queueName)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	require.NoError(t, client.DeleteQueue(queueName))
}

func TestIntegration10_Close(t *testing.T) {
	client := setupRabbitMQ(t)

	require.NoError(t, client.DeclareQueue(&amqp10.DeclareQueueOptions{Name: "close10-test-q"}))
	require.NoError(t, client.Close())
}
