package amqp091_test

import (
	"errors"
	"testing"

	"github.com/pkarakal/xk6-amqp/amqp/amqp091"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k6jscommon "go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modulestest"
)

func TestConnectionOptions_ToConnectionString(t *testing.T) {
	t.Parallel()

	t.Run("NilReceiver", func(t *testing.T) {
		t.Parallel()
		var o *amqp091.ConnectionOptions
		assert.Equal(t, "", o.ToConnectionString())
	})

	t.Run("Empty", func(t *testing.T) {
		t.Parallel()
		o := &amqp091.ConnectionOptions{}
		assert.Equal(t, "amqp://:@:0", o.ToConnectionString())
	})

	t.Run("Full", func(t *testing.T) {
		t.Parallel()
		o := &amqp091.ConnectionOptions{
			Host:     "rabbit",
			Username: "user",
			Password: "pass",
			Port:     5672,
		}
		assert.Equal(t, "amqp://user:pass@rabbit:5672", o.ToConnectionString())
	})
}

func TestNewClient_InitializesContext(t *testing.T) {
	t.Parallel()

	testRT := modulestest.NewRuntime(t)
	client := amqp091.NewClient(testRT.VU, nil, nil)
	require.NotNil(t, client)

	// Close on an unconnected client must not error.
	require.NoError(t, client.Close())
}

func TestConnect_InitContextError(t *testing.T) {
	t.Parallel()

	// modulestest.NewRuntime starts in init context (StateField is nil).
	testRT := modulestest.NewRuntime(t)
	client := amqp091.NewClient(testRT.VU, nil, &amqp091.ConnectionOptions{
		Host: "localhost", Port: 5672,
	})

	// Any operation that calls connect() must fail with InitContextError.
	err := client.DeclareQueue(&amqp091.DeclareQueueOptions{Name: "test"})
	require.Error(t, err)

	var iceErr k6jscommon.InitContextError
	assert.True(t, errors.As(err, &iceErr), "expected InitContextError, got: %v", err)
}
