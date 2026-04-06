package amqp_test

import (
	"testing"

	amqpmod "github.com/grafana/xk6-amqp/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/js/modulestest"
)

// newModuleRuntime registers the AMQP 0.9.1 module exports directly into the
// sobek runtime so tests can invoke the JS constructor without needing the full
// k6 module system (which requires internal compiler access).
func newModuleRuntime(t *testing.T) *modulestest.Runtime {
	t.Helper()
	testRT := modulestest.NewRuntime(t)
	rt := testRT.VU.Runtime()

	root := amqpmod.New()
	mi := root.NewModuleInstance(testRT.VU)
	exports := mi.Exports()

	// Expose Named exports as a JS object so tests can do `new amqp.Client(...)`.
	obj := rt.NewObject()
	for name, val := range exports.Named {
		require.NoError(t, obj.Set(name, val))
	}
	require.NoError(t, rt.Set("amqp", obj))

	return testRT
}

func TestNewClient_MissingArgs(t *testing.T) {
	t.Parallel()

	testRT := newModuleRuntime(t)
	rt := testRT.VU.Runtime()

	_, err := rt.RunString(`new amqp.Client()`)
	assert.Error(t, err, "Client() with zero arguments should throw")
}

func TestNewClient_UnknownField(t *testing.T) {
	t.Parallel()

	testRT := newModuleRuntime(t)
	rt := testRT.VU.Runtime()

	_, err := rt.RunString(`new amqp.Client({ connectionOptions: {}, bogus: 1 })`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bogus")
}

func TestNewClient_ValidOptions(t *testing.T) {
	t.Parallel()

	testRT := newModuleRuntime(t)
	rt := testRT.VU.Runtime()

	val, err := rt.RunString(`
		var c = new amqp.Client({
			connectionOptions: { host: 'localhost', port: 5672, username: 'guest', password: 'guest' }
		});
		typeof c.publish === 'function' &&
		typeof c.listen === 'function' &&
		typeof c.declareQueue === 'function' &&
		typeof c.deleteQueue === 'function'
	`)
	require.NoError(t, err)
	assert.Equal(t, true, val.Export())
}

func TestReadListenOptions_NoListener(t *testing.T) {
	t.Parallel()

	testRT := newModuleRuntime(t)
	rt := testRT.VU.Runtime()

	// Create a client first, then call listen without a listener key.
	// The option parsing error is raised before any connect() call.
	_, err := rt.RunString(`
		var c = new amqp.Client({
			connectionOptions: { host: 'localhost', port: 5672, username: 'guest', password: 'guest' }
		});
		c.listen({ queueName: 'q' })
	`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "listener")
}

func TestReadListenOptions_NotAFunction(t *testing.T) {
	t.Parallel()

	testRT := newModuleRuntime(t)
	rt := testRT.VU.Runtime()

	_, err := rt.RunString(`
		var c = new amqp.Client({
			connectionOptions: { host: 'localhost', port: 5672, username: 'guest', password: 'guest' }
		});
		c.listen({ queueName: 'q', listener: 42 })
	`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "function")
}

func TestReadOptions_DisallowsUnknownFields(t *testing.T) {
	t.Parallel()

	testRT := newModuleRuntime(t)
	rt := testRT.VU.Runtime()

	_, err := rt.RunString(`new amqp.Client({ connectionOptions: {}, unknownKey: true })`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknownKey")
}
