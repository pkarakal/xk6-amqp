package amqp10

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertOpts_DirectPointer(t *testing.T) {
	t.Parallel()

	original := &DeclareQueueOptions{Name: "myqueue"}
	got, err := convertOpts[DeclareQueueOptions]("test", original)
	require.NoError(t, err)
	// Must be the same pointer — no copy made for direct *T inputs.
	assert.Same(t, original, got)
}

func TestConvertOpts_MapConversion(t *testing.T) {
	t.Parallel()

	m := map[string]any{"name": "q1", "autoDelete": true}
	got, err := convertOpts[DeclareQueueOptions]("test", m)
	require.NoError(t, err)
	assert.Equal(t, "q1", got.Name)
	assert.True(t, got.AutoDelete)
}

func TestConvertOpts_UnexpectedType(t *testing.T) {
	t.Parallel()

	_, err := convertOpts[DeclareQueueOptions]("MyMethod", 42)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MyMethod")
	assert.Contains(t, err.Error(), "int")
}

func TestConvertOpts_InvalidMarshal(t *testing.T) {
	t.Parallel()

	// A channel cannot be JSON-marshaled.
	m := map[string]any{"x": make(chan int)}
	_, err := convertOpts[DeclareQueueOptions]("test", m)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to marshal")
}
