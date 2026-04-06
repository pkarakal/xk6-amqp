package amqp10

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPublishOptions_Name(t *testing.T) {
	t.Parallel()

	opts := &PublishOptions{}
	assert.Equal(t, "amqp/1.0", opts.Name())
}

func TestPublishOptions_DefaultFields(t *testing.T) {
	t.Parallel()

	opts := &PublishOptions{}
	assert.Empty(t, opts.QueueName)
	assert.Empty(t, opts.Exchange)
	assert.Empty(t, opts.RoutingKey)
	assert.Empty(t, opts.Body)
	assert.Empty(t, opts.ContentType)
	assert.False(t, opts.Persistent)
	assert.Empty(t, opts.Subject)
	assert.Empty(t, opts.CorrelationID)
	assert.Empty(t, opts.MessageID)
}

func TestPublishOptions_FieldAssignment(t *testing.T) {
	t.Parallel()

	opts := &PublishOptions{
		QueueName:     "q",
		Exchange:      "ex",
		RoutingKey:    "rk",
		Body:          []byte("payload"),
		ContentType:   "text/plain",
		Persistent:    true,
		Subject:       "subj",
		CorrelationID: "corr",
		MessageID:     "msg-id",
	}

	assert.Equal(t, "q", opts.QueueName)
	assert.Equal(t, "ex", opts.Exchange)
	assert.Equal(t, "rk", opts.RoutingKey)
	assert.Equal(t, []byte("payload"), opts.Body)
	assert.Equal(t, "text/plain", opts.ContentType)
	assert.True(t, opts.Persistent)
	assert.Equal(t, "subj", opts.Subject)
	assert.Equal(t, "corr", opts.CorrelationID)
	assert.Equal(t, "msg-id", opts.MessageID)
}
