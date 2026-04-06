package amqp091

import (
	"encoding/json"
	"testing"
	"time"

	rmqamqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestToAMQPPublishing_FieldMapping(t *testing.T) {
	t.Parallel()

	opts := &PublishOptions{
		ContentType:   "text/plain",
		Body:          []byte("hello"),
		CorrelationID: "corr-1",
		ReplyTo:       "reply-q",
		Expiration:    "60000",
		MessageID:     "msg-1",
		Type:          "mytype",
		UserID:        "user1",
		AppID:         "app1",
		Headers:       rmqamqp.Table{"x-custom": "value"},
	}

	p := opts.toAMQPPublishing()

	assert.Equal(t, opts.ContentType, p.ContentType)
	assert.Equal(t, opts.Body, p.Body)
	assert.Equal(t, opts.CorrelationID, p.CorrelationId)
	assert.Equal(t, opts.ReplyTo, p.ReplyTo)
	assert.Equal(t, opts.Expiration, p.Expiration)
	assert.Equal(t, opts.MessageID, p.MessageId)
	assert.Equal(t, opts.Type, p.Type)
	assert.Equal(t, opts.UserID, p.UserId)
	assert.Equal(t, opts.AppID, p.AppId)
	assert.Equal(t, rmqamqp.Table{"x-custom": "value"}, p.Headers)
}

func TestToAMQPPublishing_PersistentMode(t *testing.T) {
	t.Parallel()

	t.Run("Persistent", func(t *testing.T) {
		t.Parallel()
		p := (&PublishOptions{Persistent: true}).toAMQPPublishing()
		assert.Equal(t, rmqamqp.Persistent, p.DeliveryMode)
	})

	t.Run("NonPersistent", func(t *testing.T) {
		t.Parallel()
		p := (&PublishOptions{Persistent: false}).toAMQPPublishing()
		assert.Equal(t, uint8(0), p.DeliveryMode)
	})
}

func TestToAMQPPublishing_Timestamp(t *testing.T) {
	t.Parallel()

	t.Run("NonZero", func(t *testing.T) {
		t.Parallel()
		const epoch = int64(1700000000)
		p := (&PublishOptions{Timestamp: epoch}).toAMQPPublishing()
		assert.True(t, p.Timestamp.Equal(time.Unix(epoch, 0)))
	})

	t.Run("Zero", func(t *testing.T) {
		t.Parallel()
		p := (&PublishOptions{Timestamp: 0}).toAMQPPublishing()
		assert.True(t, p.Timestamp.IsZero())
	})
}

func TestMsgpackEncoding(t *testing.T) {
	t.Parallel()

	// Build a JSON body that mimics what JS would pass.
	jsonBody := []byte(`{"key":"value","num":42}`)

	// Re-encode as msgpack the same way Publish() does internally.
	var parsed any
	require.NoError(t, json.Unmarshal(jsonBody, &parsed))

	encoded, err := msgpack.Marshal(parsed)
	require.NoError(t, err)
	assert.NotEmpty(t, encoded)

	// Round-trip back to verify the content.
	var decoded map[string]any
	require.NoError(t, msgpack.Unmarshal(encoded, &decoded))
	assert.Equal(t, "value", decoded["key"])
	assert.EqualValues(t, 42, decoded["num"])
}
