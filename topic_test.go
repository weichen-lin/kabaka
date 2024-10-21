package kabaka

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewTopic(t *testing.T) {
	topic := NewTopic("test-new-topic", &Options{
		BufferSize:            24,
		DefaultMaxRetries:     3,
		DefaultRetryDelay:     time.Duration(5 * time.Second),
		DefaultProcessTimeout: time.Duration(10 * time.Second),
		Logger:                &MockLogger{},
	})

	require.NotNil(t, topic)
	require.Equal(t, "test-new-topic", topic.Name)
	require.NotNil(t, topic.subscribers)
	require.NotNil(t, topic.activeSubscribers)
	require.Equal(t, 24, topic.bufferSize)
	require.Equal(t, 3, topic.maxRetries)
	require.Equal(t, time.Duration(5*time.Second), topic.retryDelay)
	require.Equal(t, time.Duration(10*time.Second), topic.processTimeout)
}

func TestConsumeSuccess(t *testing.T) {
	topic := NewTopic("test-consume-success", &Options{
		BufferSize:            24,
		DefaultMaxRetries:     3,
		DefaultRetryDelay:     time.Duration(50 * time.Millisecond),
		DefaultProcessTimeout: time.Duration(10 * time.Second),
	})

	handler := func(msg *Message) error {
		return nil
	}

	subId := topic.subscribe(handler)
	require.NotEmpty(t, subId)

	msg := topic.generateTraceMessage("test-consume-success", []byte("test"), nil)

	err := topic.publish(msg)
	require.NoError(t, err)

	time.Sleep(1 * time.Second)
}

func TestConsumeError(t *testing.T) {
	topic := NewTopic("test-consume-error", &Options{
		BufferSize:            24,
		DefaultMaxRetries:     3,
		DefaultRetryDelay:     time.Duration(50 * time.Millisecond),
		DefaultProcessTimeout: time.Duration(10 * time.Second),
	})

	handler := func(msg *Message) error {
		time.Sleep(10 * time.Millisecond)
		return errors.New("error")
	}

	subId := topic.subscribe(handler)
	require.NotEmpty(t, subId)

	msg := topic.generateTraceMessage("test-consume-error", []byte("test"), nil)

	err := topic.publish(msg)
	require.NoError(t, err)

	time.Sleep(10 * time.Second)
}
