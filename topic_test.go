package kabaka

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewTopic(t *testing.T) {
	options := &Options{
		MaxWorkers:     5,
		BufferSize:     10,
		MaxRetries:     3,
		RetryDelay:     time.Second,
		ProcessTimeout: time.Second * 5,
		Logger:         &DefaultLogger{},
		Tracer:         &DefaultTracer{},
	}

	topic := NewTopic("test-topic", options, func(msg *Message) error {
		return nil
	})
	require.NotNil(t, topic)
	require.Equal(t, "test-topic", topic.Name)
	require.Equal(t, options.MaxWorkers, topic.maxWorkers)
	require.Equal(t, options.BufferSize, topic.bufferSize)
}

func TestPublishMessage(t *testing.T) {
	options := getDefaultOptions()

	options.BufferSize = 1
	options.MaxWorkers = 1

	topic := NewTopic("test-topic", options, func(msg *Message) error {
		time.Sleep(6 * time.Second)
		return nil
	})
	defer topic.Stop()

	require.Equal(t, topic.bufferSize, 1)
	require.Equal(t, topic.maxWorkers, 1)

	msg := []byte("test message")
	err := topic.Publish(msg)
	require.NoError(t, err)
	time.Sleep(1 * time.Second)

	err = topic.Publish(msg)
	require.NoError(t, err)
	time.Sleep(1 * time.Second)

	err = topic.Publish(msg)
	fmt.Printf("err: %v\n", err)
	time.Sleep(1 * time.Second)

	err = topic.Publish(msg)
	require.ErrorIs(t, err, ErrPublishTimeout)

	fmt.Printf("topic max workers: %d, buffered size: %d\n", topic.maxWorkers, topic.bufferSize)
	fmt.Printf("active workers: %d\n", topic.GetActiveWorkers())
	fmt.Printf("busy workers: %d\n", topic.GetBusyWorkers())
	fmt.Printf("on going jobs: %d\n", topic.GetOnGoingJobs())
}

func TestWorkerMetrics(t *testing.T) {
	options := &Options{
		MaxWorkers:     2,
		BufferSize:     5,
		MaxRetries:     3,
		RetryDelay:     time.Second,
		ProcessTimeout: time.Second * 5,
		Logger:         &DefaultLogger{},
	}

	topic := NewTopic("test-topic", options, func(msg *Message) error {
		time.Sleep(1 * time.Second)
		return nil
	})

	require.Equal(t, int32(2), topic.GetActiveWorkers())
	require.Equal(t, int32(0), topic.GetBusyWorkers())
	require.Equal(t, int32(0), topic.GetOnGoingJobs())
	topic.Publish([]byte("test message"))

	time.Sleep(100 * time.Millisecond)
	fmt.Printf("active workers: %d\n", topic.GetActiveWorkers())
	fmt.Printf("busy workers: %d\n", topic.GetBusyWorkers())
	fmt.Printf("on going jobs: %d\n", topic.GetOnGoingJobs())
}
