package kabaka

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type MockLogger struct {
	mu   sync.Mutex
	logs []LogMessage
}

func (m *MockLogger) Info(msg *LogMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, *msg)
}

func (m *MockLogger) Error(msg *LogMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, *msg)
}

func (m *MockLogger) Debug(msg *LogMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, *msg)
}

func (m *MockLogger) Warn(msg *LogMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, *msg)
}

func TestLogger(t *testing.T) {
	mockLogger := &MockLogger{}
	topic := &Topic{
		Name: "test-topic",
	}

	handler := func(msg *Message) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	}

	subID := topic.subscribe(handler, mockLogger)

	err := topic.publish([]byte("test message"))
	require.NoError(t, err)

	time.Sleep(20 * time.Millisecond)

	require.Len(t, mockLogger.logs, 1)
	log := mockLogger.logs[0]

	require.Equal(t, "test-topic", log.TopicName)
	require.Equal(t, "test message", log.Message)
	require.Equal(t, Success, log.MessageStatus)
	require.Equal(t, subID, log.SubScriber)
	require.Equal(t, Consume, log.Action)
	require.True(t, log.SpendTime >= 10)
}

func TestErrorLogger(t *testing.T) {
	topic := &Topic{
		Name: "test-topic",
	}

	handler := func(msg *Message) error {
		return errors.New("test error")
	}

	mockLogger := &MockLogger{}
	subID := topic.subscribe(handler, mockLogger)

	err := topic.publish([]byte("******************"))
	require.NoError(t, err)

	time.Sleep(20 * time.Millisecond)

	require.Len(t, mockLogger.logs, 4)

	status := []MessageStatus{Retry, Retry, Retry, Error}

	for i, log := range mockLogger.logs {
		require.Equal(t, "test-topic", log.TopicName)
		require.Equal(t, "******************", log.Message)
		require.Equal(t, status[i], log.MessageStatus)
		require.Equal(t, subID, log.SubScriber)
		require.True(t, log.SpendTime >= 0)
	}
}
