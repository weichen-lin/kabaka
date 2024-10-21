package kabaka

import (
	"bytes"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
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

type mockLogWriter struct {
	buf bytes.Buffer
}

func (m *mockLogWriter) Write(p []byte) (n int, err error) {
	return m.buf.Write(p)
}

func TestDefaultLogger_Debug(t *testing.T) {
	mockWriter := &mockLogWriter{}
	log.SetOutput(mockWriter)

	logger := &DefaultLogger{}
	messageID := uuid.New()
	subscriberID := uuid.New()
	headers := map[string]string{
		"header1": "value1",
		"header2": "value2",
	}
	logMsg := &LogMessage{
		TopicName:     "test-topic",
		Action:        Publish,
		MessageID:     messageID,
		Message:       "Test message",
		MessageStatus: Success,
		SubScriber:    subscriberID,
		SpendTime:     100,
		CreatedAt:     time.Now(),
		Headers:       headers,
	}
	logger.Debug(logMsg)

	logOutput := mockWriter.buf.String()
	require.Contains(t, logOutput, "[DEBUG]")
	require.Contains(t, logOutput, "test-topic")
	require.Contains(t, logOutput, "Test message")
	require.Contains(t, logOutput, "publish")
	require.Contains(t, logOutput, "success")
	require.Contains(t, logOutput, subscriberID.String())
	require.Contains(t, logOutput, "100ms")
}

func TestDefaultLogger_Info(t *testing.T) {
	mockWriter := &mockLogWriter{}
	log.SetOutput(mockWriter)

	logger := &DefaultLogger{}
	messageID := uuid.New()
	subscriberID := uuid.New()
	headers := map[string]string{
		"header1": "value1",
		"header2": "value2",
	}
	logMsg := &LogMessage{
		TopicName:     "test-topic",
		Action:        Subscribe,
		MessageID:     messageID,
		Message:       "Info message",
		MessageStatus: Success,
		SubScriber:    subscriberID,
		SpendTime:     50,
		CreatedAt:     time.Now(),
		Headers:       headers,
	}

	logger.Info(logMsg)

	logOutput := mockWriter.buf.String()
	require.Contains(t, logOutput, "[INFO]")
	require.Contains(t, logOutput, "test-topic")
	require.Contains(t, logOutput, "Info message")
	require.Contains(t, logOutput, "subscribe")
	require.Contains(t, logOutput, "success")
	require.Contains(t, logOutput, subscriberID.String())
	require.Contains(t, logOutput, "50ms")
}

func TestDefaultLogger_Warn(t *testing.T) {
	mockWriter := &mockLogWriter{}
	log.SetOutput(mockWriter)

	logger := &DefaultLogger{}
	messageID := uuid.New()
	subscriberID := uuid.New()
	headers := map[string]string{
		"header1": "value1",
		"header2": "value2",
	}
	logMsg := &LogMessage{
		TopicName:     "error-topic",
		Action:        Cancelled,
		MessageID:     messageID,
		Message:       "Error occurred",
		MessageStatus: Error,
		SubScriber:    subscriberID,
		SpendTime:     200,
		CreatedAt:     time.Now(),
		Headers:       headers,
	}

	logger.Warn(logMsg)

	logOutput := mockWriter.buf.String()
	require.Contains(t, logOutput, "[WARN]")
	require.Contains(t, logOutput, "error-topic")
	require.Contains(t, logOutput, "Error occurred")
	require.Contains(t, logOutput, "cancelled")
	require.Contains(t, logOutput, "error")
	require.Contains(t, logOutput, subscriberID.String())
	require.Contains(t, logOutput, "200ms")
}

func TestDefaultLogger_Error(t *testing.T) {
	mockWriter := &mockLogWriter{}
	log.SetOutput(mockWriter)

	logger := &DefaultLogger{}
	messageID := uuid.New()
	subscriberID := uuid.New()
	headers := map[string]string{
		"header1": "value1",
		"header2": "value2",
	}
	logMsg := &LogMessage{
		TopicName:     "error-topic",
		Action:        Cancelled,
		MessageID:     messageID,
		Message:       "Error occurred",
		MessageStatus: Error,
		SubScriber:    subscriberID,
		SpendTime:     200,
		CreatedAt:     time.Now(),
		Headers:       headers,
	}

	logger.Error(logMsg)

	logOutput := mockWriter.buf.String()
	require.Contains(t, logOutput, "[ERROR]")
	require.Contains(t, logOutput, "error-topic")
	require.Contains(t, logOutput, "Error occurred")
	require.Contains(t, logOutput, "cancelled")
	require.Contains(t, logOutput, "error")
	require.Contains(t, logOutput, subscriberID.String())
	require.Contains(t, logOutput, "200ms")
}
