package kabaka

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestLogMessage(t *testing.T) {
	now := time.Now()
	messageID := uuid.New()
	subscriberID := uuid.New()

	logMsg := LogMessage{
		TopicName:     "TestTopic",
		Action:        Publish,
		MessageID:     messageID,
		Message:       "Test message",
		MessageStatus: Success,
		SubScriber:    subscriberID,
		SpendTime:     100,
		CreatedAt:     now,
	}

	require.Equal(t, "TestTopic", logMsg.TopicName)
	require.Equal(t, Publish, logMsg.Action)
	require.Equal(t, messageID, logMsg.MessageID)
	require.Equal(t, "Test message", logMsg.Message)
	require.Equal(t, Success, logMsg.MessageStatus)
	require.Equal(t, subscriberID, logMsg.SubScriber)
	require.Equal(t, int64(100), logMsg.SpendTime)
	require.Equal(t, now, logMsg.CreatedAt)
}

func TestConfig(t *testing.T) {
	mockLogger := &MockLogger{}
	config := Config{
		logger: mockLogger,
	}

	require.Equal(t, mockLogger, config.logger)
}

func TestOptions(t *testing.T) {
	options := Options{
		Name:       "TestOptions",
		BufferSize: 50,
	}

	require.Equal(t, "TestOptions", options.Name)
	require.Equal(t, 50, options.BufferSize)
}

func TestNewOptions(t *testing.T) {
	options := NewOptions("TestNewOptions")

	require.Equal(t, "TestNewOptions", options.Name)
	require.Equal(t, 24, options.BufferSize)
}
