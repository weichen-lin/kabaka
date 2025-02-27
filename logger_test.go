package kabaka

import (
	"bytes"
	"log"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestDefaultLogger(t *testing.T) {
	logger := &DefaultLogger{}

	logMessage := &LogMessage{
		TopicName:     "test-topic",
		Action:        Subscribe,
		MessageID:     uuid.New(),
		Message:       "Test message",
		MessageStatus: Success,
		SpendTime:     150,
		CreatedAt:     time.Now(),
		Headers:       map[string]string{"key": "value", "key2": "value2", "key3": "value3"},
	}

	// 攔截 log 輸出
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(nil)

	t.Run("Debug", func(t *testing.T) {
		logger.Debug(logMessage)
		require.Contains(t, buf.String(), "[DEBUG]")
		require.Contains(t, buf.String(), logMessage.TopicName)
		require.Contains(t, buf.String(), logMessage.Message)
		buf.Reset()
	})

	t.Run("Info", func(t *testing.T) {
		logger.Info(logMessage)
		require.Contains(t, buf.String(), "[INFO]")
		require.Contains(t, buf.String(), logMessage.TopicName)
		require.Contains(t, buf.String(), logMessage.Message)
		buf.Reset()
	})

	t.Run("Warn", func(t *testing.T) {
		logger.Warn(logMessage)
		require.Contains(t, buf.String(), "[WARN]")
		require.Contains(t, buf.String(), logMessage.TopicName)
		require.Contains(t, buf.String(), logMessage.Message)
		buf.Reset()
	})

	t.Run("Error", func(t *testing.T) {
		logger.Error(logMessage)
		require.Contains(t, buf.String(), "[ERROR]")
		require.Contains(t, buf.String(), logMessage.TopicName)
		require.Contains(t, buf.String(), logMessage.Message)
	})
}
