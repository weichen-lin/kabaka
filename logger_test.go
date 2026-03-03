package kabaka

import (
	"bytes"
	"log"
	"os"
	"strings"
	"testing"
	"time"
)

// MockLogger for testing
type MockLogger struct {
	DebugCalls int
	InfoCalls  int
	WarnCalls  int
	ErrorCalls int
	LastLog    *LogMessage
}

func (m *MockLogger) Debug(args *LogMessage) {
	m.DebugCalls++
	m.LastLog = args
}

func (m *MockLogger) Info(args *LogMessage) {
	m.InfoCalls++
	m.LastLog = args
}

func (m *MockLogger) Warn(args *LogMessage) {
	m.WarnCalls++
	m.LastLog = args
}

func (m *MockLogger) Error(args *LogMessage) {
	m.ErrorCalls++
	m.LastLog = args
}

func TestMockLogger(t *testing.T) {
	logger := &MockLogger{}

	logMsg := &LogMessage{
		TopicName: "test-topic",
		Action:    Publish,
		MessageID: "msg-123",
		Message:   "test message",
		CreatedAt: time.Now(),
	}

	// Test Info
	logger.Info(logMsg)
	if logger.InfoCalls != 1 {
		t.Errorf("Expected 1 info call, got %d", logger.InfoCalls)
	}
	if logger.LastLog.TopicName != "test-topic" {
		t.Errorf("Expected test-topic, got %s", logger.LastLog.TopicName)
	}

	// Test Error
	logger.Error(logMsg)
	if logger.ErrorCalls != 1 {
		t.Errorf("Expected 1 error call, got %d", logger.ErrorCalls)
	}

	// Test Debug
	logger.Debug(logMsg)
	if logger.DebugCalls != 1 {
		t.Errorf("Expected 1 debug call, got %d", logger.DebugCalls)
	}

	// Test Warn
	logger.Warn(logMsg)
	if logger.WarnCalls != 1 {
		t.Errorf("Expected 1 warn call, got %d", logger.WarnCalls)
	}
}

func TestDefaultLogger_Info(t *testing.T) {
	var buf bytes.Buffer
	oldOutput := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(oldOutput)

	logger := &DefaultLogger{}
	logMsg := &LogMessage{
		TopicName:     "test-topic",
		Action:        Publish,
		MessageID:     "msg-123",
		Message:       "test message",
		MessageStatus: Success,
		SpendTime:     100,
		CreatedAt:     time.Now(),
	}

	logger.Info(logMsg)

	output := buf.String()
	if !strings.Contains(output, "[INFO]") {
		t.Error("Expected [INFO] in log output")
	}
	if !strings.Contains(output, "test-topic") {
		t.Error("Expected test-topic in log output")
	}
	if !strings.Contains(output, "test message") {
		t.Error("Expected test message in log output")
	}
}

func TestDefaultLogger_Error(t *testing.T) {
	var buf bytes.Buffer
	oldOutput := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(oldOutput)

	logger := &DefaultLogger{}
	logMsg := &LogMessage{
		TopicName:     "test-topic",
		Action:        Consume,
		MessageID:     "msg-456",
		Message:       "error occurred",
		MessageStatus: Error,
		SpendTime:     200,
		CreatedAt:     time.Now(),
	}

	logger.Error(logMsg)

	output := buf.String()
	if !strings.Contains(output, "[ERROR]") {
		t.Error("Expected [ERROR] in log output")
	}
	if !strings.Contains(output, "error occurred") {
		t.Error("Expected error occurred in log output")
	}
}

func TestDefaultLogger_Debug(t *testing.T) {
	var buf bytes.Buffer
	oldOutput := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(oldOutput)

	logger := &DefaultLogger{}
	logMsg := &LogMessage{
		TopicName: "test-topic",
		Action:    Subscribe,
		MessageID: "msg-789",
		Message:   "debug info",
		CreatedAt: time.Now(),
	}

	logger.Debug(logMsg)

	output := buf.String()
	if !strings.Contains(output, "[DEBUG]") {
		t.Error("Expected [DEBUG] in log output")
	}
	if !strings.Contains(output, "debug info") {
		t.Error("Expected debug info in log output")
	}
}

func TestDefaultLogger_Warn(t *testing.T) {
	var buf bytes.Buffer
	oldOutput := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(oldOutput)

	logger := &DefaultLogger{}
	logMsg := &LogMessage{
		TopicName:     "test-topic",
		Action:        Consume,
		MessageID:     "msg-999",
		Message:       "warning message",
		MessageStatus: Timeout,
		SpendTime:     5000,
		CreatedAt:     time.Now(),
	}

	logger.Warn(logMsg)

	output := buf.String()
	if !strings.Contains(output, "[WARN]") {
		t.Error("Expected [WARN] in log output")
	}
	if !strings.Contains(output, "warning message") {
		t.Error("Expected warning message in log output")
	}
}

func TestLogMessage_Actions(t *testing.T) {
	actions := []Action{Subscribe, Publish, Consume, Cancelled, WorkerStart}

	if Subscribe != "subscribe" {
		t.Errorf("Expected subscribe, got %s", Subscribe)
	}
	if Publish != "publish" {
		t.Errorf("Expected publish, got %s", Publish)
	}
	if Consume != "consume" {
		t.Errorf("Expected consume, got %s", Consume)
	}
	if Cancelled != "cancelled" {
		t.Errorf("Expected cancelled, got %s", Cancelled)
	}
	if WorkerStart != "worker_start" {
		t.Errorf("Expected worker_start, got %s", WorkerStart)
	}

	if len(actions) != 5 {
		t.Errorf("Expected 5 actions, got %d", len(actions))
	}
}

func TestLogMessage_Status(t *testing.T) {
	statuses := []MessageStatus{Success, Retry, Error, WorkerStartFailed, Timeout}

	if Success != "success" {
		t.Errorf("Expected success, got %s", Success)
	}
	if Retry != "retry" {
		t.Errorf("Expected retry, got %s", Retry)
	}
	if Error != "error" {
		t.Errorf("Expected error, got %s", Error)
	}
	if WorkerStartFailed != "worker_start_failed" {
		t.Errorf("Expected worker_start_failed, got %s", WorkerStartFailed)
	}
	if Timeout != "timeout" {
		t.Errorf("Expected timeout, got %s", Timeout)
	}

	if len(statuses) != 5 {
		t.Errorf("Expected 5 statuses, got %d", len(statuses))
	}
}

func init() {
	log.SetOutput(os.Stdout)
}
