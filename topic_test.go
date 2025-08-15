package kabaka

import (
	"context"
	"sync"
	"testing"
	"time"
)

// MockLogger is a mock implementation of the Logger interface for testing.
type MockLogger struct {
	DebugCalled bool
	InfoCalled  bool
	WarnCalled  bool
	ErrorCalled bool
	LastMessage *LogMessage
}

func (m *MockLogger) Debug(args *LogMessage) {
	m.DebugCalled = true
	m.LastMessage = args
}

func (m *MockLogger) Info(args *LogMessage) {
	m.InfoCalled = true
	m.LastMessage = args
}

func (m *MockLogger) Warn(args *LogMessage) {
	m.WarnCalled = true
	m.LastMessage = args
}

func (m *MockLogger) Error(args *LogMessage) {
	m.ErrorCalled = true
	m.LastMessage = args
}

// TestNewTopic_DefaultOptions verifies that a new topic is created with correct default values.
func TestNewTopic_DefaultOptions(t *testing.T) {
	handler := func(ctx context.Context, msg *Message) error { return nil }
	topic := newTopic("test-defaults", handler)
	defer topic.stop()

	if topic.Name != "test-defaults" {
		t.Errorf("expected topic name to be 'test-defaults', got '%s'", topic.Name)
	}
	if topic.maxWorkers != 20 {
		t.Errorf("expected default maxWorkers to be 20, got %d", topic.maxWorkers)
	}
	if topic.bufferSize != 24 {
		t.Errorf("expected default bufferSize to be 24, got %d", topic.bufferSize)
	}
	if topic.maxRetries != 3 {
		t.Errorf("expected default maxRetries to be 3, got %d", topic.maxRetries)
	}
	if topic.retryDelay != 5*time.Second {
		t.Errorf("expected default retryDelay to be 5s, got %v", topic.retryDelay)
	}
	if topic.processTimeout != 10*time.Second {
		t.Errorf("expected default processTimeout to be 10s, got %v", topic.processTimeout)
	}
	if topic.publishTimeout != 2*time.Second {
		t.Errorf("expected default publishTimeout to be 2s, got %v", topic.publishTimeout)
	}
	if topic.handler == nil {
		t.Error("handler should not be nil")
	}
}

// TestNewTopic_WithOptions verifies that custom options are applied correctly.
func TestNewTopic_WithOptions(t *testing.T) {
	handler := func(ctx context.Context, msg *Message) error { return nil }
	logger := &MockLogger{}
	topic := newTopic("test-options", handler,
		WithMaxWorkers(10),
		WithBufferSize(50),
		WithMaxRetries(5),
		WithRetryDelay(10*time.Second),
		WithProcessTimeout(20*time.Second),
		WithPublishTimeout(5*time.Second),
		WithLogger(logger),
	)
	defer topic.stop()

	if topic.maxWorkers != 10 {
		t.Errorf("expected maxWorkers to be 10, got %d", topic.maxWorkers)
	}
	if topic.bufferSize != 50 {
		t.Errorf("expected bufferSize to be 50, got %d", topic.bufferSize)
	}
	if topic.maxRetries != 5 {
		t.Errorf("expected maxRetries to be 5, got %d", topic.maxRetries)
	}
	if topic.retryDelay != 10*time.Second {
		t.Errorf("expected retryDelay to be 10s, got %v", topic.retryDelay)
	}
	if topic.processTimeout != 20*time.Second {
		t.Errorf("expected processTimeout to be 20s, got %v", topic.processTimeout)
	}
	if topic.publishTimeout != 5*time.Second {
		t.Errorf("expected publishTimeout to be 5s, got %v", topic.publishTimeout)
	}
	if topic.logger != logger {
		t.Error("custom logger was not set correctly")
	}
}

// TestTopic_Publish_Success verifies that a message is successfully published to the queue.
func TestTopic_Publish_Success(t *testing.T) {
	// 1. Setup
	var handlerCalled bool
	var mu sync.Mutex
	handler := func(ctx context.Context, msg *Message) error {
		mu.Lock()
		handlerCalled = true
		mu.Unlock()
		return nil
	}
	topic := newTopic("test-publish-success", handler, WithBufferSize(1))
	defer topic.stop()

	// 2. Action
	err := topic.publish([]byte("test message"))

	// 3. Assert
	if err != nil {
		t.Fatalf("publish failed unexpectedly: %v", err)
	}

	// Wait for the message to be processed to confirm it was in the queue
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if !handlerCalled {
		t.Error("handler was not called after publishing a message")
	}
	mu.Unlock()
}

// TestTopic_Publish_Timeout verifies that publishing to a full queue times out.
func TestTopic_Publish_Timeout(t *testing.T) {
	// 1. Setup
	blocker := make(chan struct{})
	handler := func(ctx context.Context, msg *Message) error {
		<-blocker
		return nil
	}
	topic := newTopic("test-publish-timeout", handler,
		WithMaxWorkers(1),                       // Only one worker
		WithBufferSize(1),                       // Buffer of size 1
		WithPublishTimeout(50*time.Millisecond), // Short timeout
	)
	defer close(blocker)
	defer topic.stop()

	// 2. Action
	// Message 1: Picked up by the worker, which then blocks.
	if err := topic.publish([]byte("message 1")); err != nil {
		t.Fatalf("publish 1 failed unexpectedly: %v", err)
	}
	time.Sleep(10 * time.Millisecond) // Give dispatcher time to act

	// Message 2: Picked up by the dispatcher, which then blocks waiting for a free worker.
	// The message queue is now empty again.
	if err := topic.publish([]byte("message 2")); err != nil {
		t.Fatalf("publish 2 failed unexpectedly: %v", err)
	}
	time.Sleep(10 * time.Millisecond) // Give dispatcher time to act

	// Message 3: Fills the messageQueue (buffer size is 1).
	if err := topic.publish([]byte("message 3")); err != nil {
		t.Fatalf("publish 3 failed unexpectedly: %v", err)
	}

	// Message 4: This should time out.
	// The worker is busy, the dispatcher is holding a message, and the queue is full.
	err := topic.publish([]byte("message 4"))

	// 3. Assert
	if err != ErrPublishTimeout {
		t.Errorf("expected ErrPublishTimeout, got %v", err)
	}
}

// TestTopic_Stop_Idempotent verifies that calling stop multiple times does not cause a panic.
func TestTopic_Stop_Idempotent(t *testing.T) {
	topic := newTopic("test-stop-idempotent", func(ctx context.Context, msg *Message) error { return nil })

	// Call stop multiple times
	topic.stop()
	topic.stop()

	// If no panic, the test passes. We can also check if the quit channel is closed.
	select {
	case _, ok := <-topic.quit:
		if ok {
			t.Error("topic quit channel should be closed")
		}
	default:
		t.Error("topic quit channel should be closed and readable")
	}
}
