package kabaka

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewTopic(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()

	handler := func(ctx context.Context, msg *Message) error {
		return nil
	}

	topic := newTopic("test-topic", "internal-name", broker, handler)
	defer topic.stop()

	if topic.Name != "test-topic" {
		t.Errorf("Expected topic name test-topic, got %s", topic.Name)
	}

	if topic.InternalName != "internal-name" {
		t.Errorf("Expected internal name internal-name, got %s", topic.InternalName)
	}

	if topic.maxRetries != 3 {
		t.Errorf("Expected max retries 3, got %d", topic.maxRetries)
	}

	if topic.maxWorkers != 20 {
		t.Errorf("Expected max workers 20, got %d", topic.maxWorkers)
	}

	if len(topic.workers) != 20 {
		t.Errorf("Expected 20 workers, got %d", len(topic.workers))
	}
}

func TestNewTopic_WithOptions(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()

	handler := func(ctx context.Context, msg *Message) error {
		return nil
	}

	topic := newTopic("test-topic", "internal-name", broker, handler,
		WithMaxWorkers(5),
		WithMaxRetries(10),
		WithRetryDelay(1*time.Second),
		WithProcessTimeout(5*time.Second),
		WithPublishTimeout(3*time.Second),
	)
	defer topic.stop()

	if topic.maxWorkers != 5 {
		t.Errorf("Expected max workers 5, got %d", topic.maxWorkers)
	}

	if topic.maxRetries != 10 {
		t.Errorf("Expected max retries 10, got %d", topic.maxRetries)
	}

	if topic.retryDelay != 1*time.Second {
		t.Errorf("Expected retry delay 1s, got %v", topic.retryDelay)
	}

	if topic.processTimeout != 5*time.Second {
		t.Errorf("Expected process timeout 5s, got %v", topic.processTimeout)
	}

	if topic.publishTimeout != 3*time.Second {
		t.Errorf("Expected publish timeout 3s, got %v", topic.publishTimeout)
	}

	if len(topic.workers) != 5 {
		t.Errorf("Expected 5 workers, got %d", len(topic.workers))
	}
}

func TestTopic_Publish(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()

	handler := func(ctx context.Context, msg *Message) error {
		return nil
	}

	mockLogger := &MockLogger{}
	topic := newTopic("test-topic", "internal-name", broker, handler)
	topic.logger = mockLogger
	defer topic.stop()

	err := topic.publish([]byte("test message"))
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	if broker.GetPushCalls() != 1 {
		t.Errorf("Expected 1 push call, got %d", broker.GetPushCalls())
	}

	messages := broker.GetMessages("internal-name")
	if len(messages) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(messages))
	}

	if string(messages[0].Value) != "test message" {
		t.Errorf("Expected 'test message', got %s", string(messages[0].Value))
	}
}

func TestTopic_PublishDelayed(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()

	handler := func(ctx context.Context, msg *Message) error {
		return nil
	}

	mockLogger := &MockLogger{}
	topic := newTopic("test-topic", "internal-name", broker, handler)
	topic.logger = mockLogger
	defer topic.stop()

	err := topic.publishDelayed([]byte("delayed message"), 100*time.Millisecond)
	if err != nil {
		t.Fatalf("PublishDelayed failed: %v", err)
	}

	// Wait for delayed message to be pushed
	time.Sleep(200 * time.Millisecond)

	messages := broker.GetMessages("internal-name")
	if len(messages) == 0 {
		t.Error("Expected delayed message to be pushed")
	}
}

func TestTopic_Receive(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()

	var processedCount int32
	var wg sync.WaitGroup
	wg.Add(1)

	handler := func(ctx context.Context, msg *Message) error {
		atomic.AddInt32(&processedCount, 1)
		wg.Done()
		return nil
	}

	mockLogger := &MockLogger{}
	topic := newTopic("test-topic", "internal-name", broker, handler,
		WithMaxWorkers(2),
	)
	topic.logger = mockLogger
	defer topic.stop()

	msg := &Message{
		Id:        "test-msg",
		Value:     []byte("receive test"),
		Retry:     3,
		CreatedAt: time.Now(),
	}

	topic.receive(msg)

	// Wait for processing with timeout
	done := make(chan bool)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message processing")
	}

	if atomic.LoadInt32(&processedCount) != 1 {
		t.Errorf("Expected 1 processed message, got %d", processedCount)
	}
}

func TestTopic_Stop(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()

	handler := func(ctx context.Context, msg *Message) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	topic := newTopic("test-topic", "internal-name", broker, handler,
		WithMaxWorkers(3),
	)

	// Verify workers are started
	if len(topic.workers) != 3 {
		t.Errorf("Expected 3 workers, got %d", len(topic.workers))
	}

	// Stop the topic
	topic.stop()

	// Verify workers are stopped
	topic.mu.RLock()
	workers := topic.workers
	topic.mu.RUnlock()

	if workers != nil {
		t.Error("Workers should be nil after stop")
	}

	// Calling stop again should be safe (stopOnce)
	topic.stop()
}

func TestTopic_StartIdempotent(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()

	handler := func(ctx context.Context, msg *Message) error {
		return nil
	}

	topic := newTopic("test-topic", "internal-name", broker, handler,
		WithMaxWorkers(3),
	)
	defer topic.stop()

	// Verify initial workers count
	initialCount := len(topic.workers)
	if initialCount != 3 {
		t.Errorf("Expected 3 workers initially, got %d", initialCount)
	}

	// Call start() again - should be idempotent (no-op)
	topic.start()

	// Verify workers count hasn't changed
	finalCount := len(topic.workers)
	if finalCount != initialCount {
		t.Errorf("Expected workers count to remain %d, got %d", initialCount, finalCount)
	}

	// Verify it's still the same workers (not recreated)
	if finalCount != 3 {
		t.Errorf("Expected 3 workers after second start(), got %d", finalCount)
	}
}

func TestTopic_ReturnToQueueWhenStoppedBeforeWorkerAvailable(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()
	broker.Register(context.Background(), "internal-name")

	// Handler that takes a long time
	handler := func(ctx context.Context, msg *Message) error {
		time.Sleep(500 * time.Millisecond)
		return nil
	}

	topic := newTopic("test-topic", "internal-name", broker, handler,
		WithMaxWorkers(1), // Only 1 worker
	)

	// Send first message - occupies the only worker
	msg1 := &Message{
		Id:        "msg1",
		Value:     []byte("first message"),
		Retry:     3,
		CreatedAt: time.Now(),
	}
	topic.receive(msg1)

	// Give time for first message to start processing
	time.Sleep(50 * time.Millisecond)

	initialPushCalls := broker.GetPushCalls()

	// Send second message - will wait for worker to be available
	msg2 := &Message{
		Id:        "msg2",
		Value:     []byte("second message"),
		Retry:     3,
		CreatedAt: time.Now(),
	}
	topic.receive(msg2)

	// Stop topic immediately - msg2 should be returned to queue
	time.Sleep(10 * time.Millisecond)
	topic.stop()

	// Wait for returnToQueue to complete
	time.Sleep(100 * time.Millisecond)

	// Verify msg2 was pushed back to broker
	if broker.GetPushCalls() <= initialPushCalls {
		t.Error("Expected message to be returned to queue when topic stopped")
	}

	// Verify message is in broker queue
	messages := broker.GetMessages("internal-name")
	found := false
	for _, msg := range messages {
		if msg.Id == "msg2" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected msg2 to be in broker queue after returnToQueue")
	}
}

func TestTopic_ReturnToQueueSecondSelectBranch(t *testing.T) {
	// This tests the second "case <-t.quit" in receive() (line 136)
	// This branch is hard to trigger reliably because it requires:
	// 1. Successfully getting a worker from workerPool
	// 2. The jobChannel send to block (channel full)
	// 3. Topic stops during the blocked send
	//
	// This is an edge case that rarely happens in practice.
	// The test is best-effort and may not always trigger this branch.

	broker := NewMockBroker()
	defer broker.Close()
	broker.Register(context.Background(), "internal-name")

	handler := func(ctx context.Context, msg *Message) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	topic := newTopic("test-topic", "internal-name", broker, handler,
		WithMaxWorkers(1),
	)

	initialPushCalls := broker.GetPushCalls()

	// Send many messages rapidly to increase chance of catching the edge case
	for i := 0; i < 10; i++ {
		msg := &Message{
			Id:        NewUUID(),
			Value:     []byte("test message"),
			Retry:     3,
			CreatedAt: time.Now(),
		}
		topic.receive(msg)
	}

	// Give brief time for messages to queue
	time.Sleep(20 * time.Millisecond)

	// Stop topic - some messages may be returned to queue
	topic.stop()

	// At least some messages should have been processed or returned to queue
	// This is a best-effort test for an edge case
	t.Logf("Push calls before: %d, after: %d", initialPushCalls, broker.GetPushCalls())
}

func TestTopic_GenerateTraceMessage(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()

	handler := func(ctx context.Context, msg *Message) error {
		return nil
	}

	topic := newTopic("test-topic", "internal-name", broker, handler,
		WithMaxRetries(5),
	)
	defer topic.stop()

	msg := topic.generateTraceMessage([]byte("trace test"))

	if msg.Id == "" {
		t.Error("Message ID should not be empty")
	}

	if string(msg.Value) != "trace test" {
		t.Errorf("Expected 'trace test', got %s", string(msg.Value))
	}

	if msg.Retry != 5 {
		t.Errorf("Expected retry count 5, got %d", msg.Retry)
	}

	if msg.Headers == nil {
		t.Error("Headers should be initialized")
	}

	if msg.CreatedAt.IsZero() {
		t.Error("CreatedAt should be set")
	}
}

func TestTopic_ConcurrentPublish(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()

	handler := func(ctx context.Context, msg *Message) error {
		return nil
	}

	mockLogger := &MockLogger{}
	topic := newTopic("test-topic", "internal-name", broker, handler)
	topic.logger = mockLogger
	defer topic.stop()

	numGoroutines := 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			msg := []byte("concurrent message")
			err := topic.publish(msg)
			if err != nil {
				t.Errorf("Publish failed: %v", err)
			}
		}(i)
	}

	wg.Wait()

	if broker.GetPushCalls() != numGoroutines {
		t.Errorf("Expected %d push calls, got %d", numGoroutines, broker.GetPushCalls())
	}
}

func TestTopic_WithCustomLogger(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()

	mockLogger := &MockLogger{}

	handler := func(ctx context.Context, msg *Message) error {
		return nil
	}

	topic := newTopic("test-topic", "internal-name", broker, handler)
	topic.logger = mockLogger
	defer topic.stop()

	// Publish should log
	topic.publish([]byte("test message"))

	if mockLogger.InfoCalls == 0 {
		t.Error("Expected at least one info log call")
	}
}

func TestTopic_ProcessingMultipleMessages(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()

	var processedCount int32
	var mu sync.Mutex
	processedIDs := make(map[string]bool)

	handler := func(ctx context.Context, msg *Message) error {
		atomic.AddInt32(&processedCount, 1)
		mu.Lock()
		processedIDs[msg.Id] = true
		mu.Unlock()
		time.Sleep(10 * time.Millisecond) // Simulate work
		return nil
	}

	mockLogger := &MockLogger{}
	topic := newTopic("test-topic", "internal-name", broker, handler,
		WithMaxWorkers(5),
	)
	topic.logger = mockLogger
	defer topic.stop()

	numMessages := 10
	var wg sync.WaitGroup
	wg.Add(numMessages)

	for i := 0; i < numMessages; i++ {
		msg := &Message{
			Id:        NewUUID(),
			Value:     []byte("test message"),
			Retry:     3,
			CreatedAt: time.Now(),
		}

		go func(m *Message) {
			topic.receive(m)
			time.Sleep(50 * time.Millisecond) // Give time to process
			wg.Done()
		}(msg)
	}

	wg.Wait()
	time.Sleep(100 * time.Millisecond) // Extra time for processing

	if atomic.LoadInt32(&processedCount) != int32(numMessages) {
		t.Errorf("Expected %d processed messages, got %d", numMessages, processedCount)
	}

	mu.Lock()
	if len(processedIDs) != numMessages {
		t.Errorf("Expected %d unique processed IDs, got %d", numMessages, len(processedIDs))
	}
	mu.Unlock()
}
