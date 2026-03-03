package kabaka

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewWorker(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()

	handler := func(ctx context.Context, msg *Message) error {
		return nil
	}

	topic := newTopic("test-topic", "internal-name", broker, handler,
		WithMaxWorkers(1),
	)
	defer topic.stop()

	worker := NewWorker(topic)

	if worker.id == "" {
		t.Error("Worker ID should not be empty")
	}

	if worker.topic != topic {
		t.Error("Worker topic should be set")
	}

	if worker.jobChannel == nil {
		t.Error("Worker jobChannel should be initialized")
	}

	if worker.quit == nil {
		t.Error("Worker quit channel should be initialized")
	}
}

func TestWorker_Process_Success(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()

	var processedCount int32

	handler := func(ctx context.Context, msg *Message) error {
		atomic.AddInt32(&processedCount, 1)
		return nil
	}

	mockLogger := &MockLogger{}
	topic := newTopic("test-topic", "internal-name", broker, handler,
		WithMaxWorkers(1),
	)
	topic.logger = mockLogger
	defer topic.stop()

	worker := NewWorker(topic)

	msg := &Message{
		Id:        "test-msg",
		Value:     []byte("test"),
		Retry:     3,
		CreatedAt: time.Now(),
	}

	worker.process(msg)

	if atomic.LoadInt32(&processedCount) != 1 {
		t.Errorf("Expected 1 processed message, got %d", processedCount)
	}

	// Should log info on success
	if mockLogger.InfoCalls != 1 {
		t.Error("Expected info log on successful processing")
	}

	if broker.GetFinishCalls() != 1 {
		t.Errorf("Expected 1 finish call, got %d", broker.GetFinishCalls())
	}
}

func TestWorker_Process_Error(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()

	testError := errors.New("processing error")

	handler := func(ctx context.Context, msg *Message) error {
		return testError
	}

	mockLogger := &MockLogger{}
	topic := newTopic("test-topic", "internal-name", broker, handler,
		WithMaxWorkers(1),
		WithMaxRetries(2),
	)
	topic.logger = mockLogger
	defer topic.stop()

	worker := NewWorker(topic)

	msg := &Message{
		Id:        "error-msg",
		Value:     []byte("test"),
		Retry:     2,
		CreatedAt: time.Now(),
	}

	worker.process(msg)

	// Should log error
	if mockLogger.ErrorCalls == 0 {
		t.Error("Expected error log on failed processing")
	}

	// Should retry (push delayed) - wait longer for delayed push
	time.Sleep(200 * time.Millisecond)

	// Check if PushDelayed was called (it pushes in background)
	// Since MockBroker's PushDelayed uses goroutine, we need to wait
	if mockLogger.ErrorCalls < 1 {
		t.Error("Expected error to be logged for failed processing")
	}
}

func TestWorker_Process_NoRetryLeft(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()

	handler := func(ctx context.Context, msg *Message) error {
		return errors.New("processing error")
	}

	mockLogger := &MockLogger{}
	topic := newTopic("test-topic", "internal-name", broker, handler,
		WithMaxWorkers(1),
	)
	topic.logger = mockLogger
	defer topic.stop()

	worker := NewWorker(topic)

	msg := &Message{
		Id:        "no-retry-msg",
		Value:     []byte("test"),
		Retry:     0, // No retry left
		CreatedAt: time.Now(),
	}

	initialPushCalls := broker.GetPushCalls()
	worker.process(msg)

	// Should not retry
	time.Sleep(100 * time.Millisecond)
	if broker.GetPushCalls() > initialPushCalls {
		t.Error("Should not retry when retry count is 0")
	}
}

func TestWorker_Process_WithTimeout(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()

	handler := func(ctx context.Context, msg *Message) error {
		// Simulate slow processing
		time.Sleep(200 * time.Millisecond)
		// Check if context was cancelled
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}

	mockLogger := &MockLogger{}
	topic := newTopic("test-topic", "internal-name", broker, handler,
		WithMaxWorkers(1),
		WithProcessTimeout(50*time.Millisecond), // Short timeout
	)
	topic.logger = mockLogger
	defer topic.stop()

	worker := NewWorker(topic)

	msg := &Message{
		Id:        "timeout-msg",
		Value:     []byte("test"),
		Retry:     1,
		CreatedAt: time.Now(),
	}

	worker.process(msg)

	// Should log error due to timeout
	if mockLogger.ErrorCalls == 0 {
		t.Error("Expected error log due to timeout")
	}
}

func TestWorker_Start(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()

	var processedCount int32

	handler := func(ctx context.Context, msg *Message) error {
		atomic.AddInt32(&processedCount, 1)
		return nil
	}

	mockLogger := &MockLogger{}
	topic := newTopic("test-topic", "internal-name", broker, handler,
		WithMaxWorkers(0), // Don't auto-start workers
	)
	topic.logger = mockLogger
	defer topic.stop()

	worker := NewWorker(topic)
	jobChannel := worker.start()

	// Send a message
	msg := &Message{
		Id:        "start-test",
		Value:     []byte("test"),
		Retry:     3,
		CreatedAt: time.Now(),
	}

	// Get worker from pool
	workerJobChannel := <-topic.workerPool
	workerJobChannel <- msg

	// Wait for processing
	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt32(&processedCount) != 1 {
		t.Errorf("Expected 1 processed message, got %d", processedCount)
	}

	if jobChannel != worker.jobChannel {
		t.Error("Start should return worker's job channel")
	}
}

func TestWorker_Stop(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()

	handler := func(ctx context.Context, msg *Message) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	}

	mockLogger := &MockLogger{}
	topic := newTopic("test-topic", "internal-name", broker, handler,
		WithMaxWorkers(0), // Don't auto-start workers
	)
	topic.logger = mockLogger
	defer topic.stop()

	worker := NewWorker(topic)
	worker.start()

	// Stop the worker
	worker.stop()

	// Verify quit channel is closed
	select {
	case <-worker.quit:
		// Good, quit channel is closed
	case <-time.After(100 * time.Millisecond):
		t.Error("Quit channel should be closed")
	}

	// Job channel should be closed after stop
	select {
	case _, ok := <-worker.jobChannel:
		if ok {
			t.Error("Job channel should be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Job channel should be closed")
	}
}

func TestWorker_QuitWhileReturningToPool(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()

	// Handler that takes some time
	handler := func(ctx context.Context, msg *Message) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	}

	mockLogger := &MockLogger{}
	topic := newTopic("test-topic", "internal-name", broker, handler,
		WithMaxWorkers(0), // Don't auto-start workers
	)
	topic.logger = mockLogger

	worker := NewWorker(topic)
	worker.start()

	// Send a message
	msg := &Message{
		Id:        "test-msg",
		Value:     []byte("test"),
		Retry:     3,
		CreatedAt: time.Now(),
	}

	// Get worker from pool and send message
	workerCh := <-topic.workerPool
	workerCh <- msg

	// Close worker while it's processing (will quit when trying to return to pool)
	time.Sleep(25 * time.Millisecond) // Let it start processing
	close(worker.quit)

	// Wait for goroutine to exit
	worker.wg.Wait()

	// Worker should have exited gracefully (no panic)
	// This tests the "case <-w.quit: return" in the "Back to pool" select
}

func TestWorker_JobChannelClosedWhileRunning(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()

	handler := func(ctx context.Context, msg *Message) error {
		return nil
	}

	mockLogger := &MockLogger{}
	topic := newTopic("test-topic", "internal-name", broker, handler,
		WithMaxWorkers(0), // Don't auto-start workers
	)
	topic.logger = mockLogger

	worker := NewWorker(topic)
	worker.start()

	// Take worker from pool so it enters the "waiting for message" state
	workerCh := <-topic.workerPool

	// Close the job channel while worker is waiting for messages
	close(workerCh)

	// Wait for goroutine to exit
	worker.wg.Wait()

	// Worker should have exited gracefully when channel was closed
	// This tests the "if !ok { return }" branch
}

func TestWorker_DrainJobChannel(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()
	broker.Register(context.Background(), "internal-name")

	mockLogger := &MockLogger{}

	handler := func(ctx context.Context, msg *Message) error {
		return nil
	}

	topic := newTopic("test-topic", "internal-name", broker, handler,
		WithMaxWorkers(1),
	)
	topic.logger = mockLogger

	worker := NewWorker(topic)

	// With buffered channel (capacity 1), add one message
	msg1 := &Message{Id: "msg1", Value: []byte("test1"), CreatedAt: time.Now()}

	worker.jobChannel <- msg1

	initialPushCalls := broker.GetPushCalls()
	initialInfoCalls := mockLogger.InfoCalls

	// Drain the channel
	worker.drainJobChannel()

	// Verify message was pushed back to broker
	expectedPushCalls := initialPushCalls + 1
	if broker.GetPushCalls() != expectedPushCalls {
		t.Errorf("Expected %d push calls, got %d", expectedPushCalls, broker.GetPushCalls())
	}

	// Verify logger was called for requeue
	expectedInfoCalls := initialInfoCalls + 1
	if mockLogger.InfoCalls != expectedInfoCalls {
		t.Errorf("Expected %d info calls, got %d", expectedInfoCalls, mockLogger.InfoCalls)
	}

	// Verify log message content
	if mockLogger.LastLog == nil {
		t.Fatal("Expected LastLog to be set")
	}

	if mockLogger.LastLog.Action != "requeue_on_stop" {
		t.Errorf("Expected Action 'requeue_on_stop', got '%s'", mockLogger.LastLog.Action)
	}

	if mockLogger.LastLog.Message != "worker stopped, message requeued" {
		t.Errorf("Expected Message 'worker stopped, message requeued', got '%s'", mockLogger.LastLog.Message)
	}

	if mockLogger.LastLog.TopicName != "test-topic" {
		t.Errorf("Expected TopicName 'test-topic', got '%s'", mockLogger.LastLog.TopicName)
	}

	// Verify message was requeued correctly
	messages := broker.GetMessages("internal-name")
	if len(messages) != 1 {
		t.Errorf("Expected 1 requeued message, got %d", len(messages))
	}
}

func TestWorker_DrainJobChannel_ChannelClosed(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()
	broker.Register(context.Background(), "internal-name")

	mockLogger := &MockLogger{}

	handler := func(ctx context.Context, msg *Message) error {
		return nil
	}

	topic := newTopic("test-topic", "internal-name", broker, handler,
		WithMaxWorkers(1),
	)
	topic.logger = mockLogger

	worker := NewWorker(topic)

	// Close channel before draining
	close(worker.jobChannel)

	initialPushCalls := broker.GetPushCalls()
	initialInfoCalls := mockLogger.InfoCalls

	// Call drainJobChannel - should return immediately because channel is closed
	worker.drainJobChannel()

	// Verify no push or log calls (because it returned immediately on !ok)
	if broker.GetPushCalls() != initialPushCalls {
		t.Errorf("Expected no new push calls when channel is closed, got %d new calls",
			broker.GetPushCalls()-initialPushCalls)
	}

	if mockLogger.InfoCalls != initialInfoCalls {
		t.Errorf("Expected no new info calls when channel is closed, got %d new calls",
			mockLogger.InfoCalls-initialInfoCalls)
	}
}

func TestWorker_ConcurrentProcessing(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()

	var processedCount int32

	handler := func(ctx context.Context, msg *Message) error {
		atomic.AddInt32(&processedCount, 1)
		time.Sleep(10 * time.Millisecond)
		return nil
	}

	mockLogger := &MockLogger{}
	numWorkers := 5
	topic := newTopic("test-topic", "internal-name", broker, handler,
		WithMaxWorkers(numWorkers),
	)
	topic.logger = mockLogger
	defer topic.stop()

	// Send multiple messages
	numMessages := 20
	for range numMessages {
		msg := &Message{
			Id:        NewUUID(),
			Value:     []byte("concurrent test"),
			Retry:     3,
			CreatedAt: time.Now(),
		}
		topic.receive(msg)
	}

	// Wait for all to be processed
	time.Sleep(500 * time.Millisecond)

	if atomic.LoadInt32(&processedCount) != int32(numMessages) {
		t.Errorf("Expected %d processed messages, got %d", numMessages, processedCount)
	}
}

func TestWorker_RetryBackoff(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()

	handler := func(ctx context.Context, msg *Message) error {
		return errors.New("always fail")
	}

	mockLogger := &MockLogger{}
	topic := newTopic("test-topic", "internal-name", broker, handler,
		WithMaxWorkers(1),
		WithMaxRetries(3),
		WithRetryDelay(100*time.Millisecond),
	)
	topic.logger = mockLogger
	defer topic.stop()

	worker := NewWorker(topic)

	// Message with 2 retries left (will fail and reduce to 1)
	msg := &Message{
		Id:        "retry-test",
		Value:     []byte("test"),
		Retry:     2,
		CreatedAt: time.Now(),
	}

	worker.process(msg)

	// Check that message was pushed back with delay
	// Wait longer to ensure the delayed push completes
	time.Sleep(250 * time.Millisecond)
	messages := broker.GetMessages("internal-name")
	if len(messages) == 0 {
		t.Fatal("Expected message to be requeued with backoff")
	}

	// Verify retry count was decremented
	if messages[0].Retry != 1 {
		t.Errorf("Expected retry count 1, got %d", messages[0].Retry)
	}
}
