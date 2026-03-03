package kabaka

import (
	"context"
	"sync"
	"testing"
	"time"
)

// MockBroker is a simple in-memory broker for testing
type MockBroker struct {
	mu            sync.RWMutex
	queues        map[string][]*Message
	registered    map[string]bool
	pushCalls     int
	finishCalls   int
	watchChannels map[string]chan *Task
}

func NewMockBroker() *MockBroker {
	return &MockBroker{
		queues:        make(map[string][]*Message),
		registered:    make(map[string]bool),
		watchChannels: make(map[string]chan *Task),
	}
}

func (m *MockBroker) Register(ctx context.Context, topic string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.registered[topic] = true
	return nil
}

func (m *MockBroker) Unregister(ctx context.Context, topic string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.registered, topic)
	return nil
}

func (m *MockBroker) UnregisterAndCleanup(ctx context.Context, topic string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.registered, topic)
	delete(m.queues, topic)
	return nil
}

func (m *MockBroker) Push(ctx context.Context, topic string, msg *Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pushCalls++
	m.queues[topic] = append(m.queues[topic], msg)

	// Notify watch channel if exists
	if ch, ok := m.watchChannels[topic]; ok {
		select {
		case ch <- &Task{Topic: topic, Message: msg}:
		default:
		}
	}
	return nil
}

func (m *MockBroker) PushDelayed(ctx context.Context, topic string, msg *Message, delay time.Duration) error {
	// Simulate delayed push
	go func() {
		time.Sleep(delay)
		m.Push(context.Background(), topic, msg)
	}()
	return nil
}

func (m *MockBroker) Watch(ctx context.Context, topics ...string) (<-chan *Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ch := make(chan *Task, 10)
	for _, topic := range topics {
		m.watchChannels[topic] = ch
	}

	return ch, nil
}

func (m *MockBroker) Finish(ctx context.Context, topic string, msg *Message, processErr error, duration time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.finishCalls++
	return nil
}

func (m *MockBroker) Len(ctx context.Context, topic string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return int64(len(m.queues[topic])), nil
}

func (m *MockBroker) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ch := range m.watchChannels {
		close(ch)
	}
	m.watchChannels = make(map[string]chan *Task)
	return nil
}

// Helper methods for testing
func (m *MockBroker) GetPushCalls() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.pushCalls
}

func (m *MockBroker) GetFinishCalls() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.finishCalls
}

func (m *MockBroker) GetMessages(topic string) []*Message {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.queues[topic]
}

func (m *MockBroker) IsRegistered(topic string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.registered[topic]
}

// Example tests
func TestBroker_Register(t *testing.T) {
	broker := NewMockBroker()
	ctx := context.Background()

	err := broker.Register(ctx, "test-topic")
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	if !broker.IsRegistered("test-topic") {
		t.Error("Topic should be registered")
	}
}

func TestBroker_Push(t *testing.T) {
	broker := NewMockBroker()
	ctx := context.Background()
	topic := "test-topic"

	msg := &Message{
		Id:        "123",
		Value:     []byte("test message"),
		CreatedAt: time.Now(),
	}

	err := broker.Push(ctx, topic, msg)
	if err != nil {
		t.Fatalf("Push failed: %v", err)
	}

	if broker.GetPushCalls() != 1 {
		t.Errorf("Expected 1 push call, got %d", broker.GetPushCalls())
	}

	messages := broker.GetMessages(topic)
	if len(messages) != 1 {
		t.Errorf("Expected 1 message, got %d", len(messages))
	}

	if messages[0].Id != "123" {
		t.Errorf("Expected message ID 123, got %s", messages[0].Id)
	}
}

func TestBroker_Watch(t *testing.T) {
	broker := NewMockBroker()
	ctx := context.Background()
	topic := "test-topic"

	taskCh, err := broker.Watch(ctx, topic)
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	msg := &Message{
		Id:        "456",
		Value:     []byte("watch test"),
		CreatedAt: time.Now(),
	}

	// Push message in goroutine
	go func() {
		time.Sleep(10 * time.Millisecond)
		broker.Push(ctx, topic, msg)
	}()

	// Wait for message
	select {
	case task := <-taskCh:
		if task.Topic != topic {
			t.Errorf("Expected topic %s, got %s", topic, task.Topic)
		}
		if task.Message.Id != "456" {
			t.Errorf("Expected message ID 456, got %s", task.Message.Id)
		}
	case <-time.After(1 * time.Second):
		t.Error("Timeout waiting for message")
	}
}

func TestBroker_Len(t *testing.T) {
	broker := NewMockBroker()
	ctx := context.Background()
	topic := "test-topic"

	// Initially empty
	length, err := broker.Len(ctx, topic)
	if err != nil {
		t.Fatalf("Len failed: %v", err)
	}
	if length != 0 {
		t.Errorf("Expected length 0, got %d", length)
	}

	// Push 3 messages
	for i := 0; i < 3; i++ {
		msg := &Message{
			Id:        string(rune('1' + i)),
			Value:     []byte("test"),
			CreatedAt: time.Now(),
		}
		broker.Push(ctx, topic, msg)
	}

	length, err = broker.Len(ctx, topic)
	if err != nil {
		t.Fatalf("Len failed: %v", err)
	}
	if length != 3 {
		t.Errorf("Expected length 3, got %d", length)
	}
}

func TestBroker_Unregister(t *testing.T) {
	broker := NewMockBroker()
	ctx := context.Background()
	topic := "test-topic"

	broker.Register(ctx, topic)
	if !broker.IsRegistered(topic) {
		t.Error("Topic should be registered")
	}

	broker.Unregister(ctx, topic)
	if broker.IsRegistered(topic) {
		t.Error("Topic should be unregistered")
	}
}

func TestBroker_Finish(t *testing.T) {
	broker := NewMockBroker()
	ctx := context.Background()
	topic := "test-topic"

	msg := &Message{
		Id:        "789",
		Value:     []byte("finish test"),
		CreatedAt: time.Now(),
	}

	err := broker.Finish(ctx, topic, msg, nil, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	if broker.GetFinishCalls() != 1 {
		t.Errorf("Expected 1 finish call, got %d", broker.GetFinishCalls())
	}
}
