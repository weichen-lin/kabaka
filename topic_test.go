package kabaka

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
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

	provider := otel.GetTracerProvider()

	topic := &Topic{
		Name: "test-topic",
		tracer: provider.Tracer(
			defaultTraceName,
			trace.WithInstrumentationVersion(version),
		),
		propagator: otel.GetTextMapPropagator(),
	}

	handler := func(msg *Message) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	}

	subID := topic.subscribe(handler, mockLogger)

	headers := make(map[string]string)

	msg := &Message{
		ID:       uuid.New(),
		Value:    []byte("test message"),
		Retry:    3,
		CreateAt: time.Now(),
		UpdateAt: time.Now(),
		Headers:  headers,
	}

	err := topic.publish(msg)
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

	mockLogger := &MockLogger{}

	provider := otel.GetTracerProvider()

	topic := &Topic{
		Name: "test-topic",
		tracer: provider.Tracer(
			defaultTraceName,
			trace.WithInstrumentationVersion(version),
		),
		propagator: otel.GetTextMapPropagator(),
	}

	handler := func(msg *Message) error {
		return errors.New("test error")
	}

	subID := topic.subscribe(handler, mockLogger)

	headers := make(map[string]string)

	msg := &Message{
		ID:       uuid.New(),
		Value:    []byte("test message"),
		Retry:    3,
		CreateAt: time.Now(),
		UpdateAt: time.Now(),
		Headers:  headers,
	}

	err := topic.publish(msg)
	require.NoError(t, err)

	time.Sleep(20 * time.Millisecond)

	require.Len(t, mockLogger.logs, 4)

	status := []MessageStatus{Retry, Retry, Retry, Error}

	for i, log := range mockLogger.logs {
		require.Equal(t, "test-topic", log.TopicName)
		require.Equal(t, "test message", log.Message)
		require.Equal(t, status[i], log.MessageStatus)
		require.Equal(t, subID, log.SubScriber)
		require.True(t, log.SpendTime >= 0)
	}
}

func TestPublishError(t *testing.T) {
	mockLogger := &MockLogger{}

	provider := otel.GetTracerProvider()

	topic := &Topic{
		Name: "test-topic",
		tracer: provider.Tracer(
			defaultTraceName,
			trace.WithInstrumentationVersion(version),
		),
		propagator: otel.GetTextMapPropagator(),
	}

	handler := func(msg *Message) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	subId := topic.subscribe(handler, mockLogger)

	err := topic.unsubscribe(subId)
	require.NoError(t, err)

	headers := make(map[string]string)

	msg := &Message{
		ID:       uuid.New(),
		Value:    []byte("test message"),
		Retry:    3,
		CreateAt: time.Now(),
		UpdateAt: time.Now(),
		Headers:  headers,
	}

	err = topic.publish(msg)
	require.ErrorIs(t, err, ErrNoActiveSubscribers)

	topic.subscribers = nil

	err = topic.unsubscribe(subId)
	require.ErrorIs(t, err, ErrSubscriberNotFound)
}

func TestPublishTimeout(t *testing.T) {
	mockLogger := &MockLogger{}

	provider := otel.GetTracerProvider()

	topic := &Topic{
		Name: "test-topic",
		tracer: provider.Tracer(
			defaultTraceName,
			trace.WithInstrumentationVersion(version),
		),
		propagator: otel.GetTextMapPropagator(),
	}

	handler := func(msg *Message) error {
		time.Sleep(1000 * time.Millisecond)
		return nil
	}

	topic.subscribe(handler, mockLogger)

	for i := 1; i <= 21; i++ {
		headers := make(map[string]string)

		msg := &Message{
			ID:       uuid.New(),
			Value:    []byte("test message"),
			Retry:    3,
			CreateAt: time.Now(),
			UpdateAt: time.Now(),
			Headers:  headers,
		}
		err := topic.publish(msg)
		require.NoError(t, err)
	}

	headers := make(map[string]string)

	msg := &Message{
		ID:       uuid.New(),
		Value:    []byte("test message"),
		Retry:    3,
		CreateAt: time.Now(),
		UpdateAt: time.Now(),
		Headers:  headers,
	}
	err := topic.publish(msg)

	err = topic.publish(msg)
	require.ErrorIs(t, err, ErrPublishTimeout)
}

func TestTopic_closeTopic(t *testing.T) {
	provider := otel.GetTracerProvider()

	topic := &Topic{
		Name: "test-topic",
		tracer: provider.Tracer(
			defaultTraceName,
			trace.WithInstrumentationVersion(version),
		),
		propagator: otel.GetTextMapPropagator(),
	}

	numSubscribers := 3
	for i := 0; i < numSubscribers; i++ {
		topic.activeSubscribers = append(topic.activeSubscribers, &activeSubscriber{
			id: uuid.New(),
			ch: make(chan *Message, 1),
		})
	}

	for _, sub := range topic.activeSubscribers {
		sub.ch <- &Message{ID: uuid.New(), Value: []byte("test")}
	}

	topic.closeTopic()

	for _, sub := range topic.activeSubscribers {
		select {
		case _, ok := <-sub.ch:
			require.Equal(t, false, ok)
		case <-time.After(10 * time.Millisecond):
			t.Error("Channel read timed out, it should be closed")
		}
	}

	require.Empty(t, topic.subscribers)
	require.Empty(t, topic.activeSubscribers)
}
