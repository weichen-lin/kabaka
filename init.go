package kabaka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
)

type Kabaka struct {
	sync.RWMutex
	topics     map[string]*Topic
	logger     Logger
	propagator propagation.TextMapPropagator
	tracer     trace.Tracer
}

var defaultTraceName = "kabaka"
var version = "1.0.0"

func NewKabaka(config *Config) *Kabaka {

	provider := otel.GetTracerProvider()

	return &Kabaka{
		topics:     make(map[string]*Topic),
		logger:     config.Logger,
		propagator: otel.GetTextMapPropagator(),
		tracer: provider.Tracer(
			defaultTraceName,
			trace.WithInstrumentationVersion(version),
		),
	}
}

func (t *Kabaka) CreateTopic(name string) error {
	t.Lock()
	defer t.Unlock()

	if _, ok := t.topics[name]; ok {
		return ErrTopicAlreadyCreated
	}

	topic := &Topic{
		Name:        name,
		subscribers: make([]*subscriber, 0),
		tracer:      t.tracer,
		propagator:  t.propagator,
	}

	t.topics[name] = topic

	return nil
}

func (t *Kabaka) Subscribe(name string, handler HandleFunc) (uuid.UUID, error) {
	t.RLock()
	defer t.RUnlock()

	topic, ok := t.topics[name]
	if !ok {
		return uuid.Nil, ErrTopicNotFound
	}

	return topic.subscribe(handler, t.logger), nil
}

func (t *Kabaka) Publish(name string, message []byte) error {
	topic, ok := t.topics[name]
	if !ok {
		return ErrTopicNotFound
	}

	headers := make(map[string]string)

	msg := &Message{
		ID:       uuid.New(),
		Value:    message,
		Retry:    3,
		CreateAt: time.Now(),
		UpdateAt: time.Now(),
		Headers:  headers,
	}

	parentCtx := t.propagator.Extract(context.Background(), msg)

	opts := []trace.SpanStartOption{
		trace.WithAttributes(
			semconv.MessagingDestinationKey.String(name),
			semconv.MessagingDestinationKindTopic.Key.Bool(true),
		),
		trace.WithSpanKind(trace.SpanKindProducer),
	}

	traceName := fmt.Sprintf("%s send", name)
	ctx, span := t.tracer.Start(parentCtx, traceName, opts...)

	t.propagator.Inject(ctx, propagation.MapCarrier(msg.Headers))

	msg.RootSpan = span

	err := topic.publish(msg)
	if err != nil {
		return err
	}

	return nil
}

func (t *Kabaka) UnSubscribe(name string, id uuid.UUID) error {
	topic, ok := t.topics[name]
	if !ok {
		return ErrTopicNotFound
	}

	err := topic.unsubscribe(id)
	if err != nil {
		return err
	}

	return nil
}

func (t *Kabaka) CloseTopic(name string) error {
	t.Lock()
	defer t.Unlock()

	topic, ok := t.topics[name]
	if !ok {
		return ErrTopicNotFound
	}

	topic.closeTopic()

	delete(t.topics, name)
	return nil
}

func (t *Kabaka) Close() error {
	t.Lock()
	defer t.Unlock()

	for _, topic := range t.topics {
		topic.closeTopic()
	}
	t.topics = nil
	return nil
}
