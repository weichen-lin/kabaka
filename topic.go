package kabaka

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/rand"
)

type HandleFunc func(msg *Message) error

type subscriber struct {
	active bool
	ch     chan *Message
	done   chan struct{}
}

type Topic struct {
	Name string

	sync.RWMutex
	subscribers       map[string]*subscriber
	activeSubscribers []uuid.UUID
	bufferSize        int
	maxRetries        int
	retryDelay        time.Duration
	processTimeout    time.Duration
}

func (t *Topic) subscribe(handler HandleFunc) uuid.UUID {
	t.Lock()
	defer t.Unlock()

	ch := make(chan *Message, t.bufferSize)
	id := uuid.New()

	subscriber := &subscriber{
		active: true,
		ch:     ch,
		done:   make(chan struct{}),
	}

	t.subscribers[id.String()] = subscriber
	t.activeSubscribers = append(t.activeSubscribers, id)

	go func() {
		defer close(subscriber.done)

		for msg := range ch {
			now := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), t.processTimeout)

			span := t.injectCtx(msg)

			result := make(chan error, 1)
			go func() {
				result <- handler(msg)
			}()

			select {
			case err := <-result:
				duration := time.Since(now)

				if err != nil {
					t.handleError(msg, err, id, span, duration)
				} else {
					t.handleSuccess(msg, id, span, duration)
				}
			case <-ctx.Done():
				t.handleError(msg, ctx.Err(), id, span, time.Since(now))
			}

			cancel()
		}
	}()

	return id
}

func (t *Topic) publish(msg *Message) error {
	t.RLock()
	defer t.RUnlock()

	randomIndex := rand.Intn(len(t.activeSubscribers))
	selectedID := t.activeSubscribers[randomIndex]
	selectedSubscriber := t.subscribers[selectedID.String()]

	select {
	case selectedSubscriber.ch <- msg:
		return nil
	default:
		return ErrPublishTimeout
	}
}

func (t *Topic) handleError(msg *Message, err error, id uuid.UUID, span trace.Span, duration time.Duration) {
	logger := getKabakaLogger()

	msg.Retry--

	if msg.Retry >= 0 {
		logger.Error(&LogMessage{
			TopicName:     t.Name,
			Action:        Consume,
			MessageID:     msg.ID,
			Message:       string(msg.Value),
			MessageStatus: Retry,
			SubScriber:    id,
			SpendTime:     duration.Milliseconds(),
			CreatedAt:     time.Now(),
		})

		backoff := t.retryDelay * time.Duration(math.Pow(2, float64(t.maxRetries-msg.Retry)))

		time.Sleep(backoff)

		select {
		case t.subscribers[id.String()].ch <- msg:
			span.SetStatus(codes.Error, fmt.Sprintf("retry attempt %d/%d", t.maxRetries-msg.Retry, t.maxRetries))
			span.End()
		default:
			span.SetStatus(codes.Error, "channel full, cannot retry message")
			span.End()
			if msg.RootSpan != nil {
				msg.RootSpan.End()
			}
		}
	} else {
		logger.Error(&LogMessage{
			TopicName:     t.Name,
			Action:        Consume,
			MessageID:     msg.ID,
			Message:       string(msg.Value),
			MessageStatus: Error,
			SubScriber:    id,
			SpendTime:     duration.Milliseconds(),
			CreatedAt:     time.Now(),
		})
		span.SetStatus(codes.Error, "max retries exceeded")
		span.End()
		if msg.RootSpan != nil {
			msg.RootSpan.End()
		}
	}
}

func (t *Topic) handleSuccess(msg *Message, id uuid.UUID, span trace.Span, duration time.Duration) {
	defer func() {
		span.End()
		if msg.RootSpan != nil {
			msg.RootSpan.End()
		}
	}()

	logger := getKabakaLogger()

	logger.Info(&LogMessage{
		TopicName:     t.Name,
		Action:        Consume,
		MessageID:     msg.ID,
		Message:       string(msg.Value),
		MessageStatus: Success,
		SubScriber:    id,
		SpendTime:     duration.Milliseconds(),
		CreatedAt:     time.Now(),
	})
}

func (t *Topic) unsubscribe(id uuid.UUID) error {
	t.Lock()
	defer t.Unlock()

	sub, ok := t.subscribers[id.String()]
	if !ok {
		return ErrSubscriberNotFound
	}

	found := false
	for i, activeID := range t.activeSubscribers {
		if activeID == id {
			t.activeSubscribers = append(t.activeSubscribers[:i], t.activeSubscribers[i+1:]...)
			found = true
			break
		}
	}

	if !found {
		return ErrSubscriberNotFound
	}

	sub.active = false
	close(sub.ch)

	<-sub.done

	delete(t.subscribers, id.String())

	return nil
}

func (t *Topic) closeTopic() {
	t.Lock()
	defer t.Unlock()

	for _, sub := range t.subscribers {
		close(sub.ch)
	}
	t.subscribers = nil
	t.activeSubscribers = nil
}

func (t *Topic) injectCtx(msg *Message) trace.Span {
	propagator := otel.GetTextMapPropagator()
	provider := otel.GetTracerProvider()

	tracer := provider.Tracer(
		defaultTraceName,
		trace.WithInstrumentationVersion(version),
	)

	parentCtx := propagator.Extract(context.Background(), propagation.MapCarrier(msg.Headers))

	opts := []trace.SpanStartOption{
		trace.WithAttributes(
			semconv.MessagingMessageIDKey.String(msg.ID.String()),
		),
		trace.WithSpanKind(trace.SpanKindConsumer),
	}

	ctx, span := tracer.Start(parentCtx, fmt.Sprintf("consume message %s", msg.ID.String()), opts...)
	propagator.Inject(ctx, propagation.MapCarrier(msg.Headers))

	return span
}

func (t *Topic) generateTraceMessage(topicName string, message []byte, propagation propagation.TextMapCarrier) *Message {
	headers := make(map[string]string)

	msg := &Message{
		ID:       uuid.New(),
		Value:    message,
		Retry:    t.maxRetries,
		CreateAt: time.Now(),
		Headers:  headers,
	}

	msg.initTrace(topicName, propagation)

	return msg
}

func NewTopic(name string, options *Options) *Topic {
	return &Topic{
		Name:              name,
		subscribers:       make(map[string]*subscriber),
		activeSubscribers: make([]uuid.UUID, 0),
		maxRetries:        options.DefaultMaxRetries,
		retryDelay:        options.DefaultRetryDelay,
		processTimeout:    options.DefaultProcessTimeout,
		bufferSize:        options.BufferSize,
	}
}
