package kabaka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
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
}

type Topic struct {
	Name string

	sync.RWMutex
	subscribers map[string]*subscriber
	propagator  propagation.TextMapPropagator
	tracer      trace.Tracer
}

func (t *Topic) subscribe(handler HandleFunc, logger Logger) uuid.UUID {
	t.Lock()
	defer t.Unlock()

	ch := make(chan *Message, 20)
	id := uuid.New()

	subscriber := &subscriber{
		active: true,
		ch:     ch,
	}

	t.subscribers[id.String()] = subscriber

	go func() {
		for msg := range ch {
			parentSpanContext := t.propagator.Extract(context.Background(), propagation.MapCarrier(msg.Headers))

			opts := []trace.SpanStartOption{
				trace.WithAttributes(
					semconv.MessagingMessageIDKey.String(msg.ID.String()),
				),
				trace.WithSpanKind(trace.SpanKindConsumer),
			}

			ctx, span := t.tracer.Start(parentSpanContext, fmt.Sprintf("consume message %s", msg.ID.String()), opts...)
			t.propagator.Inject(ctx, propagation.MapCarrier(msg.Headers))

			now := time.Now()

			err := handler(msg)

			if err != nil {
				logger.Error(&LogMessage{
					TopicName:     t.Name,
					Action:        Consume,
					MessageID:     msg.ID,
					Message:       string(msg.Value),
					MessageStatus: Retry,
					SubScriber:    id,
					SpendTime:     time.Since(now).Milliseconds(),
					CreatedAt:     time.Now(),
				})

				msg.Retry--

				if msg.Retry > 0 {
					msg.UpdateAt = time.Now()
					ch <- msg

					span.SetStatus(codes.Error, fmt.Errorf("message retry limit count %d", 3-msg.Retry).Error())
					span.End()
					if msg.RootSpan != nil {
						msg.RootSpan.End()
					}
				} else {
					logger.Warn(&LogMessage{
						TopicName:     t.Name,
						Action:        Consume,
						MessageID:     msg.ID,
						Message:       string(msg.Value),
						MessageStatus: Error,
						SubScriber:    id,
						SpendTime:     time.Since(now).Milliseconds(),
						CreatedAt:     time.Now(),
					})

					span.SetStatus(codes.Error, "retry to max")
					span.End()
					if msg.RootSpan != nil {
						msg.RootSpan.End()
					}
				}
			} else {
				logger.Info(&LogMessage{
					TopicName:     t.Name,
					Action:        Consume,
					MessageID:     msg.ID,
					Message:       string(msg.Value),
					MessageStatus: Success,
					SubScriber:    id,
					SpendTime:     time.Since(now).Milliseconds(),
					CreatedAt:     time.Now(),
				})

				span.SetStatus(codes.Ok, "message consume success")
				span.End()
				if msg.RootSpan != nil {
					msg.RootSpan.End()
				}
			}
		}
	}()

	return id
}

func (t *Topic) publish(msg *Message) error {
	t.RLock()
	defer t.RUnlock()

	activeSubscribers := make([]string, 0, len(t.subscribers))
	for id, sub := range t.subscribers {
		if sub.active {
			activeSubscribers = append(activeSubscribers, id)
		}
	}

	if len(activeSubscribers) == 0 {
		return ErrNoActiveSubscribers
	}

	randomIndex := rand.Intn(len(activeSubscribers))
	selectedID := activeSubscribers[randomIndex]
	selectedSubscriber := t.subscribers[selectedID]

	select {
	case selectedSubscriber.ch <- msg:
		return nil
	case <-time.After(10 * time.Millisecond):
		return ErrPublishTimeout
	default:
		return ErrPublishTimeout
	}
}

func (t *Topic) unsubscribe(id uuid.UUID) error {
	t.Lock()
	defer t.Unlock()

	sub, ok := t.subscribers[id.String()]
	if !ok {
		return ErrSubscriberNotFound
	}

	sub.active = false
	close(sub.ch)
	return nil
}

func (t *Topic) closeTopic() {
	t.Lock()
	defer t.Unlock()

	for _, sub := range t.subscribers {
		close(sub.ch)
	}
	t.subscribers = nil
}
