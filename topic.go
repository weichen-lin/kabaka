package kabaka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/rand"
)

type HandleFunc func(msg *Message) error

type Message struct {
	ID       uuid.UUID
	Value    []byte
	Retry    int
	CreateAt time.Time
	UpdateAt time.Time
	Headers  map[string]string
	RootSpan trace.Span
}

func (l *Message) Get(key string) string {
	for mapkey, value := range l.Headers {
		if key == mapkey {
			return value
		}
	}

	return ""
}
func (l *Message) Set(key string, value string) {
	l.Headers[key] = value
}
func (l *Message) Keys() []string {
	var keys []string

	for key := range l.Headers {
		keys = append(keys, l.Headers[key])
	}

	return keys
}

type subscriber struct {
	id     uuid.UUID
	active bool
}

type activeSubscriber struct {
	id uuid.UUID
	ch chan *Message
}

type Topic struct {
	Name string

	sync.RWMutex
	subscribers       []*subscriber
	activeSubscribers []*activeSubscriber
	propagator        propagation.TextMapPropagator
	tracer            trace.Tracer
}

func (t *Topic) subscribe(handler HandleFunc, logger Logger) uuid.UUID {
	t.Lock()
	defer t.Unlock()

	ch := make(chan *Message, 20)
	id := uuid.New()

	subscriber := &subscriber{
		id:     id,
		active: true,
	}

	activeSubscriber := &activeSubscriber{
		id: id,
		ch: ch,
	}

	t.subscribers = append(t.subscribers, subscriber)
	t.activeSubscribers = append(t.activeSubscribers, activeSubscriber)

	go func() {
		for msg := range ch {
			parentSpanContext := t.propagator.Extract(context.Background(), propagation.MapCarrier(msg.Headers))

			opts := []trace.SpanStartOption{
				trace.WithSpanKind(trace.SpanKindConsumer),
			}

			ctx, span := t.tracer.Start(parentSpanContext, "consumer start", opts...)
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
					msg.RootSpan.End()
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
					msg.RootSpan.End()
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
				msg.RootSpan.End()
			}
		}
	}()

	return id
}

func (t *Topic) publish(msg *Message) error {
	t.RLock()
	defer t.RUnlock()

	if len(t.activeSubscribers) == 0 {
		return ErrNoActiveSubscribers
	}

	selectedSubscriber := t.activeSubscribers[rand.Intn(len(t.activeSubscribers))]

	select {
	case selectedSubscriber.ch <- msg:
		return nil
	case <-time.After(10 * time.Millisecond):
		return ErrPublishTimeout
	}
}

func (t *Topic) unsubscribe(id uuid.UUID) error {
	t.Lock()
	defer t.Unlock()

	activeIndex := -1
	for i, actSub := range t.activeSubscribers {
		if actSub.id == id {
			close(actSub.ch)
			activeIndex = i
			break
		}
	}

	if activeIndex != -1 {
		t.activeSubscribers = append(t.activeSubscribers[:activeIndex], t.activeSubscribers[activeIndex+1:]...)
	}

	found := false
	for i, sub := range t.subscribers {
		if sub.id == id {
			t.subscribers[i].active = false
			found = true
			break
		}
	}

	if !found {
		return ErrSubscriberNotFound
	}

	return nil
}

func (t *Topic) closeTopic() {
	t.Lock()
	defer t.Unlock()

	for _, sub := range t.activeSubscribers {
		close(sub.ch)
	}
	t.subscribers = nil
	t.activeSubscribers = nil
}
