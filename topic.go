package kabaka

import (
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/exp/rand"
)

type HandleFunc func(msg *Message) error

type Message struct {
	ID       uuid.UUID
	Value    []byte
	Retry    int
	CreateAt time.Time
	UpdateAt time.Time
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
			}

		}
	}()

	return id
}

func (t *Topic) publish(message []byte) error {
	t.RLock()
	defer t.RUnlock()

	if len(t.activeSubscribers) == 0 {
		return ErrNoActiveSubscribers
	}

	selectedSubscriber := t.activeSubscribers[rand.Intn(len(t.activeSubscribers))]

	msg := &Message{
		ID:       uuid.New(),
		Value:    message,
		Retry:    3,
		CreateAt: time.Now(),
		UpdateAt: time.Now(),
	}

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
