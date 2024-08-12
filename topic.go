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

type Subscriber struct {
	ID     uuid.UUID
	Active bool
}

type ActiveSubscriber struct {
	ID uuid.UUID
	Ch chan *Message
}

type Topic struct {
	sync.RWMutex
	Name              string
	Subscribers       []*Subscriber
	ActiveSubscribers []*ActiveSubscriber
}

func (t *Topic) subscribe(handler HandleFunc, logger Logger) uuid.UUID {
	t.Lock()
	defer t.Unlock()

	ch := make(chan *Message, 20)
	id := uuid.New()

	subscriber := &Subscriber{
		ID:     id,
		Active: true,
	}

	activeSubscriber := &ActiveSubscriber{
		ID: id,
		Ch: ch,
	}

	t.Subscribers = append(t.Subscribers, subscriber)
	t.ActiveSubscribers = append(t.ActiveSubscribers, activeSubscriber)

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

	if len(t.ActiveSubscribers) == 0 {
		return ErrNoActiveSubscribers
	}

	selectedSubscriber := t.ActiveSubscribers[rand.Intn(len(t.ActiveSubscribers))]

	msg := &Message{
		ID:       uuid.New(),
		Value:    message,
		Retry:    3,
		CreateAt: time.Now(),
		UpdateAt: time.Now(),
	}

	select {
	case selectedSubscriber.Ch <- msg:
		return nil
	case <-time.After(10 * time.Millisecond):
		return ErrPublishTimeout
	}
}

func (t *Topic) unsubscribe(id uuid.UUID) error {
	t.Lock()
	defer t.Unlock()

	activeIndex := -1
	for i, actSub := range t.ActiveSubscribers {
		if actSub.ID == id {
			close(actSub.Ch)
			activeIndex = i
			break
		}
	}

	if activeIndex != -1 {
		t.ActiveSubscribers = append(t.ActiveSubscribers[:activeIndex], t.ActiveSubscribers[activeIndex+1:]...)
	}

	found := false
	for i, sub := range t.Subscribers {
		if sub.ID == id {
			t.Subscribers[i].Active = false
			found = true
			break
		}
	}

	if !found {
		return ErrSubscriberNotFound
	}

	return nil
}

func (t *Topic) closeTopic() error {
	t.Lock()
	defer t.Unlock()

	for _, sub := range t.ActiveSubscribers {
		close(sub.Ch)
	}
	t.Subscribers = nil
	t.ActiveSubscribers = nil
	return nil
}
