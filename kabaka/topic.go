package kabaka

import (
	"fmt"
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
	ID     uuid.UUID
	Ch     chan *Message
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
		ID:     id,
		Ch:     ch,
	}

	t.Subscribers = append(t.Subscribers, subscriber)
	t.ActiveSubscribers = append(t.ActiveSubscribers, activeSubscriber)

	go func() {
		for msg := range ch {
			logger.Info(fmt.Sprintf("receive message from topic %s, subscriber: %s, message: %s", t.Name, id.String(), string(msg.Value)))

			err := handler(msg)
			if err != nil {
				logger.Error(fmt.Sprintf("error at topic %s, subscriber: %s, error: %s", t.Name, id.String(), err))

				if msg.Retry > 0 {
					msg.Retry--
					msg.UpdateAt = time.Now()
					ch <- msg
				}
				return
			}
		}
	}()

	return id
}

func (t *Topic) publish(message []byte, logger Logger) error {
	t.RLock()
	defer t.RUnlock()

	if len(t.ActiveSubscribers) == 0 {
		logger.Warn(fmt.Sprintf("no active subscribers for topic %s", t.Name))
		return ErrNoActiveSubscribers
	}

	selectedSubscriber := t.ActiveSubscribers[rand.Intn(len(t.ActiveSubscribers))]

	logger.Info(fmt.Sprintf("publish to topic %s, subscriber: %s, message: %s", t.Name, selectedSubscriber.ID.String(), string(message)))

	msg := &Message{
		ID:       uuid.New(),
		Value:    message,
		Retry:    3,
		CreateAt: time.Now(),
		UpdateAt: time.Now(),
	}

	selectedSubscriber.Ch <- msg
	return nil
}

func (t *Topic) unsubscribe(id uuid.UUID) error {
	t.Lock()
	defer t.Unlock()

	for i, sub := range t.Subscribers {
		if sub.ID == id {
			t.Subscribers[i].Active = false
			t.renewActiveSubscriber()

			return nil
		}
	}

	return ErrTopicSubScriberNotFound
}

func (t *Topic) renewActiveSubscriber() {
	activeSubscribers := make([]*ActiveSubscriber, 0, len(t.Subscribers))
	for _, s := range t.Subscribers {
		if s.Active {
			activeSubscribers = append(activeSubscribers, &ActiveSubscriber{
				ID: s.ID,
				Ch: make(chan *Message, 20),
			})
		}
	}
	t.ActiveSubscribers = activeSubscribers
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
