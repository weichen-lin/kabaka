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
	Ch     chan *Message
	Active bool
}

type Topic struct {
	sync.RWMutex
	Name              string
	Subscribers       []*Subscriber
	ActiveSubscribers []*Subscriber
}

func (t *Topic) subscribe(handler HandleFunc, logger Logger) uuid.UUID {
	t.Lock()
	defer t.Unlock()

	ch := make(chan *Message, 20)
	id := uuid.New()

	subscriber := &Subscriber{
		ID:     id,
		Ch:     ch,
		Active: true,
	}

	t.Subscribers = append(t.Subscribers, subscriber)
	t.ActiveSubscribers = append(t.ActiveSubscribers, subscriber)

	go func() {
		for msg := range ch {
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

func (t *Topic) publish(msg []byte, logger Logger) error {
	t.RLock()
	defer t.RUnlock()

	if len(t.Subscribers) == 0 {
		logger.Warn(fmt.Sprintf("no active subscribers for topic %s", t.Name))
		return ErrNoActiveSubscribers
	}

	activeSubscribers := t.getActiveSubscribers()
	if len(activeSubscribers) == 0 {
		logger.Warn(fmt.Sprintf("no active subscribers for topic %s", t.Name))
		return ErrNoActiveSubscribers
	}

	selectedSubscriber := activeSubscribers[rand.Intn(len(activeSubscribers))]

	logger.Info(fmt.Sprintf("publish to topic %s, subscriber: %s, message: %s", t.Name, selectedSubscriber.ID.String(), string(msg)))

	selectedSubscriber.Ch <- &Message{
		ID:       uuid.New(),
		Value:    msg,
		Retry:    3,
		CreateAt: time.Now(),
		UpdateAt: time.Now(),
	}
	return nil
}

func (t *Topic) unsubscribe(id uuid.UUID) error {
	t.Lock()
	defer t.Unlock()

	for i, sub := range t.Subscribers {
		if sub.ID == id {
			close(sub.Ch)
			// 將訂閱者標記為非活躍，而不是直接刪除
			t.Subscribers[i].Active = false
			t.renewActiveSubscriber()
			return nil
		}
	}

	return ErrTopicSubScriberNotFound
}

func (t *Topic) renewActiveSubscriber() {
	t.Lock()
	defer t.Unlock()

	activeSubscribers := make([]*Subscriber, 0, len(t.Subscribers))
	for _, s := range t.Subscribers {
		if s.Active {
			activeSubscribers = append(activeSubscribers, s)
		}
	}
	t.ActiveSubscribers = activeSubscribers
}

func (t *Topic) getActiveSubscribers() []*Subscriber {
	t.Lock()
	defer t.Unlock()

	return t.ActiveSubscribers
}

func (t *Topic) Close() error {
	t.Lock()
	defer t.Unlock()

	for _, sub := range t.Subscribers {
		close(sub.Ch)
	}
	t.Subscribers = nil
	t.ActiveSubscribers = nil
	return nil
}
