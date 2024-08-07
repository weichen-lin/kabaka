package kabaka

import (
	"errors"
	"sync"

	"github.com/google/uuid"
)

type Kabaka struct {
	sync.RWMutex
	topics map[string]*Topic
	logger Logger
}

func NewKabaka(config *Config) *Kabaka {
	return &Kabaka{
		topics: make(map[string]*Topic),
		logger: config.logger,
	}
}

func (t *Kabaka) CreateTopic(name string) error {
	if _, ok := t.topics[name]; ok {
		return errors.New("topic already exists")
	}

	topic := &Topic{
		Name:        name,
		Subscribers: make([]*Subscriber, 0),
		Messages:   NewRingBuffer(20),
	}

	t.topics[name] = topic

	return nil
}

func (t *Kabaka) Subscribe(name string, handler HandleFunc) (uuid.UUID, error) {
	topic, ok := t.topics[name]
	if !ok {
		return uuid.Nil, ErrTopicNotFound
	}

	return topic.subscribe(handler, t.logger), nil
}

func (t *Kabaka) Publish(name string, msg []byte) error {
	topic, ok := t.topics[name]
	if !ok {
		return ErrTopicNotFound
	}

	err := topic.publish(msg, t.logger)
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

func (t *Kabaka) Close() error {
	t.Lock()
	defer t.Unlock()

	for _, topic := range t.topics {
		if err := topic.Close(); err != nil {
			return err
		}
	}
	t.topics = nil
	return nil
}
