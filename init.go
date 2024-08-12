package kabaka

import (
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
	t.Lock()
	defer t.Unlock()

	if _, ok := t.topics[name]; ok {
		return ErrTopicAlreadyCreated
	}

	topic := &Topic{
		Name:        name,
		Subscribers: make([]*Subscriber, 0),
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

func (t *Kabaka) Publish(name string, msg []byte) error {
	topic, ok := t.topics[name]
	if !ok {
		return ErrTopicNotFound
	}

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
