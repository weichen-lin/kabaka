package kabaka

import (
	"sync"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/propagation"
)

type Kabaka struct {
	sync.RWMutex
	topics map[string]*Topic
	logger Logger
}

var defaultTraceName = "kabaka"
var version = "1.0.0"

func NewKabaka(config *Config) *Kabaka {
	if config.Logger == nil {
		config.Logger = nil
	}

	return &Kabaka{
		topics: make(map[string]*Topic),
		logger: config.Logger,
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
		subscribers: make(map[string]*subscriber),
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

func (t *Kabaka) Publish(name string, message []byte, propagation propagation.TextMapCarrier) error {
	topic, ok := t.topics[name]
	if !ok {
		return ErrTopicNotFound
	}

	msg := GenerateTraceMessage(topic.Name, message, propagation)

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
