package kabaka

import (
	"sync"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/propagation"
)

type Kabaka struct {
	sync.RWMutex
	topics  map[string]*Topic
	options *Options
}

var defaultTraceName = "kabaka"
var version = "1.0.0"

func NewKabaka(options *Options) *Kabaka {
	if options == nil {
		options = getDefaultOptions()
	}

	if options.Logger == nil {
		options.Logger = defaultLogger()
	}

	setKabakaLogger(options.Logger)

	return &Kabaka{
		topics:  make(map[string]*Topic),
		options: options,
	}
}

func (k *Kabaka) CreateTopic(name string) error {
	k.Lock()
	defer k.Unlock()

	if _, ok := k.topics[name]; ok {
		return ErrTopicAlreadyCreated
	}

	topic := &Topic{
		Name:              name,
		subscribers:       make(map[string]*subscriber),
		activeSubscribers: make([]uuid.UUID, 0),
		bufferSize:        k.options.BufferSize,
		maxRetries:        k.options.DefaultMaxRetries,
		retryDelay:        k.options.DefaultRetryDelay,
		processTimeout:    k.options.DefaultProcessTimeout,
	}

	k.topics[name] = topic

	return nil
}

func (k *Kabaka) Subscribe(name string, handler HandleFunc) (uuid.UUID, error) {
	k.RLock()
	defer k.RUnlock()

	topic, ok := k.topics[name]
	if !ok {
		return uuid.Nil, ErrTopicNotFound
	}

	return topic.subscribe(handler), nil
}

func (k *Kabaka) Publish(name string, message []byte, propagation propagation.TextMapCarrier) error {
	topic, ok := k.topics[name]
	if !ok {
		return ErrTopicNotFound
	}

	msg := topic.generateTraceMessage(topic.Name, message, propagation)

	topic.injectCtx(msg)

	err := topic.publish(msg)
	if err != nil {
		return err
	}

	return nil
}

func (k *Kabaka) UnSubscribe(name string, id uuid.UUID) error {
	topic, ok := k.topics[name]
	if !ok {
		return ErrTopicNotFound
	}

	err := topic.unsubscribe(id)
	if err != nil {
		return err
	}

	return nil
}

func (k *Kabaka) CloseTopic(name string) error {
	k.Lock()
	defer k.Unlock()

	topic, ok := k.topics[name]
	if !ok {
		return ErrTopicNotFound
	}

	topic.closeTopic()

	delete(k.topics, name)
	return nil
}

func (k *Kabaka) Close() error {
	k.Lock()
	defer k.Unlock()

	for _, topic := range k.topics {
		topic.closeTopic()
	}
	k.topics = nil
	return nil
}
