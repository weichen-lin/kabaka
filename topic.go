package kabaka

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/weichen-lin/kabaka/broker"
)

type Topic struct {
	Name         string
	InternalName string
	broker       broker.Broker
	handler      HandleFunc

	// Configuration
	maxRetries     int
	retryDelay     time.Duration
	processTimeout time.Duration
	schema         string

	// State
	Paused atomic.Bool

	// Metrics
	stats *TopicStats
}

type Option func(*Topic)

func newTopic(name, internalName string, b broker.Broker, handler HandleFunc, options ...Option) *Topic {
	t := &Topic{
		Name:           name,
		InternalName:   internalName,
		broker:         b,
		handler:        handler,
		maxRetries:     3,
		retryDelay:     1 * time.Second,
		processTimeout: 30 * time.Second,
		stats:          &TopicStats{},
	}

	for _, opt := range options {
		opt(t)
	}

	return t
}

func WithMaxRetries(n int) Option {
	return func(t *Topic) {
		t.maxRetries = n
	}
}

func WithRetryDelay(d time.Duration) Option {
	return func(t *Topic) {
		t.retryDelay = d
	}
}

func WithProcessTimeout(d time.Duration) Option {
	return func(t *Topic) {
		t.processTimeout = d
	}
}

func WithSchema(schema string) Option {
	return func(t *Topic) {
		t.schema = schema
	}
}

// ToMetadata creates a TopicMetadata from the Topic configuration.
func (t *Topic) ToMetadata() *broker.TopicMetadata {
	return &broker.TopicMetadata{
		Name:           t.Name,
		InternalName:   t.InternalName,
		CreatedAt:      time.Now(),
		ProcessTimeout: t.processTimeout,
		RetryDelay:     t.retryDelay,
		MaxRetries:     t.maxRetries,
	}
}

// CreateTopic registers a new topic with a handler and options.
func (k *Kabaka) CreateTopic(name string, handler HandleFunc, options ...Option) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	internalName := k.generateInternalName(name)
	if _, ok := k.topics[internalName]; ok {
		return ErrTopicAlreadyCreated
	}

	ctx, cancel := context.WithTimeout(context.Background(), k.brokerTimeout)
	defer cancel()

	topic := newTopic(name, internalName, k.broker, handler, options...)
	meta := topic.ToMetadata()

	if err := k.broker.Register(ctx, meta); err != nil {
		return err
	}

	k.topics[internalName] = topic

	// Cache the metadata immediately after topic creation (already holding lock)
	k.metaCache[name] = &metaCacheEntry{
		metadata:  meta,
		expiresAt: time.Now().Add(k.metaCacheTTL),
	}

	k.logger.Info(&LogMessage{
		TopicName:     name,
		Action:        Subscribe,
		Message:       "Topic created",
		MessageStatus: Success,
		CreatedAt:     time.Now(),
	})

	return nil
}
