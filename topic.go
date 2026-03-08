package kabaka

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"time"

	"github.com/invopop/jsonschema"
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
	schemaType     string

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

func WithSchema(v interface{}) Option {
	return func(t *Topic) {
		switch s := v.(type) {
		case string:
			t.schema = s
			t.schemaType = "json"
		default:
			// Reflect the struct to JSON Schema
			sch := jsonschema.Reflect(v)

			// 將結構體轉為 map 以安全地移除 $schema 欄位
			var m map[string]interface{}
			schData, _ := json.Marshal(sch)
			if err := json.Unmarshal(schData, &m); err == nil {
				delete(m, "$schema")
				finalData, _ := json.Marshal(m)
				t.schema = string(finalData)
				t.schemaType = "json"
			}
		}
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
		Schema:         t.schema,
		SchemaType:     t.schemaType,
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
