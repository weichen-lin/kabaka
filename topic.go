package kabaka

import (
	"time"
)

type Topic struct {
	Name         string
	InternalName string
	broker       Broker
	handler      HandleFunc

	// Configuration
	maxRetries     int
	retryDelay     time.Duration
	processTimeout time.Duration
	schema         string
}

type Option func(*Topic)

func newTopic(name, internalName string, broker Broker, handler HandleFunc, options ...Option) *Topic {
	t := &Topic{
		Name:           name,
		InternalName:   internalName,
		broker:         broker,
		handler:        handler,
		maxRetries:     3,
		retryDelay:     1 * time.Second,
		processTimeout: 30 * time.Second,
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
