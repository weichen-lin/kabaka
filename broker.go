package kabaka

import (
	"context"
	"time"
)

type Task struct {
	Topic   string
	Message *Message
}

type Broker interface {
	Register(ctx context.Context, topic string) error
	Unregister(ctx context.Context, topic string) error
	UnregisterAndCleanup(ctx context.Context, topic string) error
	Push(ctx context.Context, topic string, msg *Message) error
	PushDelayed(ctx context.Context, topic string, msg *Message, delay time.Duration) error
	Watch(ctx context.Context, topics ...string) (<-chan *Task, error)
	Finish(ctx context.Context, topic string, msg *Message, processErr error, duration time.Duration) error
	Len(ctx context.Context, topic string) (int64, error)
	Close() error

	// Metadata management
	SetMetadata(ctx context.Context, name string, internalName string) error
	GetMetadata(ctx context.Context, name string) (string, error)
}
