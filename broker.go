package kabaka

import (
	"context"
	"time"
)

type Task struct {
	InternalName string
	Message      *Message
}

type Broker interface {
	// Topic/Metadata management
	Register(ctx context.Context, topic string) error
	Unregister(ctx context.Context, topic string) error
	UnregisterAndCleanup(ctx context.Context, topic string) error
	SetMetadata(ctx context.Context, name string, internalName string) error
	GetMetadata(ctx context.Context, name string) (string, error)

	// Shared queue operations
	Push(ctx context.Context, msg *Message) error
	PushDelayed(ctx context.Context, msg *Message, delay time.Duration) error
	Watch(ctx context.Context) (<-chan *Task, error)
	Finish(ctx context.Context, msg *Message, processErr error, duration time.Duration) error
	
	// Stats and management
	Len(ctx context.Context) (int64, error)
	Close() error
}
