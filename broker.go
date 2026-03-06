package kabaka

import (
	"context"
	"time"
)

// QueueStats is a point-in-time snapshot of all queue lengths from the broker.
type QueueStats struct {
	Pending    int64 // tasks in the main queue waiting to be picked up
	Delayed    int64 // tasks in the delayed queue waiting for their schedule time
	Processing int64 // tasks currently being processed (in-flight)
}

type Task struct {
	InternalName string
	Message      *Message
}

type Broker interface {
	// Topic/Metadata management
	Register(ctx context.Context, meta *TopicMetadata) error
	Unregister(ctx context.Context, topic string) error
	UnregisterAndCleanup(ctx context.Context, topic string) error
	GetTopicMetadata(ctx context.Context, name string) (*TopicMetadata, error)

	// Shared queue operations
	Push(ctx context.Context, msg *Message) error
	PushDelayed(ctx context.Context, msg *Message, delay time.Duration) error
	Watch(ctx context.Context) (<-chan *Task, error)
	Finish(ctx context.Context, msg *Message, processErr error, duration time.Duration) error

	// Stats and management
	QueueStats(ctx context.Context) (QueueStats, error)
	TopicQueueStats(ctx context.Context, internalName string) (QueueStats, error)
	Close() error
}
