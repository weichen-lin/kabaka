package kabaka

import (
	"context"
	"time"
)

// Broker defines the interface for message storage and retrieval.
// This allows Kabaka to switch between In-Memory, Redis, or other backends.
type Broker interface {
	// Push adds a message to the specified topic.
	Push(ctx context.Context, topic string, msg *Message) error

	// PushDelayed adds a message to the specified topic with a delay.
	PushDelayed(ctx context.Context, topic string, msg *Message, delay time.Duration) error

	// Pop retrieves a message from the specified topic.
	// It should block until a message is available or the context is cancelled.
	Pop(ctx context.Context, topic string) (*Message, error)

	// Ack acknowledges that a message has been successfully processed.
	Ack(ctx context.Context, topic string, msg *Message) error

	// Len returns the number of messages in the specified topic.
	Len(ctx context.Context, topic string) (int64, error)

	// IncSuccess increments the success counter for the topic.
	IncSuccess(ctx context.Context, topic string) error
	// IncFailed increments the failure counter for the topic.
	IncFailed(ctx context.Context, topic string) error
	// IncRetried increments the retry counter for the topic.
	IncRetried(ctx context.Context, topic string) error
	// RecordDuration records the processing duration of a task.
	RecordDuration(ctx context.Context, topic string, d time.Duration) error
	// GetStats returns the success, failed, retried counts, p95, and p99 for the topic.
	GetStats(ctx context.Context, topic string) (success, failed, retried int64, p95, p99 float64, err error)
	// ResetStats resets the metrics for the topic.
	ResetStats(ctx context.Context, topic string) error

	// Close cleans up broker resources.
	Close() error
}
