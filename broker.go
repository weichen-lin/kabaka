package kabaka

import (
	"context"
)

// Broker defines the interface for message storage and retrieval.
// This allows Kabaka to switch between In-Memory, Redis, or other backends.
type Broker interface {
	// Push adds a message to the specified topic.
	Push(ctx context.Context, topic string, msg *Message) error

	// Pop retrieves a message from the specified topic.
	// It should block until a message is available or the context is cancelled.
	Pop(ctx context.Context, topic string) (*Message, error)

	// Ack acknowledges that a message has been successfully processed.
	Ack(ctx context.Context, topic string, msg *Message) error

	// Len returns the number of messages in the specified topic.
	Len(ctx context.Context, topic string) (int64, error)

	// Close cleans up broker resources.
	Close() error
}
