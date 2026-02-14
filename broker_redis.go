package kabaka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type RedisBroker struct {
	client *redis.Client
	prefix string
}

// RedisBrokerOptions defines the options for RedisBroker.
type RedisBrokerOptions struct {
	Prefix string
}

// NewRedisBroker creates a new RedisBroker with a connection string.
func NewRedisBroker(addr string, password string, db int, opts ...RedisBrokerOptions) *RedisBroker {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	prefix := "kabaka:"
	if len(opts) > 0 && opts[0].Prefix != "" {
		prefix = opts[0].Prefix
	}

	return &RedisBroker{
		client: client,
		prefix: prefix,
	}
}

// NewRedisBrokerWithClient creates a new RedisBroker with an existing redis client.
func NewRedisBrokerWithClient(client *redis.Client, opts ...RedisBrokerOptions) *RedisBroker {
	prefix := "kabaka:"
	if len(opts) > 0 && opts[0].Prefix != "" {
		prefix = opts[0].Prefix
	}

	return &RedisBroker{
		client: client,
		prefix: prefix,
	}
}

func (b *RedisBroker) Push(ctx context.Context, topic string, msg *Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	key := b.prefix + topic
	return b.client.RPush(ctx, key, data).Err()
}

func (b *RedisBroker) Pop(ctx context.Context, topic string) (*Message, error) {
	key := b.prefix + topic
	processingKey := key + ":processing"

	// BLMove blocks until a message is available and moves it to the processing queue.
	// This ensures at-least-once delivery.
	res, err := b.client.BLMove(ctx, key, processingKey, "RIGHT", "LEFT", 0).Result()
	if err != nil {
		return nil, err
	}

	var msg Message
	if err := json.Unmarshal([]byte(res), &msg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	return &msg, nil
}

func (b *RedisBroker) Ack(ctx context.Context, topic string, msg *Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message for ack: %w", err)
	}

	key := b.prefix + topic + ":processing"
	// Remove the specific message from the processing queue
	return b.client.LRem(ctx, key, 1, data).Err()
}

func (b *RedisBroker) Len(ctx context.Context, topic string) (int64, error) {
	key := b.prefix + topic
	return b.client.LLen(ctx, key).Result()
}

func (b *RedisBroker) Close() error {
	return b.client.Close()
}
