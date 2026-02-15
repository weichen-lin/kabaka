package kabaka

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisBroker struct {
	client *redis.Client
	prefix string
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	pollers map[string]bool
}

const redisMaxSamples = 1000

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

	ctx, cancel := context.WithCancel(context.Background())
	return &RedisBroker{
		client:  client,
		prefix:  prefix,
		ctx:     ctx,
		cancel:  cancel,
		pollers: make(map[string]bool),
	}
}

// NewRedisBrokerWithClient creates a new RedisBroker with an existing redis client.
func NewRedisBrokerWithClient(client *redis.Client, opts ...RedisBrokerOptions) *RedisBroker {
	prefix := "kabaka:"
	if len(opts) > 0 && opts[0].Prefix != "" {
		prefix = opts[0].Prefix
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &RedisBroker{
		client:  client,
		prefix:  prefix,
		ctx:     ctx,
		cancel:  cancel,
		pollers: make(map[string]bool),
	}
}

var moveDelayedScript = redis.NewScript(`
	local val = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 100)
	if #val > 0 then
		for i, v in ipairs(val) do
			redis.call('RPUSH', KEYS[2], v)
			redis.call('ZREM', KEYS[1], v)
		end
	end
	return #val
`)

func (b *RedisBroker) Push(ctx context.Context, topic string, msg *Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	key := b.prefix + topic
	b.ensurePoller(topic)
	return b.client.RPush(ctx, key, data).Err()
}

func (b *RedisBroker) PushDelayed(ctx context.Context, topic string, msg *Message, delay time.Duration) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	key := b.prefix + topic + ":delayed"
	score := time.Now().Add(delay).UnixMilli()

	b.ensurePoller(topic)

	// Check if this new message will be the new head of the delayed queue
	head, _ := b.client.ZRangeWithScores(ctx, key, 0, 0).Result()
	isNewHead := len(head) == 0 || float64(score) < head[0].Score

	err = b.client.ZAdd(ctx, key, redis.Z{
		Score:  float64(score),
		Member: data,
	}).Err()

	if err == nil && isNewHead {
		// Notify the poller to wake up and re-calculate sleep time
		notifyKey := b.prefix + topic + ":notify"
		b.client.Publish(ctx, notifyKey, score)
	}

	return err
}

func (b *RedisBroker) ensurePoller(topic string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.pollers[topic] {
		return
	}

	b.pollers[topic] = true
	go b.startPoller(topic)
}

func (b *RedisBroker) startPoller(topic string) {
	delayedKey := b.prefix + topic + ":delayed"
	queueKey := b.prefix + topic
	notifyKey := b.prefix + topic + ":notify"

	pubsub := b.client.Subscribe(b.ctx, notifyKey)
	defer pubsub.Close()

	ch := pubsub.Channel()

	for {
		// 1. Move expired messages
		now := time.Now().UnixMilli()
		_, err := moveDelayedScript.Run(b.ctx, b.client, []string{delayedKey, queueKey}, now).Result()
		if err != nil && err != context.Canceled {
			time.Sleep(time.Second) // Error backoff
			continue
		}

		// 2. Get the next message's execution time to determine sleep duration
		res, err := b.client.ZRangeWithScores(b.ctx, delayedKey, 0, 0).Result()
		
		var waitTime time.Duration
		if err == nil && len(res) > 0 {
			diff := int64(res[0].Score) - time.Now().UnixMilli()
			if diff > 0 {
				waitTime = time.Duration(diff) * time.Millisecond
			} else {
				waitTime = 0 // Should process immediately
			}
		} else {
			waitTime = 10 * time.Minute // Long sleep if no tasks
		}

		// 3. Wait for either: next task time, new task notification, or context cancellation
		if waitTime > 0 {
			timer := time.NewTimer(waitTime)
			select {
			case <-b.ctx.Done():
				timer.Stop()
				return
			case <-ch:
				// New task added (possibly earlier), wake up to re-check
				timer.Stop()
			case <-timer.C:
				// Time reached
			}
		}
	}
}

func (b *RedisBroker) Pop(ctx context.Context, topic string) (*Message, error) {
	key := b.prefix + topic
	processingKey := key + ":processing"

	b.ensurePoller(topic)
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

func (b *RedisBroker) IncSuccess(ctx context.Context, topic string) error {
	key := b.prefix + topic + ":stats"
	return b.client.HIncrBy(ctx, key, "success", 1).Err()
}

func (b *RedisBroker) IncFailed(ctx context.Context, topic string) error {
	key := b.prefix + topic + ":stats"
	return b.client.HIncrBy(ctx, key, "failed", 1).Err()
}

func (b *RedisBroker) IncRetried(ctx context.Context, topic string) error {
	key := b.prefix + topic + ":stats"
	return b.client.HIncrBy(ctx, key, "retried", 1).Err()
}

func (b *RedisBroker) RecordDuration(ctx context.Context, topic string, d time.Duration) error {
	key := b.prefix + topic + ":durations"
	ms := d.Milliseconds()

	pipe := b.client.Pipeline()
	pipe.LPush(ctx, key, ms)
	pipe.LTrim(ctx, key, 0, redisMaxSamples-1)
	_, err := pipe.Exec(ctx)
	return err
}

func (b *RedisBroker) GetStats(ctx context.Context, topic string) (success, failed, retried int64, p95, p99 float64, err error) {
	key := b.prefix + topic + ":stats"
	res, err := b.client.HGetAll(ctx, key).Result()
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}

	parse := func(s string) int64 {
		var n int64
		fmt.Sscanf(s, "%d", &n)
		return n
	}

	// Fetch durations for P95/P99
	durationKey := b.prefix + topic + ":durations"
	durationStrings, _ := b.client.LRange(ctx, durationKey, 0, -1).Result()

	samples := make([]float64, 0, len(durationStrings))
	for _, s := range durationStrings {
		if val, err := strconv.ParseFloat(s, 64); err == nil {
			samples = append(samples, val)
		}
	}
	sort.Float64s(samples)

	calcPercentile := func(p float64) float64 {
		if len(samples) == 0 {
			return 0
		}
		idx := int(float64(len(samples)) * p)
		if idx >= len(samples) {
			idx = len(samples) - 1
		}
		return samples[idx]
	}

	return parse(res["success"]), parse(res["failed"]), parse(res["retried"]), calcPercentile(0.95), calcPercentile(0.99), nil
}

func (b *RedisBroker) ResetStats(ctx context.Context, topic string) error {
	statsKey := b.prefix + topic + ":stats"
	durationKey := b.prefix + topic + ":durations"
	return b.client.Del(ctx, statsKey, durationKey).Err()
}

func (b *RedisBroker) Close() error {
	b.cancel()
	return b.client.Close()
}
