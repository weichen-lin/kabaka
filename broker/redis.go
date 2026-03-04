package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/weichen-lin/kabaka"
)

type RedisBroker struct {
	client      *redis.Client
	prefix      string
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	pollers     map[string]chan struct{}
	allTopics   []string
	notifyOnce  sync.Once
	moverOnce   sync.Once
	anyNotifyCh chan struct{}
	watchCh     chan *kabaka.Task
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

	ctx, cancel := context.WithCancel(context.Background())
	return &RedisBroker{
		client:      client,
		prefix:      prefix,
		ctx:         ctx,
		cancel:      cancel,
		pollers:     make(map[string]chan struct{}),
		anyNotifyCh: make(chan struct{}, 1),
		watchCh:     make(chan *kabaka.Task, 100),
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
		client:      client,
		prefix:      prefix,
		ctx:         ctx,
		cancel:      cancel,
		pollers:     make(map[string]chan struct{}),
		anyNotifyCh: make(chan struct{}, 1),
		watchCh:     make(chan *kabaka.Task, 100),
	}
}

var pushScript = redis.NewScript(`
	redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
	redis.call('RPUSH', KEYS[2], ARGV[1])
	return 1
`)

var pushDelayedScript = redis.NewScript(`
	redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
	redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
	return 1
`)

var moveDelayedScript = redis.NewScript(`
	local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 100)
	if #ids > 0 then
		for i, id in ipairs(ids) do
			redis.call('RPUSH', KEYS[2], id)
			redis.call('ZREM', KEYS[1], id)
		end
	end
	return #ids
`)

func (b *RedisBroker) Push(ctx context.Context, topic string, msg *kabaka.Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	queueKey := b.prefix + topic
	msgKey := b.prefix + topic + ":messages"
	
	err = pushScript.Run(ctx, b.client, []string{msgKey, queueKey}, msg.Id, data).Err()
	if err == nil {
		// Notify mover that there is a new message
		b.client.Publish(ctx, b.prefix+topic+":notify", "push")
	}
	return err
}

func (b *RedisBroker) PushDelayed(ctx context.Context, topic string, msg *kabaka.Message, delay time.Duration) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	delayedKey := b.prefix + topic + ":delayed"
	msgKey := b.prefix + topic + ":messages"
	score := time.Now().Add(delay).UnixMilli()

	// Check if this new message will be the new head of the delayed queue
	head, _ := b.client.ZRangeWithScores(ctx, delayedKey, 0, 0).Result()
	isNewHead := len(head) == 0 || float64(score) < head[0].Score

	err = pushDelayedScript.Run(ctx, b.client, []string{msgKey, delayedKey}, msg.Id, data, score).Err()

	if err == nil && isNewHead {
		// Notify the poller to wake up and re-calculate sleep time
		notifyKey := b.prefix + topic + ":notify"
		b.client.Publish(ctx, notifyKey, score)
	}

	return err
}

func (b *RedisBroker) Register(ctx context.Context, topic string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.pollers[topic]; ok {
		return nil // Already registered
	}

	ch := make(chan struct{}, 1)
	b.pollers[topic] = ch
	b.allTopics = append(b.allTopics, topic)

	go b.startPoller(topic, ch)

	b.notifyOnce.Do(func() {
		go b.listenNotifications()
	})

	b.moverOnce.Do(func() {
		go b.listenMover()
	})

	return nil
}

func (b *RedisBroker) Unregister(ctx context.Context, topic string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// 1. Close the poller channel to stop startPoller goroutine
	if ch, ok := b.pollers[topic]; ok {
		close(ch)
		delete(b.pollers, topic)
	}

	// 2. Remove from allTopics list
	for i, t := range b.allTopics {
		if t == topic {
			b.allTopics = append(b.allTopics[:i], b.allTopics[i+1:]...)
			break
		}
	}

	return nil
}

func (b *RedisBroker) UnregisterAndCleanup(ctx context.Context, topic string) error {
	// First unregister the topic
	if err := b.Unregister(ctx, topic); err != nil {
		return err
	}

	// Then clean up Redis resources
	processingKey := b.prefix + topic + ":processing"
	mainKey := b.prefix + topic
	delayedKey := b.prefix + topic + ":delayed"
	msgKey := b.prefix + topic + ":messages"

	return b.client.Del(ctx, processingKey, mainKey, delayedKey, msgKey).Err()
}

func (b *RedisBroker) listenNotifications() {
	pattern := b.prefix + "*:notify"
	pubsub := b.client.PSubscribe(b.ctx, pattern)
	defer pubsub.Close()

	ch := pubsub.Channel()
	for {
		select {
		case <-b.ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			// Extract topic from channel name "prefix:topic:notify"
			topic := strings.TrimPrefix(msg.Channel, b.prefix)
			topic = strings.TrimSuffix(topic, ":notify")

			b.mu.Lock()
			if notifyCh, ok := b.pollers[topic]; ok {
				select {
				case notifyCh <- struct{}{}:
				default:
				}
			}
			// Wake up the global mover
			select {
			case b.anyNotifyCh <- struct{}{}:
			default:
			}
			b.mu.Unlock()
		}
	}
}

func (b *RedisBroker) listenMover() {
	for {
		select {
		case <-b.ctx.Done():
			return
		default:
			b.mu.Lock()
			topics := make([]string, len(b.allTopics))
			copy(topics, b.allTopics)
			b.mu.Unlock()

			foundAny := false
			for _, topic := range topics {
				key := b.prefix + topic
				processingKey := key + ":processing"
				msgKey := key + ":messages"

				// Attempt non-blocking move (moves ID from queue to processing)
				res, err := b.client.LMove(b.ctx, key, processingKey, "RIGHT", "LEFT").Result()
				if err != nil {
					// redis.Nil means the queue is empty, which is expected
					if err != redis.Nil {
						// Log or handle unexpected errors here if needed
					}
					continue
				}

				// Fetch full message from hash warehouse using the ID
				data, err := b.client.HGet(b.ctx, msgKey, res).Result()
				if err != nil {
					// Message ID exists in queue but not in warehouse? 
					// Should not happen with Lua push, but handle it.
					continue
				}

				var msg kabaka.Message
				if err := json.Unmarshal([]byte(data), &msg); err == nil {
					select {
					case b.watchCh <- &kabaka.Task{Topic: topic, Message: &msg}:
						foundAny = true
					case <-b.ctx.Done():
						return
					}
				}
			}

			if !foundAny {
				// No messages in any topic, sleep until notified or timeout
				select {
				case <-b.anyNotifyCh:
				case <-time.After(5 * time.Second):
				case <-b.ctx.Done():
					return
				}
			}
		}
	}
}

func (b *RedisBroker) startPoller(topic string, ch chan struct{}) {
	delayedKey := b.prefix + topic + ":delayed"
	queueKey := b.prefix + topic

	for {
		// 1. Move expired messages
		now := time.Now().UnixMilli()
		res, err := moveDelayedScript.Run(b.ctx, b.client, []string{delayedKey, queueKey}, now).Int64()
		if err == nil && res > 0 {
			// Notify mover
			select {
			case b.anyNotifyCh <- struct{}{}:
			default:
			}
		}

		// 2. Get the next message's execution time to determine sleep duration
		res2, err := b.client.ZRangeWithScores(b.ctx, delayedKey, 0, 0).Result()

		var waitTime time.Duration
		if err == nil && len(res2) > 0 {
			diff := int64(res2[0].Score) - time.Now().UnixMilli()
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

func (b *RedisBroker) Watch(ctx context.Context, topics ...string) (<-chan *kabaka.Task, error) {
	return b.watchCh, nil
}

// Finish handles message completion. It removes the message from the processing queue.
func (b *RedisBroker) Finish(ctx context.Context, topic string, msg *kabaka.Message, processErr error, duration time.Duration) error {
	// 1. Remove from processing queue (Ack) using ID
	processingKey := b.prefix + topic + ":processing"
	err := b.client.LRem(ctx, processingKey, 1, msg.Id).Err()
	if err != nil {
		return err
	}

	// 2. Remove from hash warehouse
	msgKey := b.prefix + topic + ":messages"
	return b.client.HDel(ctx, msgKey, msg.Id).Err()
}

func (b *RedisBroker) Len(ctx context.Context, topic string) (int64, error) {
	key := b.prefix + topic
	return b.client.LLen(ctx, key).Result()
}

func (b *RedisBroker) Close() error {
	b.cancel()
	return b.client.Close()
}
