package broker

import (
	"context"
	"encoding/json"
	"fmt"
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

type RedisBrokerOptions struct {
	Prefix string
}

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

func (b *RedisBroker) Push(ctx context.Context, msg *kabaka.Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}

	queueKey := b.prefix + "main_queue"
	msgKey := b.prefix + "messages"
	
	err = pushScript.Run(ctx, b.client, []string{msgKey, queueKey}, msg.Id, data).Err()
	if err == nil {
		b.client.Publish(ctx, b.prefix+"notify", "push")
	}
	return err
}

func (b *RedisBroker) PushDelayed(ctx context.Context, msg *kabaka.Message, delay time.Duration) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}

	delayedKey := b.prefix + "delayed_queue"
	msgKey := b.prefix + "messages"
	score := time.Now().Add(delay).UnixMilli()

	err = pushDelayedScript.Run(ctx, b.client, []string{msgKey, delayedKey}, msg.Id, data, score).Err()
	if err == nil {
		b.client.Publish(ctx, b.prefix+"notify", score)
	}
	return err
}

func (b *RedisBroker) Watch(ctx context.Context) (<-chan *kabaka.Task, error) {
	b.moverOnce.Do(func() {
		go b.listenMover()
		go b.startPoller()
	})
	return b.watchCh, nil
}

func (b *RedisBroker) Finish(ctx context.Context, msg *kabaka.Message, processErr error, duration time.Duration) error {
	processingKey := b.prefix + "processing_queue"
	msgKey := b.prefix + "messages"
	
	b.client.LRem(ctx, processingKey, 1, msg.Id)
	return b.client.HDel(ctx, msgKey, msg.Id).Err()
}

func (b *RedisBroker) listenMover() {
	queueKey := b.prefix + "main_queue"
	processingKey := b.prefix + "processing_queue"
	msgKey := b.prefix + "messages"

	for {
		select {
		case <-b.ctx.Done():
			return
		default:
			// Move one message from main to processing
			res, err := b.client.LMove(b.ctx, queueKey, processingKey, "RIGHT", "LEFT").Result()
			if err != nil {
				if err != redis.Nil {
					time.Sleep(100 * time.Millisecond)
				} else {
					// Queue empty, wait for notification
					select {
					case <-b.anyNotifyCh:
					case <-time.After(5 * time.Second):
					case <-b.ctx.Done():
						return
					}
				}
				continue
			}

			// Fetch message and dispatch
			data, err := b.client.HGet(b.ctx, msgKey, res).Result()
			if err == nil {
				var msg kabaka.Message
				if err := json.Unmarshal([]byte(data), &msg); err == nil {
					select {
					case b.watchCh <- &kabaka.Task{InternalName: msg.InternalName, Message: &msg}:
					case <-b.ctx.Done():
						return
					}
				}
			}
		}
	}
}

func (b *RedisBroker) startPoller() {
	delayedKey := b.prefix + "delayed_queue"
	queueKey := b.prefix + "main_queue"

	for {
		now := time.Now().UnixMilli()
		moveDelayedScript.Run(b.ctx, b.client, []string{delayedKey, queueKey}, now)

		select {
		case <-b.ctx.Done():
			return
		case <-time.After(1 * time.Second):
		}
	}
}

func (b *RedisBroker) Register(ctx context.Context, topic string) error {
	// For shared queue, Register might just be a no-op or used for metadata
	return nil
}

func (b *RedisBroker) Unregister(ctx context.Context, topic string) error {
	return nil
}

func (b *RedisBroker) UnregisterAndCleanup(ctx context.Context, topic string) error {
	return nil // Use global cleanup if needed
}

func (b *RedisBroker) SetMetadata(ctx context.Context, name string, internalName string) error {
	key := b.prefix + "meta:topics"
	return b.client.HSet(ctx, key, name, internalName).Err()
}

func (b *RedisBroker) GetMetadata(ctx context.Context, name string) (string, error) {
	key := b.prefix + "meta:topics"
	return b.client.HGet(ctx, key, name).Result()
}

func (b *RedisBroker) Len(ctx context.Context) (int64, error) {
	return b.client.LLen(ctx, b.prefix+"main_queue").Result()
}

func (b *RedisBroker) Close() error {
	b.cancel()
	return b.client.Close()
}
