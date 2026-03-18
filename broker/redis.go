package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisBroker struct {
	client      *redis.Client
	prefix      string
	ctx         context.Context
	cancel      context.CancelFunc
	moverOnce   sync.Once
	anyNotifyCh chan struct{}
	watchCh     chan *Task
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
		anyNotifyCh: make(chan struct{}, 1),
		watchCh:     make(chan *Task, 100),
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
		anyNotifyCh: make(chan struct{}, 1),
		watchCh:     make(chan *Task, 100),
	}
}

var popAndMoveScript = redis.NewScript(`
	local msgId = redis.call('LPOP', KEYS[1])
	if msgId then
		redis.call('ZADD', KEYS[2], ARGV[1], msgId)
	end
	return msgId
`)

var pushScript = redis.NewScript(`
	redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
	redis.call('HSET', KEYS[4], ARGV[1], ARGV[3])
	redis.call('RPUSH', KEYS[2], ARGV[1])
	redis.call('HINCRBY', KEYS[3], 'pending', 1)
	return 1
`)

var pushDelayedScript = redis.NewScript(`
	redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
	redis.call('HSET', KEYS[4], ARGV[1], ARGV[4])
	redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
	redis.call('HINCRBY', KEYS[3], 'delayed', 1)
	return 1
`)

var moveDelayedScript = redis.NewScript(`
	local msgKey = KEYS[3]
	local statsPrefix = ARGV[2]
	local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 100)
	if #ids > 0 then
		for i, id in ipairs(ids) do
			redis.call('RPUSH', KEYS[2], id)
			redis.call('ZREM', KEYS[1], id)
			
			-- Get message and extract InternalName for stats
			local msgData = redis.call('HGET', msgKey, id)
			if msgData then
				local internalName = string.match(msgData, '"InternalName":"([^"]+)"')
				if internalName then
					local statsKey = statsPrefix .. internalName
					redis.call('HINCRBY', statsKey, 'delayed', -1)
					redis.call('HINCRBY', statsKey, 'pending', 1)
				end
			end
			end
	end
	return #ids
`)

var redeliverTimeoutScript = redis.NewScript(`
	local processingKey = KEYS[1]
	local mainQueue = KEYS[2]
	local msgKey = KEYS[3]
	local statsPrefix = ARGV[2]
	local now = ARGV[1]
	
	-- Find timeout tasks (score < now)
	local ids = redis.call('ZRANGEBYSCORE', processingKey, '-inf', now, 'LIMIT', 0, 100)
	if #ids > 0 then
		for i, id in ipairs(ids) do
			-- Move back to main queue
			redis.call('RPUSH', mainQueue, id)
			redis.call('ZREM', processingKey, id)
			
			-- Update stats: processing -> pending
			local msgData = redis.call('HGET', msgKey, id)
			if msgData then
				local internalName = string.match(msgData, '"InternalName":"([^"]+)"')
				if internalName then
					local statsKey = statsPrefix .. internalName
					redis.call('HINCRBY', statsKey, 'processing', -1)
					redis.call('HINCRBY', statsKey, 'pending', 1)
				end
			end
		end
	end
	return #ids
`)

func (b *RedisBroker) Push(ctx context.Context, msg *Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}

	// Get timeout from message or use default
	timeout := msg.ProcessTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	queueKey := b.prefix + "main_queue"
	msgKey := b.prefix + "messages"
	timeoutKey := b.prefix + "timeouts"
	statsKey := b.prefix + "stats:" + msg.InternalName

	// ARGV[3] is the timeout duration in seconds
	err = pushScript.Run(ctx, b.client, []string{msgKey, queueKey, statsKey, timeoutKey}, msg.Id, data, int64(timeout.Seconds())).Err()
	if err == nil {
		b.client.Publish(ctx, b.prefix+"notify", "push")
	}
	return err
}

func (b *RedisBroker) PushDelayed(ctx context.Context, msg *Message, delay time.Duration) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}

	// Get timeout from message or use default
	timeout := msg.ProcessTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	delayedKey := b.prefix + "delayed_queue"
	msgKey := b.prefix + "messages"
	timeoutKey := b.prefix + "timeouts"
	statsKey := b.prefix + "stats:" + msg.InternalName
	score := time.Now().Add(delay).UnixMilli()

	// ARGV[4] is the timeout duration in seconds
	err = pushDelayedScript.Run(ctx, b.client, []string{msgKey, delayedKey, statsKey, timeoutKey}, msg.Id, data, score, int64(timeout.Seconds())).Err()
	if err == nil {
		b.client.Publish(ctx, b.prefix+"notify", score)
	}
	return err
}

func (b *RedisBroker) Watch(ctx context.Context) (<-chan *Task, error) {
	b.moverOnce.Do(func() {
		go b.listenMover()
		go b.startPoller()
		go b.startTimeoutScanner()
		go b.startPubSubListener()
	})
	return b.watchCh, nil
}

func (b *RedisBroker) Finish(ctx context.Context, msg *Message, processErr error, duration time.Duration) error {
	processingKey := b.prefix + "processing_queue"
	msgKey := b.prefix + "messages"
	timeoutKey := b.prefix + "timeouts"
	statsKey := b.prefix + "stats:" + msg.InternalName

	pipe := b.client.Pipeline()
	pipe.ZRem(ctx, processingKey, msg.Id)
	pipe.HIncrBy(ctx, statsKey, "processing", -1)

	// Only delete message data if processing was successful
	if processErr == nil {
		pipe.HDel(ctx, msgKey, msg.Id)
		pipe.HDel(ctx, timeoutKey, msg.Id)
	}

	_, err := pipe.Exec(ctx)
	return err
}

func (b *RedisBroker) StoreResult(ctx context.Context, result *JobResult, limit int) error {
	if limit <= 0 {
		return nil
	}

	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal result failed: %w", err)
	}

	key := b.prefix + "history:" + result.Topic
	pipe := b.client.Pipeline()
	pipe.LPush(ctx, key, data)
	pipe.LTrim(ctx, key, 0, int64(limit-1))
	_, err = pipe.Exec(ctx)
	return err
}

func (b *RedisBroker) FetchResults(ctx context.Context, topic string, limit int) ([]*JobResult, error) {
	key := b.prefix + "history:" + topic

	var vals []string
	var err error
	if limit > 0 {
		vals, err = b.client.LRange(ctx, key, 0, int64(limit-1)).Result()
	} else {
		vals, err = b.client.LRange(ctx, key, 0, -1).Result()
	}
	if err != nil {
		return nil, err
	}

	results := make([]*JobResult, 0, len(vals))
	for _, v := range vals {
		var r JobResult
		if err := json.Unmarshal([]byte(v), &r); err != nil {
			continue
		}
		results = append(results, &r)
	}
	return results, nil
}

func (b *RedisBroker) listenMover() {
	queueKey := b.prefix + "main_queue"
	processingKey := b.prefix + "processing_queue"
	msgKey := b.prefix + "messages"
	timeoutKey := b.prefix + "timeouts"

	for {
		select {
		case <-b.ctx.Done():
			return
		default:
			// Peek if there's a message to get its timeout
			res, err := b.client.LIndex(b.ctx, queueKey, 0).Result()
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

			// Get timeout duration from Redis Hash or use default
			timeoutSec, err := b.client.HGet(b.ctx, timeoutKey, res).Int64()
			if err != nil {
				timeoutSec = 30 // Fallback
			}
			visibilityTimeout := time.Now().Add(time.Duration(timeoutSec) * time.Second).UnixMilli()

			// Atomically pop and move to processing queue
			msgId, err := popAndMoveScript.Run(b.ctx, b.client, []string{queueKey, processingKey}, visibilityTimeout).Result()
			if err != nil || msgId == nil {
				continue
			}
			id := msgId.(string)

			// Fetch message data and dispatch
			data, err := b.client.HGet(b.ctx, msgKey, id).Result()
			if err == nil {
				var msg Message
				if err := json.Unmarshal([]byte(data), &msg); err == nil {
					// Update counters: pending -> processing
					statsKey := b.prefix + "stats:" + msg.InternalName
					pipe := b.client.Pipeline()
					pipe.HIncrBy(b.ctx, statsKey, "pending", -1)
					pipe.HIncrBy(b.ctx, statsKey, "processing", 1)
					pipe.Exec(b.ctx)

					select {
					case b.watchCh <- &Task{InternalName: msg.InternalName, Message: &msg}:
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
	msgKey := b.prefix + "messages"
	statsPrefix := b.prefix + "stats:"

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			now := time.Now().UnixMilli()
			moveDelayedScript.Run(b.ctx, b.client, []string{delayedKey, queueKey, msgKey}, now, statsPrefix)
		}
	}
}

func (b *RedisBroker) startTimeoutScanner() {
	processingKey := b.prefix + "processing_queue"
	queueKey := b.prefix + "main_queue"
	msgKey := b.prefix + "messages"
	statsPrefix := b.prefix + "stats:"

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			now := time.Now().UnixMilli()
			// Redeliver timeout tasks (visibility timeout expired)
			count, _ := redeliverTimeoutScript.Run(b.ctx, b.client, []string{processingKey, queueKey, msgKey}, now, statsPrefix).Int()

			// Notify mover if tasks were redelivered
			if count > 0 {
				select {
				case b.anyNotifyCh <- struct{}{}:
				default:
				}
			}
		}
	}
}

// startPubSubListener subscribes to the notify channel and wakes up listenMover on new messages.
func (b *RedisBroker) startPubSubListener() {
	for {
		select {
		case <-b.ctx.Done():
			return
		default:
		}

		pubsub := b.client.Subscribe(b.ctx, b.prefix+"notify")
		ch := pubsub.Channel()

		for {
			select {
			case <-b.ctx.Done():
				pubsub.Close()
				return
			case _, ok := <-ch:
				if !ok {
					// Channel closed, reconnect
					goto reconnect
				}
				// Wake up listenMover
				select {
				case b.anyNotifyCh <- struct{}{}:
				default:
				}
			}
		}

	reconnect:
		pubsub.Close()
		time.Sleep(1 * time.Second)
	}
}

func (b *RedisBroker) Register(ctx context.Context, meta *TopicMetadata) error {
	key := b.prefix + "meta:topics"
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal metadata failed: %w", err)
	}
	return b.client.HSet(ctx, key, meta.Name, data).Err()
}

func (b *RedisBroker) Unregister(ctx context.Context, topic string) error {
	key := b.prefix + "meta:topics"
	return b.client.HDel(ctx, key, topic).Err()
}

func (b *RedisBroker) UnregisterAndCleanup(ctx context.Context, topic string) error {
	// Look up internalName from metadata before deleting
	meta, err := b.GetTopicMetadata(ctx, topic)
	if err != nil {
		return fmt.Errorf("get metadata for cleanup: %w", err)
	}

	// Remove metadata first
	if err := b.Unregister(ctx, topic); err != nil {
		return err
	}

	// Purge all queued messages (main_queue + delayed_queue)
	b.Purge(ctx, meta.InternalName)

	// Also clean processing_queue
	processingKey := b.prefix + "processing_queue"
	msgKey := b.prefix + "messages"
	timeoutKey := b.prefix + "timeouts"

	ids, _ := b.client.ZRange(ctx, processingKey, 0, -1).Result()
	for _, id := range ids {
		data, _ := b.client.HGet(ctx, msgKey, id).Result()
		if data != "" && strings.Contains(data, fmt.Sprintf("\"InternalName\":\"%s\"", meta.InternalName)) {
			b.client.ZRem(ctx, processingKey, id)
			b.client.HDel(ctx, msgKey, id)
			b.client.HDel(ctx, timeoutKey, id)
		}
	}

	// Delete stats and history
	statsKey := b.prefix + "stats:" + meta.InternalName
	histoKey := b.prefix + "history:" + meta.InternalName
	b.client.Del(ctx, statsKey, histoKey)

	return nil
}

func (b *RedisBroker) GetTopicMetadata(ctx context.Context, name string) (*TopicMetadata, error) {
	key := b.prefix + "meta:topics"
	data, err := b.client.HGet(ctx, key, name).Result()
	if err != nil {
		return nil, err
	}

	var meta TopicMetadata
	if err := json.Unmarshal([]byte(data), &meta); err != nil {
		return nil, fmt.Errorf("unmarshal metadata failed: %w", err)
	}
	return &meta, nil
}

func (b *RedisBroker) QueueStats(ctx context.Context) (QueueStats, error) {
	pipe := b.client.Pipeline()
	pendingCmd := pipe.LLen(ctx, b.prefix+"main_queue")
	delayedCmd := pipe.ZCard(ctx, b.prefix+"delayed_queue")
	processingCmd := pipe.ZCard(ctx, b.prefix+"processing_queue")

	if _, err := pipe.Exec(ctx); err != nil {
		return QueueStats{}, err
	}

	return QueueStats{
		Pending:    pendingCmd.Val(),
		Delayed:    delayedCmd.Val(),
		Processing: processingCmd.Val(),
	}, nil
}

func (b *RedisBroker) TopicQueueStats(ctx context.Context, internalName string) (QueueStats, error) {
	statsKey := b.prefix + "stats:" + internalName
	result, err := b.client.HMGet(ctx, statsKey, "pending", "delayed", "processing").Result()
	if err != nil && err != redis.Nil {
		return QueueStats{}, err
	}

	parseInt := func(val interface{}) int64 {
		if val == nil {
			return 0
		}
		if str, ok := val.(string); ok {
			var num int64
			fmt.Sscanf(str, "%d", &num)
			return num
		}
		return 0
	}

	var pending, delayed, processing int64
	if len(result) > 0 {
		pending = parseInt(result[0])
	}
	if len(result) > 1 {
		delayed = parseInt(result[1])
	}
	if len(result) > 2 {
		processing = parseInt(result[2])
	}

	return QueueStats{
		Pending:    pending,
		Delayed:    delayed,
		Processing: processing,
	}, nil
}

func (b *RedisBroker) Purge(ctx context.Context, internalName string) error {
	queueKey := b.prefix + "main_queue"
	delayedKey := b.prefix + "delayed_queue"
	msgKey := b.prefix + "messages"
	timeoutKey := b.prefix + "timeouts"
	statsKey := b.prefix + "stats:" + internalName

	// 1. Get all message IDs in the queues to find which ones belong to this topic
	// This is slightly expensive for Redis but accurate

	// Helper to find and remove IDs
	purgeFromList := func(key string) {
		ids, _ := b.client.LRange(ctx, key, 0, -1).Result()
		for _, id := range ids {
			data, _ := b.client.HGet(ctx, msgKey, id).Result()
			if data != "" {
				if strings.Contains(data, fmt.Sprintf("\"InternalName\":\"%s\"", internalName)) {
					b.client.LRem(ctx, key, 0, id)
					b.client.HDel(ctx, msgKey, id)
					b.client.HDel(ctx, timeoutKey, id)
				}
			}
		}
	}

	purgeFromZSet := func(key string) {
		ids, _ := b.client.ZRange(ctx, key, 0, -1).Result()
		for _, id := range ids {
			data, _ := b.client.HGet(ctx, msgKey, id).Result()
			if data != "" {
				if strings.Contains(data, fmt.Sprintf("\"InternalName\":\"%s\"", internalName)) {
					b.client.ZRem(ctx, key, id)
					b.client.HDel(ctx, msgKey, id)
					b.client.HDel(ctx, timeoutKey, id)
				}
			}
		}
	}

	purgeFromList(queueKey)
	purgeFromZSet(delayedKey)

	// Reset counters
	b.client.HSet(ctx, statsKey, "pending", 0, "delayed", 0)

	return nil
}

func (b *RedisBroker) Close() error {
	b.cancel()
	return b.client.Close()
}
