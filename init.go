package kabaka

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"sync"
	"time"
)

type metaCacheEntry struct {
	internalName string
	expiresAt    time.Time
}

type Kabaka struct {
	mu     sync.RWMutex
	topics map[string]*Topic
	broker Broker

	// Metadata cache for lightweight publishing
	metaCache map[string]*metaCacheEntry

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type KabakaOption func(*Kabaka)

func NewKabaka(options ...KabakaOption) *Kabaka {
	ctx, cancel := context.WithCancel(context.Background())
	k := &Kabaka{
		topics:    make(map[string]*Topic),
		metaCache: make(map[string]*metaCacheEntry),
		ctx:       ctx,
		cancel:    cancel,
	}

	for _, opt := range options {
		opt(k)
	}

	return k
}

func (k *Kabaka) Start() {
	k.wg.Add(1)
	go k.dispatch()
}

func (k *Kabaka) dispatch() {
	defer k.wg.Done()

	k.mu.RLock()
	var internalNames []string
	for name := range k.topics {
		internalNames = append(internalNames, name)
	}
	k.mu.RUnlock()

	if len(internalNames) == 0 {
		return // Or handle dynamic topic registration later
	}

	taskCh, err := k.broker.Watch(k.ctx, internalNames...)
	if err != nil {
		return
	}

	for {
		select {
		case <-k.ctx.Done():
			return
		case task, ok := <-taskCh:
			if !ok {
				return
			}
			k.mu.RLock()
			topic, ok := k.topics[task.Topic]
			k.mu.RUnlock()

			if ok {
				topic.receive(task.Message)
			}
		}
	}
}

func WithBroker(broker Broker) KabakaOption {
	return func(k *Kabaka) {
		k.broker = broker
	}
}

func (k *Kabaka) generateInternalName(name string) string {
	h := sha1.New()
	h.Write([]byte(name))
	return hex.EncodeToString(h.Sum(nil))
}

func (k *Kabaka) getInternalName(name string) (string, error) {
	k.mu.RLock()
	// 1. Check local cache first
	if entry, ok := k.metaCache[name]; ok {
		// Passive expiration check
		if time.Now().Before(entry.expiresAt) {
			k.mu.RUnlock()
			return entry.internalName, nil
		}
	}
	k.mu.RUnlock()

	// 2. Not in cache or expired, ask the broker
	ctx, cancel := context.WithTimeout(k.ctx, 2*time.Second)
	defer cancel()
	internalName, err := k.broker.GetMetadata(ctx, name)
	if err == nil {
		// Update cache with 10-minute TTL
		k.mu.Lock()
		k.metaCache[name] = &metaCacheEntry{
			internalName: internalName,
			expiresAt:    time.Now().Add(10 * time.Minute),
		}
		k.mu.Unlock()
		return internalName, nil
	}

	// 3. Last resort: If the broker doesn't have it, but we have it locally registered
	// This handles cases where the topic was just created but not yet in metadata
	k.mu.RLock()
	defer k.mu.RUnlock()
	
	// Check if we have a topic with this as an original name
	for _, topic := range k.topics {
		if topic.Name == name {
			return topic.InternalName, nil
		}
	}

	return "", ErrTopicNotFound
}

func (k *Kabaka) CreateTopic(name string, handler HandleFunc, options ...Option) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	internalName := k.generateInternalName(name)

	// Check if already registered locally
	if _, ok := k.topics[internalName]; ok {
		return ErrTopicAlreadyCreated
	}

	// Explicitly register the topic in the broker
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	// 1. Set global metadata
	if err := k.broker.SetMetadata(ctx, name, internalName); err != nil {
		return err
	}

	// 2. Register for polling
	if err := k.broker.Register(ctx, internalName); err != nil {
		return err
	}

	topic := newTopic(name, internalName, k.broker, handler, options...)
	k.topics[internalName] = topic

	return nil
}

func (k *Kabaka) Publish(name string, message []byte) error {
	internalName, err := k.getInternalName(name)
	if err != nil {
		return err
	}

	// Send message through the broker
	msg := &Message{
		Id:        NewUUID(),
		Value:     message,
		Retry:     3, // Default retry
		CreatedAt: time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	return k.broker.Push(ctx, internalName, msg)
}

func (k *Kabaka) PublishDelayed(name string, message []byte, delay time.Duration) error {
	internalName, err := k.getInternalName(name)
	if err != nil {
		return err
	}

	msg := &Message{
		Id:        NewUUID(),
		Value:     message,
		Retry:     3, // Default retry
		CreatedAt: time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	return k.broker.PushDelayed(ctx, internalName, msg, delay)
}

func (k *Kabaka) CloseTopic(name string) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	internalName := k.generateInternalName(name)
	topic, ok := k.topics[internalName]
	if !ok {
		return ErrTopicNotFound
	}

	topic.stop()

	// Unregister from broker to clean up resources
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = k.broker.Unregister(ctx, topic.InternalName)

	delete(k.topics, internalName)
	return nil
}

func (k *Kabaka) Close() error {
	k.cancel()
	k.wg.Wait()

	k.mu.Lock()
	defer k.mu.Unlock()

	for _, topic := range k.topics {
		topic.stop()
	}

	if k.broker != nil {
		return k.broker.Close()
	}

	return nil
}

func (k *Kabaka) GetTopicSchema(name string) (string, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	internalName := k.generateInternalName(name)
	topic, ok := k.topics[internalName]
	if !ok {
		return "", ErrTopicNotFound
	}

	return topic.schema, nil
}
