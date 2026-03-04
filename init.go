package kabaka

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"sync"
	"time"
)

type HandleFunc func(context.Context, *Message) error

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

	// Shared worker pool architecture
	workerPool chan chan *Message
	workers    []*Worker
	maxWorkers int

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type KabakaOption func(*Kabaka)

func NewKabaka(options ...KabakaOption) *Kabaka {
	ctx, cancel := context.WithCancel(context.Background())
	k := &Kabaka{
		topics:     make(map[string]*Topic),
		metaCache:  make(map[string]*metaCacheEntry),
		maxWorkers: 10, // Default worker count
		ctx:        ctx,
		cancel:     cancel,
	}

	for _, opt := range options {
		opt(k)
	}

	// Initialize shared worker pool
	k.workerPool = make(chan chan *Message, k.maxWorkers)
	for i := 0; i < k.maxWorkers; i++ {
		worker := NewWorker(k)
		k.workers = append(k.workers, worker)
		worker.start()
	}

	return k
}

func (k *Kabaka) Start() {
	k.wg.Add(1)
	go k.dispatch()
}

func (k *Kabaka) dispatch() {
	defer k.wg.Done()

	// In shared queue architecture, we just watch the single shared channel
	taskCh, err := k.broker.Watch(k.ctx)
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

			// Dispatch to an available worker from the SHARED pool
			// Each worker will handle its own topic lookup based on task.Topic
			select {
			case workerCh := <-k.workerPool:
				workerCh <- task.Message
			case <-k.ctx.Done():
				return
			}
		}
	}
}

func WithBroker(broker Broker) KabakaOption {
	return func(k *Kabaka) {
		k.broker = broker
	}
}

func WithMaxWorkers(n int) KabakaOption {
	return func(k *Kabaka) {
		k.maxWorkers = n
	}
}

func (k *Kabaka) generateInternalName(name string) string {
	h := sha1.New()
	h.Write([]byte(name))
	return hex.EncodeToString(h.Sum(nil))
}

func (k *Kabaka) getInternalName(name string) (string, error) {
	k.mu.RLock()
	if entry, ok := k.metaCache[name]; ok {
		if time.Now().Before(entry.expiresAt) {
			k.mu.RUnlock()
			return entry.internalName, nil
		}
	}
	k.mu.RUnlock()

	ctx, cancel := context.WithTimeout(k.ctx, 2*time.Second)
	defer cancel()
	internalName, err := k.broker.GetMetadata(ctx, name)
	if err == nil {
		k.mu.Lock()
		k.metaCache[name] = &metaCacheEntry{
			internalName: internalName,
			expiresAt:    time.Now().Add(10 * time.Minute),
		}
		k.mu.Unlock()
		return internalName, nil
	}

	return "", ErrTopicNotFound
}

func (k *Kabaka) CreateTopic(name string, handler HandleFunc, options ...Option) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	internalName := k.generateInternalName(name)
	if _, ok := k.topics[internalName]; ok {
		return ErrTopicAlreadyCreated
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	k.broker.SetMetadata(ctx, name, internalName)
	k.broker.Register(ctx, internalName)

	topic := newTopic(name, internalName, k.broker, handler, options...)
	k.topics[internalName] = topic

	return nil
}

func (k *Kabaka) Publish(name string, message []byte) error {
	internalName, err := k.getInternalName(name)
	if err != nil {
		// Fallback to generating it if not found (lightweight producer)
		internalName = k.generateInternalName(name)
	}

	msg := &Message{
		Id:           NewUUID(),
		InternalName: internalName,
		Value:        message,
		Retry:        3,
		CreatedAt:    time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	return k.broker.Push(ctx, msg)
}

func (k *Kabaka) PublishDelayed(name string, message []byte, delay time.Duration) error {
	internalName, err := k.getInternalName(name)
	if err != nil {
		internalName = k.generateInternalName(name)
	}

	msg := &Message{
		Id:           NewUUID(),
		InternalName: internalName,
		Value:        message,
		Retry:        3,
		CreatedAt:    time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	return k.broker.PushDelayed(ctx, msg, delay)
}

func (k *Kabaka) Close() error {
	k.cancel()
	k.wg.Wait()

	for _, worker := range k.workers {
		worker.stop()
	}

	if k.broker != nil {
		return k.broker.Close()
	}

	return nil
}
