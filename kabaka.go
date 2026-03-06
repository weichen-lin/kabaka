package kabaka

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type HandleFunc func(context.Context, *Message) error

type metaCacheEntry struct {
	metadata  *TopicMetadata
	expiresAt time.Time
}

// Kabaka is the main message queue manager.
type Kabaka struct {
	mu     sync.RWMutex
	topics map[string]*Topic
	broker Broker
	logger Logger

	// Metadata cache for lightweight publishing
	metaCache    map[string]*metaCacheEntry
	metaCacheTTL time.Duration // TTL for metadata cache entries (default: 5m)

	// Semaphore to limit concurrent job executions
	sem        chan struct{}
	maxWorkers int
	activejobs atomic.Int64 // number of currently running jobs

	// Timeouts
	brokerTimeout time.Duration // timeout for broker operations (default: 2s)

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type KabakaOption func(*Kabaka)

// NewKabaka creates a new Kabaka instance with the given options.
func NewKabaka(options ...KabakaOption) *Kabaka {
	ctx, cancel := context.WithCancel(context.Background())
	k := &Kabaka{
		topics:        make(map[string]*Topic),
		metaCache:     make(map[string]*metaCacheEntry),
		metaCacheTTL:  5 * time.Minute, // Default metadata cache TTL
		logger:        &DefaultLogger{},
		maxWorkers:    10,              // Default worker count
		brokerTimeout: 2 * time.Second, // Default broker timeout
		ctx:           ctx,
		cancel:        cancel,
	}

	for _, opt := range options {
		opt(k)
	}

	// Initialize semaphore
	k.sem = make(chan struct{}, k.maxWorkers)

	return k
}

// Close stops the Kabaka instance and waits for all workers to finish.
func (k *Kabaka) Close() error {
	k.cancel()
	k.wg.Wait()

	if k.broker != nil {
		return k.broker.Close()
	}

	return nil
}

// getMetadataFromCache retrieves metadata from cache if available and not expired.
func (k *Kabaka) getMetadataFromCache(topicName string) (*TopicMetadata, bool) {
	k.mu.RLock()
	entry, exists := k.metaCache[topicName]
	k.mu.RUnlock()

	if !exists || time.Now().After(entry.expiresAt) {
		return nil, false
	}

	return entry.metadata, true
}

// setMetadataCache stores metadata in cache with a TTL.
func (k *Kabaka) setMetadataCache(topicName string, meta *TopicMetadata, ttl time.Duration) {
	k.mu.Lock()
	k.metaCache[topicName] = &metaCacheEntry{
		metadata:  meta,
		expiresAt: time.Now().Add(ttl),
	}
	k.mu.Unlock()
}

// getOrFetchMetadata tries to get metadata from cache, otherwise fetches from broker.
func (k *Kabaka) getOrFetchMetadata(ctx context.Context, topicName string) (*TopicMetadata, error) {
	// Try cache first
	if meta, ok := k.getMetadataFromCache(topicName); ok {
		return meta, nil
	}

	// Cache miss or expired, fetch from broker
	meta, err := k.broker.GetTopicMetadata(ctx, topicName)
	if err != nil {
		return nil, err
	}

	// Store in cache with configured TTL
	k.mu.RLock()
	ttl := k.metaCacheTTL
	k.mu.RUnlock()
	k.setMetadataCache(topicName, meta, ttl)

	return meta, nil
}

// SetMetaCacheTTL sets the TTL for metadata cache entries at runtime.
func (k *Kabaka) SetMetaCacheTTL(ttl time.Duration) {
	k.mu.Lock()
	k.metaCacheTTL = ttl
	k.mu.Unlock()
}

// GetMetaCacheTTL returns the current TTL for metadata cache entries.
func (k *Kabaka) GetMetaCacheTTL() time.Duration {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.metaCacheTTL
}
