package kabaka

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v5"
	"github.com/weichen-lin/kabaka/broker"
)

type HandleFunc func(context.Context, *broker.Message) error

type metaCacheEntry struct {
	metadata  *broker.TopicMetadata
	expiresAt time.Time
}

// Kabaka is the main message queue manager.
type Kabaka struct {
	mu     sync.RWMutex
	topics map[string]*Topic
	broker broker.Broker
	logger Logger

	// Metadata cache for lightweight publishing
	metaCache    map[string]*metaCacheEntry
	metaCacheTTL time.Duration // TTL for metadata cache entries (default: 5m)

	// Schema cache for fast validation
	schemaCache sync.Map // map[string]*jsonschema.Schema

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
		schemaCache:   sync.Map{},
		logger:        &DefaultLogger{},
		maxWorkers:    10,                       // Default worker count
		broker:        broker.NewMemoryBroker(), // Default in-memory broker
		brokerTimeout: 2 * time.Second,          // Default broker timeout
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
func (k *Kabaka) getMetadataFromCache(topicName string) (*broker.TopicMetadata, bool) {
	k.mu.RLock()
	entry, exists := k.metaCache[topicName]
	k.mu.RUnlock()

	if !exists || time.Now().After(entry.expiresAt) {
		return nil, false
	}

	return entry.metadata, true
}

// setMetadataCache stores metadata in cache with a TTL.
func (k *Kabaka) setMetadataCache(topicName string, meta *broker.TopicMetadata, ttl time.Duration) {
	k.mu.Lock()
	k.metaCache[topicName] = &metaCacheEntry{
		metadata:  meta,
		expiresAt: time.Now().Add(ttl),
	}
	k.mu.Unlock()
}

// getOrFetchMetadata tries to get metadata from cache, otherwise fetches from broker.
func (k *Kabaka) getOrFetchMetadata(ctx context.Context, topicName string) (*broker.TopicMetadata, error) {
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

// SetTopicPaused sets the paused state of a topic.
func (k *Kabaka) SetTopicPaused(name string, paused bool) error {
	k.mu.RLock()
	defer k.mu.RUnlock()

	internalName := k.generateInternalName(name)
	topic, ok := k.topics[internalName]
	if !ok {
		return ErrTopicNotFound
	}

	topic.Paused.Store(paused)
	return nil
}

// PurgeTopic removes all pending/delayed tasks for a topic from the broker.
func (k *Kabaka) PurgeTopic(name string, internalName string) error {
	ctx, cancel := context.WithTimeout(k.ctx, k.brokerTimeout)
	defer cancel()

	return k.broker.Purge(ctx, internalName)
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

// getOrCompileSchema retrieves a compiled schema from cache or compiles it.
func (k *Kabaka) getOrCompileSchema(topicName string, schemaStr string) (*jsonschema.Schema, error) {
	if schemaStr == "" {
		return nil, nil
	}

	// Try cache first
	if val, ok := k.schemaCache.Load(topicName); ok {
		return val.(*jsonschema.Schema), nil
	}

	// Compile and store in cache
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource(topicName+".json", strings.NewReader(schemaStr)); err != nil {
		return nil, err
	}
	sch, err := compiler.Compile(topicName + ".json")
	if err != nil {
		return nil, err
	}

	k.schemaCache.Store(topicName, sch)
	return sch, nil
}

// validateMessage checks if the message value matches the topic's schema.
func (k *Kabaka) validateMessage(topicName string, schemaStr string, value []byte) error {
	sch, err := k.getOrCompileSchema(topicName, schemaStr)
	if err != nil {
		return err
	}

	if sch == nil {
		return nil
	}

	var v interface{}
	if err := json.Unmarshal(value, &v); err != nil {
		return err
	}

	return sch.Validate(v)
}
