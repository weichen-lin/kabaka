package broker

import (
	"container/heap"
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type delayedMessage struct {
	message    *Message
	scheduleAt time.Time
	index      int // index in the heap
}

// delayedHeap implements heap.Interface for delayed messages
type delayedHeap []*delayedMessage

func (h delayedHeap) Len() int           { return len(h) }
func (h delayedHeap) Less(i, j int) bool { return h[i].scheduleAt.Before(h[j].scheduleAt) }
func (h delayedHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *delayedHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*delayedMessage)
	item.index = n
	*h = append(*h, item)
}

func (h *delayedHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[0 : n-1]
	return item
}

type processingEntry struct {
	message   *Message
	startTime time.Time
}

// MemoryBroker is an in-memory implementation of the Broker interface.
type MemoryBroker struct {
	mu         sync.RWMutex
	metadata   map[string]*TopicMetadata   // topic name -> metadata
	messages   *list.List                  // pending messages queue (FIFO)
	delayed    *delayedHeap                // delayed messages (min-heap by scheduleAt)
	processing map[string]*processingEntry // message ID -> processing info
	history    map[string][]*JobResult     // topic name -> historical records (LRU)

	watchCh        chan *Task
	notifyCh       chan struct{} // notification channel for new messages
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	closeOnce      sync.Once
	dispatcherOnce sync.Once

	// Configurable intervals
	dispatchInterval        time.Duration
	delayedCheckInterval    time.Duration
	processingCheckInterval time.Duration

	// Max retry attempts for failed messages (0 = unlimited)
	maxRetries int
}

// Type returns the broker type identifier.
func (b *MemoryBroker) Type() string { return "memory" }

// NewMemoryBroker creates a new in-memory broker.
func NewMemoryBroker() *MemoryBroker {
	ctx, cancel := context.WithCancel(context.Background())
	h := &delayedHeap{}
	heap.Init(h)

	mb := &MemoryBroker{
		metadata:                make(map[string]*TopicMetadata),
		messages:                list.New(),
		delayed:                 h,
		processing:              make(map[string]*processingEntry),
		history:                 make(map[string][]*JobResult),
		watchCh:                 make(chan *Task, 100),
		notifyCh:                make(chan struct{}, 1),
		ctx:                     ctx,
		cancel:                  cancel,
		dispatchInterval:        10 * time.Millisecond,
		delayedCheckInterval:    100 * time.Millisecond,
		processingCheckInterval: 1 * time.Second,
		maxRetries:              3, // Default: retry 3 times before giving up
	}

	// Start background workers
	mb.wg.Add(2)
	go mb.moveDelayedMessages()
	go mb.cleanupStaleProcessing()

	return mb
}

// isClosed checks if the broker has been closed by checking the context.
func (mb *MemoryBroker) isClosed() bool {
	return mb.ctx.Err() != nil
}

// Register registers a new topic with metadata.
func (mb *MemoryBroker) Register(ctx context.Context, meta *TopicMetadata) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if mb.isClosed() {
		return fmt.Errorf("broker is closed")
	}

	if _, exists := mb.metadata[meta.Name]; exists {
		return fmt.Errorf("topic %s already registered", meta.Name)
	}

	mb.metadata[meta.Name] = meta
	return nil
}

// Unregister removes a topic registration.
func (mb *MemoryBroker) Unregister(ctx context.Context, topic string) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if mb.isClosed() {
		return fmt.Errorf("broker is closed")
	}

	if _, exists := mb.metadata[topic]; !exists {
		return fmt.Errorf("topic %s not found", topic)
	}

	delete(mb.metadata, topic)
	return nil
}

// UnregisterAndCleanup removes a topic and cleans up all its messages.
func (mb *MemoryBroker) UnregisterAndCleanup(ctx context.Context, topic string) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	meta, exists := mb.metadata[topic]
	if !exists {
		return fmt.Errorf("topic %s not found", topic)
	}

	internalName := meta.InternalName

	// Remove from metadata
	delete(mb.metadata, topic)

	// Clean up pending messages
	var next *list.Element
	for elem := mb.messages.Front(); elem != nil; elem = next {
		next = elem.Next()
		msg := elem.Value.(*Message)
		if msg.InternalName == internalName {
			mb.messages.Remove(elem)
		}
	}

	// Clean up delayed messages - rebuild heap without matching messages
	newDelayed := &delayedHeap{}
	for _, dm := range *mb.delayed {
		if dm.message.InternalName != internalName {
			*newDelayed = append(*newDelayed, dm)
		}
	}
	heap.Init(newDelayed)
	mb.delayed = newDelayed

	// Clean up processing messages
	for msgID, entry := range mb.processing {
		if entry.message.InternalName == internalName {
			delete(mb.processing, msgID)
		}
	}

	return nil
}

// GetTopicMetadata retrieves metadata for a topic.
func (mb *MemoryBroker) GetTopicMetadata(ctx context.Context, name string) (*TopicMetadata, error) {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

	meta, exists := mb.metadata[name]
	if !exists {
		return nil, fmt.Errorf("topic %s not found", name)
	}

	return meta, nil
}

// Push adds a message to the pending queue.
func (mb *MemoryBroker) Push(ctx context.Context, msg *Message) error {
	mb.mu.Lock()
	if mb.isClosed() {
		mb.mu.Unlock()
		return fmt.Errorf("broker is closed")
	}
	mb.messages.PushBack(msg)
	mb.mu.Unlock()

	// Notify dispatcher (non-blocking)
	select {
	case mb.notifyCh <- struct{}{}:
	default:
	}

	return nil
}

// PushDelayed adds a message to the delayed queue.
func (mb *MemoryBroker) PushDelayed(ctx context.Context, msg *Message, delay time.Duration) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if mb.isClosed() {
		return fmt.Errorf("broker is closed")
	}

	dm := &delayedMessage{
		message:    msg,
		scheduleAt: time.Now().Add(delay),
	}
	heap.Push(mb.delayed, dm)

	return nil
}

// Watch returns a channel that receives tasks from the queue.
// This method can be called multiple times and will return the same channel.
// The dispatcher is started only once.
func (mb *MemoryBroker) Watch(ctx context.Context) (<-chan *Task, error) {
	mb.mu.Lock()
	if mb.isClosed() {
		mb.mu.Unlock()
		return nil, fmt.Errorf("broker is closed")
	}
	mb.mu.Unlock()

	// Start dispatcher only once
	mb.dispatcherOnce.Do(func() {
		mb.wg.Add(1)
		go mb.dispatcher()
	})

	return mb.watchCh, nil
}

// Finish marks a message as processed and removes it from processing queue.
func (mb *MemoryBroker) Finish(ctx context.Context, msg *Message, processErr error, duration time.Duration) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	_, exists := mb.processing[msg.Id]
	if !exists {
		return fmt.Errorf("message %s not found in processing queue", msg.Id)
	}

	// Note: processErr and duration could be used for metrics/logging
	// Example: logger.LogProcessing(msg.Id, processErr, duration)

	delete(mb.processing, msg.Id)
	return nil
}

// StoreResult saves a job result to the history LRU.
func (mb *MemoryBroker) StoreResult(ctx context.Context, result *JobResult, limit int) error {
	if limit <= 0 {
		return nil
	}

	mb.mu.Lock()
	defer mb.mu.Unlock()

	h := mb.history[result.Topic]
	// Append new result (O(1) operation)
	h = append(h, result)

	// Trim from front if exceeds limit
	if len(h) > limit {
		h = h[len(h)-limit:]
	}
	mb.history[result.Topic] = h

	return nil
}

// FetchResults retrieves the historical records for a topic.
func (mb *MemoryBroker) FetchResults(ctx context.Context, topic string, limit int) ([]*JobResult, error) {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

	h, exists := mb.history[topic]
	if !exists {
		return []*JobResult{}, nil
	}

	// Apply limit if specified
	count := len(h)
	if limit > 0 && count > limit {
		count = limit
	}

	// Perform Deep Copy and Reverse order (newest first)
	res := make([]*JobResult, count)
	for i := 0; i < count; i++ {
		orig := h[len(h)-1-i]
		// Deep copy the JobResult struct
		copyResult := *orig
		// Deep copy the Payload slice
		if orig.Payload != nil {
			copyResult.Payload = make(json.RawMessage, len(orig.Payload))
			copy(copyResult.Payload, orig.Payload)
		}
		res[i] = &copyResult
	}

	return res, nil
}

// QueueStats returns overall queue statistics.
func (mb *MemoryBroker) QueueStats(ctx context.Context) (QueueStats, error) {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

	return QueueStats{
		Pending:    int64(mb.messages.Len()),
		Delayed:    int64(mb.delayed.Len()),
		Processing: int64(len(mb.processing)),
	}, nil
}

// TopicQueueStats returns queue statistics for a specific topic.
func (mb *MemoryBroker) TopicQueueStats(ctx context.Context, internalName string) (QueueStats, error) {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

	stats := QueueStats{}

	// Count pending messages
	for elem := mb.messages.Front(); elem != nil; elem = elem.Next() {
		msg := elem.Value.(*Message)
		if msg.InternalName == internalName {
			stats.Pending++
		}
	}

	// Count delayed messages
	for _, dm := range *mb.delayed {
		if dm.message.InternalName == internalName {
			stats.Delayed++
		}
	}

	// Count processing messages
	for _, entry := range mb.processing {
		if entry.message.InternalName == internalName {
			stats.Processing++
		}
	}

	return stats, nil
}

// Purge removes all pending and delayed messages for a specific topic.
func (mb *MemoryBroker) Purge(ctx context.Context, internalName string) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if mb.isClosed() {
		return fmt.Errorf("broker is closed")
	}

	// Clean up pending messages
	var next *list.Element
	for elem := mb.messages.Front(); elem != nil; elem = next {
		next = elem.Next()
		msg := elem.Value.(*Message)
		if msg.InternalName == internalName {
			mb.messages.Remove(elem)
		}
	}

	// Clean up delayed messages - rebuild heap without matching messages
	newDelayed := &delayedHeap{}
	for _, dm := range *mb.delayed {
		if dm.message.InternalName != internalName {
			*newDelayed = append(*newDelayed, dm)
		}
	}
	heap.Init(newDelayed)
	mb.delayed = newDelayed

	return nil
}

// Close shuts down the broker.
func (mb *MemoryBroker) Close() error {
	mb.closeOnce.Do(func() {
		mb.cancel()
		mb.wg.Wait()
		close(mb.watchCh)
	})
	return nil
}

// dispatcher continuously dispatches messages from the queue.
func (mb *MemoryBroker) dispatcher() {
	defer mb.wg.Done()

	ticker := time.NewTicker(mb.dispatchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-mb.ctx.Done():
			return
		case <-mb.notifyCh:
			mb.dispatchNext()
		case <-ticker.C:
			mb.dispatchNext()
		}
	}
}

// dispatchNext sends the next available message to the watch channel.
func (mb *MemoryBroker) dispatchNext() {
	mb.mu.Lock()

	// Check if broker is closed
	if mb.isClosed() {
		mb.mu.Unlock()
		return
	}

	if mb.messages.Len() == 0 {
		mb.mu.Unlock()
		return
	}

	// Pop first message
	elem := mb.messages.Front()
	msg := mb.messages.Remove(elem).(*Message)

	// Mark as processing with timestamp
	mb.processing[msg.Id] = &processingEntry{
		message:   msg,
		startTime: time.Now(),
	}

	// Create task before releasing lock
	task := &Task{
		InternalName: msg.InternalName,
		Message:      msg,
	}
	mb.mu.Unlock()

	// Try to send to watch channel (safe because we hold the channel open until Close is done)
	select {
	case mb.watchCh <- task:
		// Successfully dispatched
	case <-mb.ctx.Done():
		// Context cancelled, requeue only if not fully closed yet
		mb.mu.Lock()
		if !mb.isClosed() {
			mb.messages.PushFront(msg)
			delete(mb.processing, msg.Id)
		}
		mb.mu.Unlock()
	}
}

// moveDelayedMessages periodically checks delayed messages and moves ready ones to pending queue.
func (mb *MemoryBroker) moveDelayedMessages() {
	defer mb.wg.Done()

	ticker := time.NewTicker(mb.delayedCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-mb.ctx.Done():
			return
		case <-ticker.C:
			mb.mu.Lock()
			now := time.Now()
			movedCount := 0

			// Pop all ready messages from heap
			for mb.delayed.Len() > 0 {
				// Peek at the earliest message
				dm := (*mb.delayed)[0]
				if now.Before(dm.scheduleAt) {
					// Not ready yet, and since it's a min-heap, nothing else is ready
					break
				}

				// Pop and move to pending queue
				heap.Pop(mb.delayed)
				mb.messages.PushBack(dm.message)
				movedCount++
			}

			mb.mu.Unlock()

			// Notify dispatcher only if we moved any messages
			if movedCount > 0 {
				select {
				case mb.notifyCh <- struct{}{}:
				default:
				}
			}
		}
	}
}

// cleanupStaleProcessing periodically checks for stale processing messages and requeues them.
func (mb *MemoryBroker) cleanupStaleProcessing() {
	defer mb.wg.Done()

	ticker := time.NewTicker(mb.processingCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-mb.ctx.Done():
			return
		case <-ticker.C:
			mb.mu.Lock()
			now := time.Now()
			staleMessages := make([]*Message, 0)

			// Find stale messages
			for msgID, entry := range mb.processing {
				timeout := entry.message.ProcessTimeout
				if timeout == 0 {
					timeout = 30 * time.Second // default timeout
				}

				if now.Sub(entry.startTime) > timeout {
					msg := entry.message
					msg.Retry++ // Increment retry count

					// Check max retries (0 = unlimited)
					if mb.maxRetries == 0 || msg.Retry <= mb.maxRetries {
						staleMessages = append(staleMessages, msg)
					}
					// else: message exceeded max retries, drop it

					delete(mb.processing, msgID)
				}
			}

			// Requeue stale messages that haven't exceeded max retries
			for _, msg := range staleMessages {
				mb.messages.PushBack(msg)
			}

			mb.mu.Unlock()

			// Notify dispatcher if we requeued any messages
			if len(staleMessages) > 0 {
				select {
				case mb.notifyCh <- struct{}{}:
				default:
				}
			}
		}
	}
}
