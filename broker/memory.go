package broker

import (
	"container/heap"
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/weichen-lin/kabaka"
)

type delayedMessage struct {
	message    *kabaka.Message
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
	message   *kabaka.Message
	startTime time.Time
}

// MemoryBroker is an in-memory implementation of the Broker interface.
type MemoryBroker struct {
	mu         sync.RWMutex
	metadata   map[string]*kabaka.TopicMetadata // topic name -> metadata
	messages   *list.List                       // pending messages queue (FIFO)
	delayed    *delayedHeap                     // delayed messages (min-heap by scheduleAt)
	processing map[string]*processingEntry      // message ID -> processing info

	watchCh        chan *kabaka.Task
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

// NewMemoryBroker creates a new in-memory broker.
func NewMemoryBroker() *MemoryBroker {
	ctx, cancel := context.WithCancel(context.Background())
	h := &delayedHeap{}
	heap.Init(h)

	mb := &MemoryBroker{
		metadata:                make(map[string]*kabaka.TopicMetadata),
		messages:                list.New(),
		delayed:                 h,
		processing:              make(map[string]*processingEntry),
		watchCh:                 make(chan *kabaka.Task, 100),
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
func (mb *MemoryBroker) Register(ctx context.Context, meta *kabaka.TopicMetadata) error {
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
		msg := elem.Value.(*kabaka.Message)
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
func (mb *MemoryBroker) GetTopicMetadata(ctx context.Context, name string) (*kabaka.TopicMetadata, error) {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

	meta, exists := mb.metadata[name]
	if !exists {
		return nil, fmt.Errorf("topic %s not found", name)
	}

	return meta, nil
}

// Push adds a message to the pending queue.
func (mb *MemoryBroker) Push(ctx context.Context, msg *kabaka.Message) error {
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
func (mb *MemoryBroker) PushDelayed(ctx context.Context, msg *kabaka.Message, delay time.Duration) error {
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
func (mb *MemoryBroker) Watch(ctx context.Context) (<-chan *kabaka.Task, error) {
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
func (mb *MemoryBroker) Finish(ctx context.Context, msg *kabaka.Message, processErr error, duration time.Duration) error {
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

// QueueStats returns overall queue statistics.
func (mb *MemoryBroker) QueueStats(ctx context.Context) (kabaka.QueueStats, error) {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

	return kabaka.QueueStats{
		Pending:    int64(mb.messages.Len()),
		Delayed:    int64(mb.delayed.Len()),
		Processing: int64(len(mb.processing)),
	}, nil
}

// TopicQueueStats returns queue statistics for a specific topic.
func (mb *MemoryBroker) TopicQueueStats(ctx context.Context, internalName string) (kabaka.QueueStats, error) {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

	stats := kabaka.QueueStats{}

	// Count pending messages
	for elem := mb.messages.Front(); elem != nil; elem = elem.Next() {
		msg := elem.Value.(*kabaka.Message)
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
		msg := elem.Value.(*kabaka.Message)
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
	msg := mb.messages.Remove(elem).(*kabaka.Message)

	// Mark as processing with timestamp
	mb.processing[msg.Id] = &processingEntry{
		message:   msg,
		startTime: time.Now(),
	}

	// Create task before releasing lock
	task := &kabaka.Task{
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
			staleMessages := make([]*kabaka.Message, 0)

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
