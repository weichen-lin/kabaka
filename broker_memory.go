package kabaka

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type MemoryBroker struct {
	mu     sync.RWMutex
	queues map[string]chan *Message
	stats  map[string]*topicStats
	buffer int
}

type topicStats struct {
	mu        sync.Mutex
	success   int64
	failed    int64
	retried   int64
	durations []float64 // Last 1000 durations in ms
}

const maxSamples = 1000

func NewMemoryBroker(buffer int) *MemoryBroker {
	return &MemoryBroker{
		queues: make(map[string]chan *Message),
		stats:  make(map[string]*topicStats),
		buffer: buffer,
	}
}

func (b *MemoryBroker) getStats(topic string) *topicStats {
	b.mu.RLock()
	s, ok := b.stats[topic]
	b.mu.RUnlock()

	if ok {
		return s
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if s, ok = b.stats[topic]; ok {
		return s
	}

	s = &topicStats{
		durations: make([]float64, 0, maxSamples),
	}
	b.stats[topic] = s
	return s
}

func (b *MemoryBroker) IncSuccess(ctx context.Context, topic string) error {
	s := b.getStats(topic)
	atomic.AddInt64(&s.success, 1)
	return nil
}

func (b *MemoryBroker) IncFailed(ctx context.Context, topic string) error {
	s := b.getStats(topic)
	atomic.AddInt64(&s.failed, 1)
	return nil
}

func (b *MemoryBroker) IncRetried(ctx context.Context, topic string) error {
	s := b.getStats(topic)
	atomic.AddInt64(&s.retried, 1)
	return nil
}

func (b *MemoryBroker) RecordDuration(ctx context.Context, topic string, d time.Duration) error {
	s := b.getStats(topic)
	s.mu.Lock()
	defer s.mu.Unlock()

	ms := float64(d.Milliseconds())
	if len(s.durations) >= maxSamples {
		s.durations = s.durations[1:]
	}
	s.durations = append(s.durations, ms)
	return nil
}

func (b *MemoryBroker) GetStats(ctx context.Context, topic string) (success, failed, retried int64, p95, p99 float64, err error) {
	s := b.getStats(topic)
	
	s.mu.Lock()
	samples := make([]float64, len(s.durations))
	copy(samples, s.durations)
	s.mu.Unlock()

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

	return atomic.LoadInt64(&s.success), atomic.LoadInt64(&s.failed), atomic.LoadInt64(&s.retried), calcPercentile(0.95), calcPercentile(0.99), nil
}

func (b *MemoryBroker) ResetStats(ctx context.Context, topic string) error {
	s := b.getStats(topic)
	atomic.StoreInt64(&s.success, 0)
	atomic.StoreInt64(&s.failed, 0)
	atomic.StoreInt64(&s.retried, 0)
	
	s.mu.Lock()
	s.durations = make([]float64, 0, maxSamples)
	s.mu.Unlock()
	
	return nil
}

func (b *MemoryBroker) getQueue(topic string) chan *Message {
	b.mu.RLock()
	ch, ok := b.queues[topic]
	b.mu.RUnlock()

	if ok {
		return ch
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Double check after lock
	if ch, ok = b.queues[topic]; ok {
		return ch
	}

	ch = make(chan *Message, b.buffer)
	b.queues[topic] = ch
	return ch
}

func (b *MemoryBroker) Push(ctx context.Context, topic string, msg *Message) error {
	ch := b.getQueue(topic)

	select {
	case ch <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *MemoryBroker) Pop(ctx context.Context, topic string) (*Message, error) {
	ch := b.getQueue(topic)

	select {
	case msg := <-ch:
		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (b *MemoryBroker) Ack(ctx context.Context, topic string, msg *Message) error {
	// Memory broker doesn't currently support reliable processing queues
	return nil
}

func (b *MemoryBroker) Len(ctx context.Context, topic string) (int64, error) {
	ch := b.getQueue(topic)
	return int64(len(ch)), nil
}

func (b *MemoryBroker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for name, ch := range b.queues {
		close(ch)
		delete(b.queues, name)
	}
	return nil
}
