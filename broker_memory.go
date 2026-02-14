package kabaka

import (
	"context"
	"sync"
)

type MemoryBroker struct {
	mu     sync.RWMutex
	queues map[string]chan *Message
	buffer int
}

func NewMemoryBroker(buffer int) *MemoryBroker {
	return &MemoryBroker{
		queues: make(map[string]chan *Message),
		buffer: buffer,
	}
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
