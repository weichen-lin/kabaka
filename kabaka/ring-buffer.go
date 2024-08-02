package kabaka

import "sync"

type RingBuffer struct {
	buffer   []*Message
	size     int
	start    int
	end      int
	count    int
	mu       sync.RWMutex
	notEmpty *sync.Cond
	notFull  *sync.Cond
}

func NewRingBuffer(size int) *RingBuffer {
	rb := &RingBuffer{
		buffer: make([]*Message, size),
		size:   size,
	}
	rb.notEmpty = sync.NewCond(&rb.mu)
	rb.notFull = sync.NewCond(&rb.mu)
	return rb
}

func (rb *RingBuffer) Push(msg *Message) bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	for rb.count == rb.size {
		rb.notFull.Wait()
	}

	rb.buffer[rb.end] = msg
	rb.end = (rb.end + 1) % rb.size
	rb.count++

	rb.notEmpty.Signal()

	return true
}

func (rb *RingBuffer) Pop() *Message {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	for rb.count == 0 {
		rb.notEmpty.Wait()
	}

	msg := rb.buffer[rb.start]
	rb.start = (rb.start + 1) % rb.size
	rb.count--

	rb.notFull.Signal()

	return msg
}
