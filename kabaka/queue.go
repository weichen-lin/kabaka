package kabaka

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var ErrEmptyQueue = errors.New("queue is empty")

type node[T any] struct {
	value *T
	next  *node[T]
}

type Queue[T any] struct {
	sync.RWMutex
	head *node[T]
	tail *node[T]
	size int32
}

func (q *Queue[T]) Enqueue(value *T) {
	q.Lock()
	defer q.Unlock()
	newNode := &node[T]{value: value}
	if q.tail != nil {
		q.tail.next = newNode
	}
	q.tail = newNode
	if q.head == nil {
		q.head = newNode
	}
	atomic.AddInt32(&q.size, 1)
}

func (q *Queue[T]) Dequeue() (*T, error) {
	q.Lock()
	defer q.Unlock()
	if q.head == nil {
		return nil, fmt.Errorf("queue is empty")
	}
	value := q.head.value
	q.head = q.head.next
	if q.head == nil {
		q.tail = nil
	}

	atomic.AddInt32(&q.size, -1)
	return value, nil
}

func (q *Queue[T]) Peek() (*T, error) {
	q.RLock()
	defer q.RUnlock()
	if q.head == nil {
		return nil, fmt.Errorf("queue is empty")
	}
	return q.head.value, nil
}

func (q *Queue[T]) IsEmpty() bool {
	return atomic.LoadInt32(&q.size) == 0
}
