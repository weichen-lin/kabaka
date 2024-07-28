package kabaka

import (
	"fmt"
)

type Node struct {
    value int
    next  *Node
}

type Queue struct {
    head *Node
    tail *Node
}

func (q *Queue) Enqueue(value int) {
    newNode := &Node{value: value}
    if q.tail != nil {
        q.tail.next = newNode
    }
    q.tail = newNode
    if q.head == nil {
        q.head = newNode
    }
}

func (q *Queue) Dequeue() (int, error) {
    if q.head == nil {
        return 0, fmt.Errorf("queue is empty")
    }
    value := q.head.value
    q.head = q.head.next
    if q.head == nil {
        q.tail = nil
    }
    return value, nil
}

func (q *Queue) Peek() (int, error) {
    if q.head == nil {
        return 0, fmt.Errorf("queue is empty")
    }
    return q.head.value, nil
}

func (q *Queue) IsEmpty() bool {
    return q.head == nil
}
