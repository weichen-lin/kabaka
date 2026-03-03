package kabaka

import (
	"context"
	"sync"
	"time"
)

// HandleFunc is a user-defined message processing function type.
type HandleFunc func(ctx context.Context, msg *Message) error

type Topic struct {
	Name         string
	InternalName string

	instanceID     string
	workers        []*Worker
	workerPool     chan chan *Message
	broker         Broker
	maxWorkers     int
	maxRetries     int
	retryDelay     time.Duration
	processTimeout time.Duration
	publishTimeout time.Duration

	handler HandleFunc
	quit    chan bool

	logger Logger
	schema string

	mu       sync.RWMutex
	stopOnce sync.Once
}

type Option func(*Topic)

func newTopic(name string, internalName string, broker Broker, handler HandleFunc, options ...Option) *Topic {
	t := &Topic{
		Name:           name,
		InternalName:   internalName,
		instanceID:     NewUUID(),
		broker:         broker,
		maxRetries:     3,
		maxWorkers:     20,
		retryDelay:     5 * time.Second,
		processTimeout: 10 * time.Second,
		publishTimeout: 2 * time.Second,
		logger:         &DefaultLogger{},
		handler:        handler,
		quit:           make(chan bool),
	}

	for _, opt := range options {
		opt(t)
	}

	t.workerPool = make(chan chan *Message, t.maxWorkers)
	t.start()

	return t
}

func WithMaxWorkers(maxWorkers int) Option {
	return func(t *Topic) { t.maxWorkers = maxWorkers }
}

func WithRetryDelay(retryDelay time.Duration) Option {
	return func(t *Topic) { t.retryDelay = retryDelay }
}

func WithProcessTimeout(processTimeout time.Duration) Option {
	return func(t *Topic) { t.processTimeout = processTimeout }
}

func WithPublishTimeout(publishTimeout time.Duration) Option {
	return func(t *Topic) { t.publishTimeout = publishTimeout }
}

func WithMaxRetries(maxRetries int) Option {
	return func(t *Topic) { t.maxRetries = maxRetries }
}

func (t *Topic) start() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.workers != nil {
		return
	}

	t.workers = make([]*Worker, 0, t.maxWorkers)
	for i := 0; i < t.maxWorkers; i++ {
		worker := NewWorker(t)
		t.workers = append(t.workers, worker)
		worker.start()
	}
}

func (t *Topic) stop() {
	t.stopOnce.Do(func() {
		close(t.quit)

		t.mu.RLock()
		workers := make([]*Worker, len(t.workers))
		copy(workers, t.workers)
		t.mu.RUnlock()

		var wg sync.WaitGroup
		wg.Add(len(workers))
		for _, worker := range workers {
			go func(w *Worker) {
				defer wg.Done()
				w.stop()
			}(worker)
		}
		wg.Wait()

		t.mu.Lock()
		t.workers = nil
		t.mu.Unlock()
	})
}

func (t *Topic) receive(msg *Message) {
	go func() {
		var jobChannel chan *Message
		select {
		case jobChannel = <-t.workerPool:
		case <-t.quit:
			t.returnToQueue(msg)
			return
		}

		select {
		case jobChannel <- msg:
		case <-t.quit:
			t.returnToQueue(msg)
			return
		}
	}()
}

func (t *Topic) returnToQueue(msg *Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := t.broker.Push(ctx, t.InternalName, msg); err == nil {
		_ = t.broker.Finish(ctx, t.InternalName, msg, nil, 0)
	}
}

func (t *Topic) publish(message []byte) error {
	msg := t.generateTraceMessage(message)
	ctx, cancel := context.WithTimeout(context.Background(), t.publishTimeout)
	defer cancel()

	if err := t.broker.Push(ctx, t.InternalName, msg); err != nil {
		return err
	}

	t.logger.Info(&LogMessage{
		TopicName: t.Name,
		Action:    Publish,
		MessageID: msg.Id,
		Message:   string(message),
		CreatedAt: time.Now(),
	})
	return nil
}

func (t *Topic) publishDelayed(message []byte, delay time.Duration) error {
	msg := t.generateTraceMessage(message)
	ctx, cancel := context.WithTimeout(context.Background(), t.publishTimeout)
	defer cancel()

	if err := t.broker.PushDelayed(ctx, t.InternalName, msg, delay); err != nil {
		return err
	}

	t.logger.Info(&LogMessage{
		TopicName: t.Name,
		Action:    Publish,
		MessageID: msg.Id,
		Message:   string(message) + " (delayed)",
		CreatedAt: time.Now(),
	})
	return nil
}

func (t *Topic) generateTraceMessage(message []byte) *Message {
	return &Message{
		Id:        NewUUID(),
		Value:     message,
		Retry:     t.maxRetries,
		CreatedAt: time.Now(),
		Headers:   make(map[string]string),
	}
}
