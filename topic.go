package kabaka

import (
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type HandleFunc func(msg *Message) error

type Topic struct {
	Name string

	workers        []*Worker
	workerPool     chan chan *Message
	messageQueue   chan *Message
	maxWorkers     int
	bufferSize     int
	maxRetries     int
	retryDelay     time.Duration
	processTimeout time.Duration

	handler HandleFunc
	quit    chan bool

	logger Logger

	// metrics
	activeWorkers int32
	busyWorkers   int32
	onGoingJobs   int32
}

func NewTopic(name string, options *Options, hadler HandleFunc) *Topic {
	t := &Topic{
		Name:           name,
		workerPool:     make(chan chan *Message, options.MaxWorkers),
		messageQueue:   make(chan *Message, options.BufferSize),
		bufferSize:     options.BufferSize,
		maxRetries:     options.MaxRetries,
		maxWorkers:     options.MaxWorkers,
		retryDelay:     options.RetryDelay,
		processTimeout: options.ProcessTimeout,
		logger:         options.Logger,
		handler:        hadler,
		quit:           make(chan bool),
	}

	t.Start()

	return t
}

func (t *Topic) Start() {
	t.workers = make([]*Worker, t.maxWorkers)
	for i := 0; i < t.maxWorkers; i++ {
		worker := NewWorker(uuid.New(), t)
		t.workers[i] = worker
		worker.Start()
	}

	go t.dispatch()
}

func (t *Topic) Stop() {
	for _, worker := range t.workers {
		worker.Stop()
	}
}

func (t *Topic) dispatch() {
	for {
		select {
		case msg := <-t.messageQueue:
			jobChannel := <-t.workerPool
			jobChannel <- msg
		case <-t.quit:
			return
		}
	}
}

func (t *Topic) Publish(message []byte) error {
	msg := t.generateTraceMessage(message)

	// 使用 select 非阻塞發送
	select {
	case t.messageQueue <- msg:
		t.logger.Info(&LogMessage{
			TopicName:     t.Name,
			Action:        Publish,
			MessageID:     msg.ID,
			Message:       string(msg.Value),
			MessageStatus: Success,
			SpendTime:     0,
			CreatedAt:     time.Now(),
		})
		return nil
	case <-time.After(100 * time.Millisecond):
		return ErrPublishTimeout
	}
}

func (t *Topic) generateTraceMessage(message []byte) *Message {
	headers := make(map[string]string)

	msg := &Message{
		ID:        uuid.New(),
		Value:     message,
		Retry:     t.maxRetries,
		CreatedAt: time.Now(),
		Headers:   headers,
	}

	return msg
}

func (t *Topic) workerJoin() {
	atomic.AddInt32(&t.activeWorkers, 1)
}

func (t *Topic) workerStartWork() {
	atomic.AddInt32(&t.activeWorkers, -1)
	atomic.AddInt32(&t.busyWorkers, 1)
}

func (t *Topic) workerFinishWork() {
	atomic.AddInt32(&t.activeWorkers, 1)
	atomic.AddInt32(&t.busyWorkers, -1)
}

func (t *Topic) jobInWorkerStart() {
	atomic.AddInt32(&t.onGoingJobs, 1)
}

func (t *Topic) jobInWorkerFinish() {
	atomic.AddInt32(&t.onGoingJobs, -1)
}

func (t *Topic) GetActiveWorkers() int32 {
	return atomic.LoadInt32(&t.activeWorkers)
}

func (t *Topic) GetBusyWorkers() int32 {
	return atomic.LoadInt32(&t.busyWorkers)
}

func (t *Topic) GetOnGoingJobs() int32 {
	return atomic.LoadInt32(&t.onGoingJobs)
}
