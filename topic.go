package kabaka

import (
	"sync"
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

	mu           sync.RWMutex
	dispatcherWg sync.WaitGroup
	stopOnce     sync.Once
}

type Option func(*Topic)

func newTopic(name string, hadler HandleFunc, options ...Option) *Topic {
	t := &Topic{
		Name:           name,
		bufferSize:     24,
		maxRetries:     3,
		maxWorkers:     20,
		retryDelay:     5 * time.Second,
		processTimeout: 10 * time.Second,
		logger:         &DefaultLogger{},
		handler:        hadler,
		quit:           make(chan bool),
	}

	for _, opt := range options {
		opt(t)
	}

	t.workerPool = make(chan chan *Message, t.maxWorkers)
	t.messageQueue = make(chan *Message, t.bufferSize)

	t.start()

	return t
}

func WithMaxWorkers(maxWorkers int) Option {
	return func(t *Topic) {
		t.maxWorkers = maxWorkers
	}
}

func WithBufferSize(bufferSize int) Option {
	return func(t *Topic) {
		t.bufferSize = bufferSize
		t.messageQueue = make(chan *Message, bufferSize)
	}
}

func WithRetryDelay(retryDelay time.Duration) Option {
	return func(t *Topic) {
		t.retryDelay = retryDelay
	}
}

func WithProcessTimeout(processTimeout time.Duration) Option {
	return func(t *Topic) {
		t.processTimeout = processTimeout
	}
}

func WithMaxRetries(maxRetries int) Option {
	return func(t *Topic) {
		t.maxRetries = maxRetries
	}
}

func WithLogger(logger Logger) Option {
	return func(t *Topic) {
		t.logger = logger
	}
}

func (t *Topic) start() {
	t.mu.Lock()

	// 如果 workers 已經初始化，則不需要再次初始化
	if t.workers != nil {
		t.mu.Unlock()
		return
	}

	t.workers = make([]*Worker, t.maxWorkers)
	for i := 0; i < t.maxWorkers; i++ {
		worker := NewWorker(t)
		t.workers[i] = worker
		jobCh := worker.start()
		if jobCh == nil {
			t.logger.Error(
				&LogMessage{
					TopicName:     t.Name,
					Action:        WorkerStart,
					MessageID:     uuid.Nil,
					Message:       "worker start failed",
					MessageStatus: WorkerStartFailed,
					SpendTime:     0,
					CreatedAt:     time.Now(),
					Headers:       nil,
				},
			)
			continue
		}
		t.workerJoin()
	}
	t.mu.Unlock()

	t.dispatcherWg.Add(1)
	go t.dispatch()
}

func (t *Topic) stop() {
	t.stopOnce.Do(func() {
		// 1. 先關閉 quit channel，通知所有 goroutine 停止
		close(t.quit)

		// 2. 等待 dispatcher 退出
		t.dispatcherWg.Wait()

		// 3. 停止所有 worker 並等待它們完成
		t.mu.RLock()
		workers := make([]*Worker, len(t.workers))
		copy(workers, t.workers) // 複製 slice 以避免並發修改
		t.mu.RUnlock()

		var wg sync.WaitGroup
		wg.Add(len(workers))
		for _, worker := range workers {
			if worker != nil {
				go func(w *Worker) {
					defer wg.Done()
					w.stop()
				}(worker)
			} else {
				wg.Done()
			}
		}
		wg.Wait() // 等待所有 worker 停止

		// 4. 清理資源
		t.mu.Lock()
		t.workers = nil
		t.mu.Unlock()

		// 如果需要，可以關閉其他 channel
		// close(t.workerPool)
		// close(t.messageQueue) // 謹慎處理，如果 Publish 可能仍被呼叫
	})
}

func (t *Topic) dispatch() {
	defer t.dispatcherWg.Done()

	for {
		select {
		case msg := <-t.messageQueue:
			var jobChannel chan *Message
			select {
			case jobChannel = <-t.workerPool:
			case <-t.quit:
				return
			}

			select {
			case jobChannel <- msg:
			case <-t.quit:
				return
			}
		case <-t.quit:
			return
		}
	}
}

func (t *Topic) publish(message []byte) error {
	msg := t.generateTraceMessage(message)

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
			Headers:       msg.Headers,
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

func (t *Topic) workerLeave() {
	atomic.AddInt32(&t.activeWorkers, -1)
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

func (t *Topic) workerStop(id uuid.UUID) {
	for i, worker := range t.workers {
		if worker.id == id {
			// 重新分配 worker
			t.workers = append(t.workers[:i], t.workers[i+1:]...)
			// 更新 active workers
			atomic.AddInt32(&t.activeWorkers, -1)
			break
		}
	}
}
