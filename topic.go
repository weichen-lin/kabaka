package kabaka

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// HandleFunc is a user-defined message processing function type.
//
// Parameters:
//   - ctx: A context with timeout control. Users should check this context
//     during long-running operations to respond to timeout or cancellation signals.
//     The timeout duration is controlled by the Topic's processTimeout configuration.
//   - msg: The message object to be processed.
//
// Returns:
//   - error: Returns nil if processing is successful, or an error if it fails.
//     Returning an error will trigger the retry mechanism (if configured).
//
// Best Practices:
//  1. Pass the ctx to all downstream calls that support context (e.g., HTTP requests, database queries).
//  2. Periodically check ctx.Done() in long-running loops.
//  3. When ctx.Done() is triggered, you should return ctx.Err() as soon as possible.
type HandleFunc func(ctx context.Context, msg *Message) error

type Topic struct {
	Name string // Topic name

	workers        []*Worker          // List of Worker instances
	workerPool     chan chan *Message // Pool of available worker job channels
	messageQueue   chan *Message      // Central buffer queue for messages
	maxWorkers     int                // Maximum number of workers
	bufferSize     int                // Buffer size of the message queue
	maxRetries     int                // Maximum number of retries after a message processing failure
	retryDelay     time.Duration      // Delay time before a retry
	processTimeout time.Duration      // Processing timeout for each message
	publishTimeout time.Duration      // Publish timeout for each message

	handler HandleFunc // Registered user-defined message processing function
	quit    chan bool  // Channel to notify all goroutines to stop

	logger Logger // Logger interface

	// --- metrics ---
	activeWorkers int32 // Number of currently idle (waiting for tasks) workers
	busyWorkers   int32 // Number of workers currently processing tasks
	onGoingJobs   int32 // Total number of tasks being processed

	mu           sync.RWMutex   // RWMutex to protect shared resources like the workers slice
	dispatcherWg sync.WaitGroup // Used to wait for the dispatcher goroutine to exit safely
	stopOnce     sync.Once      // Ensures the stop logic is executed only once
}

// Option is a function type for configuring a Topic, following the functional options pattern.
type Option func(*Topic)

// newTopic creates and returns a new Topic instance.
// It initializes based on the provided options and starts the internal dispatcher and workers.
// name: The name of the Topic.
// handler: The function to process messages.
// options: One or more Option functions to customize the Topic's configuration.
func newTopic(name string, hadler HandleFunc, options ...Option) *Topic {
	t := &Topic{
		Name:           name,
		bufferSize:     24,
		maxRetries:     3,
		maxWorkers:     20,
		retryDelay:     5 * time.Second,
		processTimeout: 10 * time.Second,
		publishTimeout: 2 * time.Second,
		logger:         &DefaultLogger{},
		handler:        hadler,
		quit:           make(chan bool),
	}

	// Apply all incoming configuration options
	for _, opt := range options {
		opt(t)
	}

	// Initialize the core channels
	t.workerPool = make(chan chan *Message, t.maxWorkers)
	t.messageQueue = make(chan *Message, t.bufferSize)

	// Start the worker pool and dispatcher
	t.start()

	return t
}

// WithMaxWorkers is an Option function that sets the maximum number of workers for the Topic.
func WithMaxWorkers(maxWorkers int) Option {
	return func(t *Topic) {
		t.maxWorkers = maxWorkers
	}
}

// WithBufferSize is an Option function that sets the buffer size of the message queue.
func WithBufferSize(bufferSize int) Option {
	return func(t *Topic) {
		t.bufferSize = bufferSize
		t.messageQueue = make(chan *Message, bufferSize)
	}
}

// WithRetryDelay is an Option function that sets the delay time for message retries.
func WithRetryDelay(retryDelay time.Duration) Option {
	return func(t *Topic) {
		t.retryDelay = retryDelay
	}
}

// WithProcessTimeout is an Option function that sets the processing timeout for messages.
func WithProcessTimeout(processTimeout time.Duration) Option {
	return func(t *Topic) {
		t.processTimeout = processTimeout
	}
}

// WithPublishTimeout is an Option function that sets the publish timeout for messages.
func WithPublishTimeout(publishTimeout time.Duration) Option {
	return func(t *Topic) {
		t.publishTimeout = publishTimeout
	}
}

// WithMaxRetries is an Option function that sets the maximum number of retries after a message processing failure.
func WithMaxRetries(maxRetries int) Option {
	return func(t *Topic) {
		t.maxRetries = maxRetries
	}
}

// WithLogger is an Option function that sets a custom logger.
func WithLogger(logger Logger) Option {
	return func(t *Topic) {
		t.logger = logger
	}
}

// start is responsible for initializing and starting all background goroutines for the Topic.
// This includes creating and starting all Workers, as well as starting the core dispatcher.
// This method is not thread-safe and should be called within newTopic or while holding a lock.
func (t *Topic) start() {
	t.mu.Lock()
	defer t.mu.Unlock()
	// If workers are already initialized, no need to initialize again
	if t.workers != nil {
		return
	}

	t.workers = make([]*Worker, 0, t.maxWorkers)
	for i := 0; i < t.maxWorkers; i++ {

		worker := NewWorker(t)

		t.workers = append(t.workers, worker)

		jobCh := worker.start()

		if jobCh == nil {
			t.logger.Error(
				&LogMessage{
					TopicName:     t.Name,
					Action:        WorkerStart,
					MessageID:     NewUUID(),
					Message:       "worker start failed",
					MessageStatus: WorkerStartFailed,
					SpendTime:     0,
					CreatedAt:     time.Now(),
					Headers:       nil,
				},
			)
			continue
		}
		// Remove duplicate logic: worker.start() already adds itself to the workerPool and calls workerJoin()
	}

	// Register and start the dispatcher goroutine
	t.dispatcherWg.Add(1)
	go t.dispatch()
}

// stop gracefully shuts down the Topic.
// It stops accepting new tasks, waits for existing tasks to complete, and closes all background goroutines.
// It uses sync.Once to ensure the shutdown logic is executed only once.
func (t *Topic) stop() {
	t.stopOnce.Do(func() {
		// 1. Close the quit channel to notify the topic to stop accepting new messages and not dispatching tasks to workers
		close(t.quit)

		// 2. Wait for the dispatcher goroutine to exit completely
		t.dispatcherWg.Wait()

		// 3. Stop all workers and wait for them to finish
		t.mu.RLock()
		workers := make([]*Worker, len(t.workers))
		copy(workers, t.workers) // Copy the slice to avoid concurrent modification during iteration
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
		wg.Wait() // Wait for all worker goroutines to stop

		// 4. Clean up resources
		t.mu.Lock()
		t.workers = nil
		t.mu.Unlock()
	})
}

func (t *Topic) dispatch() {
	defer t.dispatcherWg.Done() // 確保在退出時通知 WaitGroup

	for {
		select {
		case msg := <-t.messageQueue: // 等待新消息
			var jobChannel chan *Message
			// 等待一個空閒的 worker
			select {
			case jobChannel = <-t.workerPool: // 獲取到空閒 worker 的任務 channel
			case <-t.quit: // 如果收到退出信號，則返回
				return
			}

			// 將消息發送給獲取到的 worker
			// 使用 recover 防止向已關閉的 channel 發送消息時發生 panic
			func() {
				defer func() {
					if r := recover(); r != nil {
						// jobChannel 已關閉，將消息放回隊列
						select {
						case t.messageQueue <- msg:
							// 成功放回隊列
						default:
							// 隊列已滿，記錄錯誤
							if t.logger != nil {
								t.logger.Error(&LogMessage{
									TopicName:     t.Name,
									Action:        Publish,
									MessageID:     msg.ID,
									Message:       "failed to return message to queue - worker channel closed",
									MessageStatus: Error,
									SpendTime:     0,
									CreatedAt:     time.Now(),
									Headers:       msg.Headers,
								})
							}
						}
					}
				}()

				select {
				case jobChannel <- msg:
					// 成功發送
				case <-t.quit: // 如果在發送時收到退出信號，則將消息放回隊列並返回
					select {
					case t.messageQueue <- msg:
						// 成功放回隊列
					default:
						// 隊列已滿，記錄錯誤
						if t.logger != nil {
							t.logger.Error(&LogMessage{
								TopicName:     t.Name,
								Action:        Publish,
								MessageID:     msg.ID,
								Message:       "failed to return message to queue during shutdown",
								MessageStatus: Error,
								SpendTime:     0,
								CreatedAt:     time.Now(),
								Headers:       msg.Headers,
							})
						}
					}
				}
			}()
		case <-t.quit: // 如果在等待消息時收到退出信號，則返回
			return
		}
	}
}

// publish 是向 Topic 發布新消息的公共接口。
// 它會將消息放入 messageQueue，如果隊列已滿，則會等待一小段時間後超時。
func (t *Topic) publish(message []byte) error {
	msg := t.generateTraceMessage(message)

	select {
	case t.messageQueue <- msg: // 嘗試將消息放入隊列
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
	case <-time.After(t.publishTimeout): // 如果在 publishTimeout 內無法放入，則返回超時錯誤
		return ErrPublishTimeout
	}
}

// generateTraceMessage 是一個輔助函數，用於創建一個帶有追蹤信息的新 Message。
func (t *Topic) generateTraceMessage(message []byte) *Message {
	headers := make(map[string]string)

	msg := &Message{
		ID:        NewUUID(),
		Value:     message,
		Retry:     t.maxRetries,
		CreatedAt: time.Now(),
		Headers:   headers,
	}

	return msg
}

// workerJoin 以原子方式增加 activeWorkers 計數器。
// 當一個新的 worker 啟動並加入池時調用。
func (t *Topic) workerJoin() {
	atomic.AddInt32(&t.activeWorkers, 1)
}

// workerLeave 以原子方式減少 activeWorkers 計數器。
// 當一個 worker 停止時調用。
func (t *Topic) workerLeave() {
	atomic.AddInt32(&t.activeWorkers, -1)
}

// workerStartWork 以原子方式更新計數器，表示一個 worker 開始處理任務。
// activeWorkers 減一，busyWorkers 加一。
func (t *Topic) workerStartWork() {
	atomic.AddInt32(&t.activeWorkers, -1)
	atomic.AddInt32(&t.busyWorkers, 1)
}

// workerFinishWork 以原子方式更新計數器，表示一個 worker 完成了任務。
// activeWorkers 加一，busyWorkers 減一。
func (t *Topic) workerFinishWork() {
	atomic.AddInt32(&t.activeWorkers, 1)
	atomic.AddInt32(&t.busyWorkers, -1)
}

// jobInWorkerStart 以原子方式增加 onGoingJobs 計數器。
// 當一個 worker 內的具體任務處理邏輯開始時調用。
func (t *Topic) jobInWorkerStart() {
	atomic.AddInt32(&t.onGoingJobs, 1)
}

// jobInWorkerFinish 以原子方式減少 onGoingJobs 計數器。
// 當一個 worker 內的具體任務處理邏輯完成時調用。
func (t *Topic) jobInWorkerFinish() {
	atomic.AddInt32(&t.onGoingJobs, -1)
}

// GetActiveWorkers 以原子方式獲取當前空閒的 Worker 數量。
func (t *Topic) GetActiveWorkers() int32 {
	return atomic.LoadInt32(&t.activeWorkers)
}

// GetBusyWorkers 以原子方式獲取當前正在工作的 Worker 數量。
func (t *Topic) GetBusyWorkers() int32 {
	return atomic.LoadInt32(&t.busyWorkers)
}

// GetOnGoingJobs 以原子方式獲取當前正在處理的任務總數。
func (t *Topic) GetOnGoingJobs() int32 {
	return atomic.LoadInt32(&t.onGoingJobs)
}

// workerStop 從 workers 列表中移除指定的 worker。
// 注意：這個函數目前在 stop 流程中沒有被直接使用，
// 它可能用於未來更動態的 worker 管理。
func (t *Topic) workerStop(id string) {
	for i, worker := range t.workers {
		if worker.id == id {
			// 從 slice 中移除 worker
			t.workers = append(t.workers[:i], t.workers[i+1:]...)
			// 更新 active workers 計數
			atomic.AddInt32(&t.activeWorkers, -1)
			break
		}
	}
}
