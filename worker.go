package kabaka

import (
	"context"
	"math"
	"sync"
	"time"
)

type Worker struct {
	wg sync.WaitGroup

	id         string
	jobChannel chan *Message
	quit       chan bool
	topic      *Topic
}

func NewWorker(
	topic *Topic,
) *Worker {
	id := NewUUID()

	return &Worker{
		id:         id,
		jobChannel: make(chan *Message),
		quit:       make(chan bool),
		topic:      topic,
	}
}

func (w *Worker) start() chan *Message {
	w.wg.Add(1)

	go func() {
		defer w.wg.Done()

		select {
		case w.topic.workerPool <- w.jobChannel:
			w.topic.workerJoin()
		case <-w.quit:
			w.topic.workerLeave()
			return
		}

		for {
			select {
			case msg, ok := <-w.jobChannel:
				if !ok {
					w.topic.workerLeave()
					return
				}

				w.topic.workerStartWork()

				now := time.Now().UTC()
				ctx, cancel := context.WithTimeout(context.Background(), w.topic.processTimeout)

				done := make(chan error, 1)
				go func() {
					w.topic.jobInWorkerStart()
					defer w.topic.jobInWorkerFinish()

					done <- w.topic.handler(ctx, msg)
				}()

				select {
				case err := <-done:
					duration := time.Since(now)
					if err != nil {
						w.handleError(err, msg, duration)
					} else {
						w.handleSuccess(msg, duration)
					}
				case <-ctx.Done():
					duration := time.Since(now)
					w.handleTimeOut(msg, duration)
				case <-w.quit:
					// 收到停止信號，但正在處理消息
					// 取消 context 並等待處理完成或超時
					cancel()
					select {
					case err := <-done:
						duration := time.Since(now)
						if err != nil {
							w.handleError(err, msg, duration)
						} else {
							w.handleSuccess(msg, duration)
						}
					case <-time.After(w.topic.processTimeout):
						// 如果超時，將消息重新放回隊列
						duration := time.Since(now)
						w.handleTimeOut(msg, duration)
					}
					w.topic.workerFinishWork()
					w.topic.workerLeave()
					return
				}

				cancel()

				// 先更新計數器，再重新加入 pool
				w.topic.workerFinishWork()

				// 重新將自己加入 worker pool，準備接收下一個任務
				select {
				case w.topic.workerPool <- w.jobChannel:
				case <-w.quit:
					w.topic.workerLeave()
					return
				}
			case <-w.quit:
				w.topic.workerLeave()
				return
			}
		}
	}()

	return w.jobChannel
}

func (w *Worker) stop() {
	// 使用 sync.Once 防止多次關閉或檢查 channel 是否已關閉
	select {
	case <-w.quit:
		// 已關閉
	default:
		close(w.quit) // 向 goroutine 發出停止信號
	}
	w.wg.Wait() // 等待 goroutine 實際退出

	// 清理 jobChannel 中剩餘的消息，將它們重新放回 messageQueue
	w.drainJobChannel()

	// 關閉 jobChannel，防止其他 goroutine 繼續向它發送消息
	close(w.jobChannel)
}

// drainJobChannel 清空 jobChannel 中的剩餘消息並重新放回 messageQueue
func (w *Worker) drainJobChannel() {
	for {
		select {
		case msg, ok := <-w.jobChannel:
			if !ok {
				return // channel 已關閉
			}
			// 將未處理的消息重新放回 messageQueue
			// 使用非阻塞的 select 避免死鎖
			select {
			case w.topic.messageQueue <- msg:
				// 成功放回隊列
				if w.topic.logger != nil {
					w.topic.logger.Warn(&LogMessage{
						TopicName:     w.topic.Name,
						Action:        Consume,
						MessageID:     msg.ID,
						Message:       "message returned to queue due to worker shutdown",
						MessageStatus: Retry,
						SpendTime:     0,
						CreatedAt:     time.Now().UTC(),
						Headers:       msg.Headers,
					})
				}
			default:
				// messageQueue 已滿，記錄警告
				if w.topic.logger != nil {
					w.topic.logger.Error(&LogMessage{
						TopicName:     w.topic.Name,
						Action:        Consume,
						MessageID:     msg.ID,
						Message:       "failed to return message to queue during worker shutdown - message lost",
						MessageStatus: Error,
						SpendTime:     0,
						CreatedAt:     time.Now().UTC(),
						Headers:       msg.Headers,
					})
				}
				// TODO: 可以考慮將這些消息放入 dead letter queue
			}
		default:
			return // 沒有更多消息
		}
	}
}

func (w *Worker) handleTimeOut(msg *Message, duration time.Duration) {

	msg.Retry--

	w.topic.logger.Warn(&LogMessage{
		TopicName:     w.topic.Name,
		Action:        Consume,
		MessageID:     msg.ID,
		Message:       string(msg.Value),
		MessageStatus: Timeout, // 使用 Timeout 而不是 Retry 狀態
		SpendTime:     duration.Milliseconds(),
		CreatedAt:     time.Now().UTC(),
		Headers:       msg.Headers,
	})

	if msg.Retry > 0 { // 與 handleError 保持一致的條件判斷
		backoff := w.topic.retryDelay.Abs() * time.Duration(math.Pow(2, float64(w.topic.maxRetries-msg.Retry)))

		time.Sleep(backoff)

		select {
		case w.topic.messageQueue <- msg:
		default:
			// TODO: move to dead letter queue
		}
	}
}

func (w *Worker) handleError(err error, msg *Message, duration time.Duration) {
	msg.Retry--

	w.topic.logger.Error(&LogMessage{
		TopicName:     w.topic.Name,
		Action:        Consume,
		MessageID:     msg.ID,
		Message:       err.Error(),
		MessageStatus: Retry,
		SpendTime:     duration.Milliseconds(),
		CreatedAt:     time.Now().UTC(),
		Headers:       msg.Headers,
	})

	if msg.Retry > 0 {
		backoff := w.topic.retryDelay.Abs() * time.Duration(math.Pow(2, float64(w.topic.maxRetries-msg.Retry)))

		time.Sleep(backoff)

		select {
		case w.topic.messageQueue <- msg:
		default:
			// TODO: move to dead letter queue
		}
	}
}

func (w *Worker) handleSuccess(msg *Message, duration time.Duration) {
	if w.topic.logger == nil {
		return
	}

	w.topic.logger.Info(&LogMessage{
		TopicName:     w.topic.Name,
		Action:        Consume,
		MessageID:     msg.ID,
		Message:       string(msg.Value),
		MessageStatus: Success,
		SpendTime:     duration.Milliseconds(),
		CreatedAt:     time.Now().UTC(),
		Headers:       msg.Headers,
	})
}
