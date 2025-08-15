package kabaka

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Worker struct {
	wg sync.WaitGroup

	id         uuid.UUID
	jobChannel chan *Message
	quit       chan bool
	topic      *Topic
}

func NewWorker(
	topic *Topic,
) *Worker {
	id := uuid.New()

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

		for {
			select {
			case msg, ok := <-w.jobChannel:
				if !ok {
					w.topic.workerLeave()
					return
				}

				w.topic.workerStartWork()

				now := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), w.topic.processTimeout)

				done := make(chan error, 1)
				go func() {
					w.topic.jobInWorkerStart()
					defer w.topic.jobInWorkerFinish()

					done <- w.topic.handler(msg)
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
					w.handelTimeOut(msg, duration)
				}

				cancel()

				w.topic.workerPool <- w.jobChannel

				w.topic.workerFinishWork()

			case <-w.quit:
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
}

func (w *Worker) handelTimeOut(msg *Message, duration time.Duration) {

	msg.Retry--

	w.topic.logger.Warn(&LogMessage{
		TopicName:     w.topic.Name,
		Action:        Consume,
		MessageID:     msg.ID,
		Message:       string(msg.Value),
		MessageStatus: Retry,
		SpendTime:     duration.Milliseconds(),
		CreatedAt:     time.Now(),
		Headers:       msg.Headers,
	})

	if msg.Retry >= 0 {
		backoff := w.topic.retryDelay.Abs() * time.Duration(math.Pow(2, float64(w.topic.maxRetries-msg.Retry)))

		time.Sleep(backoff)

		select {
		case w.topic.messageQueue <- msg:
			fmt.Printf("retry attempt %d/%d", w.topic.maxRetries-msg.Retry, w.topic.maxRetries)
		default:
			fmt.Println("queue full, cannot retry message")
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
			fmt.Printf("retry attempt %d/%d", w.topic.maxRetries-msg.Retry, w.topic.maxRetries)
		default:
			fmt.Println("queue full, cannot retry message")
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
		CreatedAt:     time.Now(),
		Headers:       msg.Headers,
	})
}
