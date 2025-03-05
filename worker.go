package kabaka

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
)

type Worker struct {
	id         uuid.UUID
	jobChannel chan *Message
	quit       chan bool
	topic      *Topic
}

func NewWorker(id uuid.UUID, topic *Topic) *Worker {
	return &Worker{
		id:         id,
		jobChannel: make(chan *Message),
		quit:       make(chan bool),
		topic:      topic,
	}
}

func (w *Worker) Start() {
	w.topic.workerJoin()

	go func() {

		w.topic.workerPool <- w.jobChannel

		for {

			select {
			case msg := <-w.jobChannel:
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
						w.handleError(msg, duration)
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
}

func (w *Worker) Stop() {
	w.quit <- true
}

func (w *Worker) handelTimeOut(msg *Message, duration time.Duration) {

	msg.Retry--

	if msg.Retry >= 0 {
		w.topic.logger.Error(&LogMessage{
			TopicName:     w.topic.Name,
			Action:        Consume,
			MessageID:     msg.ID,
			Message:       string(msg.Value),
			MessageStatus: Retry,
			SpendTime:     duration.Milliseconds(),
			CreatedAt:     time.Now(),
		})

		backoff := w.topic.retryDelay.Abs() * time.Duration(math.Pow(2, float64(w.topic.maxRetries-msg.Retry)))

		time.Sleep(backoff)

		select {
		case w.topic.messageQueue <- msg:
			fmt.Printf("retry attempt %d/%d", w.topic.maxRetries-msg.Retry, w.topic.maxRetries)
		default:
			fmt.Println("queue full, cannot retry message")
		}
	} else {
		w.topic.logger.Error(&LogMessage{
			TopicName:     w.topic.Name,
			Action:        Consume,
			MessageID:     msg.ID,
			Message:       string(msg.Value),
			MessageStatus: Error,
			SpendTime:     duration.Milliseconds(),
			CreatedAt:     time.Now(),
		})
	}
}

func (w *Worker) handleError(msg *Message, duration time.Duration) {
	msg.Retry--

	if msg.Retry >= 0 {
		w.topic.logger.Error(&LogMessage{
			TopicName:     w.topic.Name,
			Action:        Consume,
			MessageID:     msg.ID,
			Message:       string(msg.Value),
			MessageStatus: Retry,
			SpendTime:     duration.Milliseconds(),
			CreatedAt:     time.Now(),
		})

		backoff := w.topic.retryDelay.Abs() * time.Duration(math.Pow(2, float64(w.topic.maxRetries-msg.Retry)))

		time.Sleep(backoff)

		select {
		case w.topic.messageQueue <- msg:
			fmt.Printf("retry attempt %d/%d", w.topic.maxRetries-msg.Retry, w.topic.maxRetries)
		default:
			fmt.Println("queue full, cannot retry message")
		}
	} else {
		w.topic.logger.Error(&LogMessage{
			TopicName:     w.topic.Name,
			Action:        Consume,
			MessageID:     msg.ID,
			Message:       string(msg.Value),
			MessageStatus: Error,
			SpendTime:     duration.Milliseconds(),
			CreatedAt:     time.Now(),
		})
	}
}

func (w *Worker) handleSuccess(msg *Message, duration time.Duration) {
	w.topic.logger.Info(&LogMessage{
		TopicName:     w.topic.Name,
		Action:        Consume,
		MessageID:     msg.ID,
		Message:       string(msg.Value),
		MessageStatus: Success,
		SpendTime:     duration.Milliseconds(),
		CreatedAt:     time.Now(),
	})
}
