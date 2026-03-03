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

func NewWorker(topic *Topic) *Worker {
	return &Worker{
		id:         NewUUID(),
		jobChannel: make(chan *Message, 1),
		quit:       make(chan bool),
		topic:      topic,
	}
}

func (w *Worker) start() chan *Message {
	w.wg.Go(func() {
		// Join the pool
		select {
		case w.topic.workerPool <- w.jobChannel:
		case <-w.quit:
			return
		}

		for {
			select {
			case msg, ok := <-w.jobChannel:
				if !ok {
					return
				}

				w.process(msg)

				// Back to pool
				select {
				case w.topic.workerPool <- w.jobChannel:
				case <-w.quit:
					return
				}
			case <-w.quit:
				return
			}
		}
	})

	return w.jobChannel
}

func (w *Worker) process(msg *Message) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), w.topic.processTimeout)
	defer cancel()

	// Execute business logic
	err := w.topic.handler(ctx, msg)
	duration := time.Since(start)

	// Logging
	if err != nil {
		w.topic.logger.Error(&LogMessage{
			TopicName: w.topic.Name,
			Action:    Consume,
			MessageID: msg.Id,
			Message:   err.Error(),
			SpendTime: duration.Milliseconds(),
		})
	} else {
		w.topic.logger.Info(&LogMessage{
			TopicName: w.topic.Name,
			Action:    Consume,
			MessageID: msg.Id,
			Message:   "success",
			SpendTime: duration.Milliseconds(),
		})
	}

	// Retry Logic: If failed and has retries left, push back to delayed queue
	if err != nil && msg.Retry > 0 {
		msg.Retry--
		backoff := w.topic.retryDelay * time.Duration(math.Pow(2, float64(w.topic.maxRetries-msg.Retry-1)))

		// Re-push as delayed task
		retryCtx, retryCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = w.topic.broker.PushDelayed(retryCtx, w.topic.InternalName, msg, backoff)
		retryCancel()
	}

	// Finalize: Ack and record stats in one shot
	finishCtx, finishCancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = w.topic.broker.Finish(finishCtx, w.topic.InternalName, msg, err, duration)
	finishCancel()
}

func (w *Worker) stop() {
	select {
	case <-w.quit:
	default:
		close(w.quit)
	}
	w.wg.Wait()
	w.drainJobChannel()
	close(w.jobChannel)
}

func (w *Worker) drainJobChannel() {
	for {
		select {
		case msg, ok := <-w.jobChannel:
			if !ok {
				return
			}
			// Return to broker
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			if err := w.topic.broker.Push(ctx, w.topic.InternalName, msg); err == nil {
				w.topic.logger.Info(&LogMessage{
					TopicName: w.topic.Name,
					Action:    "requeue_on_stop",
					MessageID: msg.Id,
					Message:   "worker stopped, message requeued",
				})
			}

			cancel()
		default:
			return
		}
	}
}
