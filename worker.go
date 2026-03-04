package kabaka

import (
	"context"
	"math"
	"time"
)

type Worker struct {
	id         string
	jobChannel chan *Message
	quit       chan bool
	kabaka     *Kabaka
}

func NewWorker(k *Kabaka) *Worker {
	return &Worker{
		id:         NewUUID(),
		jobChannel: make(chan *Message),
		quit:       make(chan bool),
		kabaka:     k,
	}
}

func (w *Worker) start() {
	go func() {
		for {
			// Join the SHARED pool
			select {
			case w.kabaka.workerPool <- w.jobChannel:
			case <-w.quit:
				return
			case <-w.kabaka.ctx.Done():
				return
			}

			// Wait for a job
			select {
			case msg, ok := <-w.jobChannel:
				if !ok {
					return
				}
				w.process(msg)
			case <-w.quit:
				return
			case <-w.kabaka.ctx.Done():
				return
			}
		}
	}()
}

func (w *Worker) process(msg *Message) {
	start := time.Now()

	// 1. Get the topic config using the hashed internal name
	w.kabaka.mu.RLock()
	topic, ok := w.kabaka.topics[msg.InternalName]
	w.kabaka.mu.RUnlock()

	if !ok {
		// Should not happen as dispatch already checked, but for safety
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), topic.processTimeout)
	defer cancel()

	// 2. Execute business logic
	err := topic.handler(ctx, msg)
	duration := time.Since(start)

	// 3. Retry Logic
	if err != nil && msg.Retry > 0 {
		msg.Retry--
		backoff := topic.retryDelay * time.Duration(math.Pow(2, float64(3-msg.Retry-1))) // Simplified backoff
		
		retryCtx, retryCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = w.kabaka.broker.PushDelayed(retryCtx, msg, backoff)
		retryCancel()
	}

	// 4. Finalize: Shared Ack
	finishCtx, finishCancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = w.kabaka.broker.Finish(finishCtx, msg, err, duration)
	finishCancel()
}

func (w *Worker) stop() {
	close(w.quit)
}
