package kabaka

import (
	"context"
	"math"
	"time"

	"github.com/weichen-lin/kabaka/broker"
)

// Start begins the message dispatcher.
func (k *Kabaka) Start() {
	k.wg.Add(1)
	go k.dispatch()
}

func (k *Kabaka) dispatch() {
	defer k.wg.Done()

	// In shared queue architecture, we just watch the single shared channel
	taskCh, err := k.broker.Watch(k.ctx)
	if err != nil {
		return
	}

	for {
		select {
		case <-k.ctx.Done():
			return
		case task, ok := <-taskCh:
			if !ok {
				return
			}

			k.mu.RLock()
			topic, ok := k.topics[task.Message.InternalName]
			k.mu.RUnlock()

			if !ok {
				// Topic not found - acknowledge and remove to prevent infinite redelivery
				k.logger.Error(
					&LogMessage{
						TopicName:     task.Message.InternalName,
						Action:        Publish,
						MessageID:     task.Message.Id,
						Message:       "Received message for unknown topic; discarding",
						MessageStatus: Error,
						SpendTime:     0,
						CreatedAt:     time.Now(),
						Headers:       task.Message.Headers,
					},
				)
				finishCtx, cancel := context.WithTimeout(context.Background(), k.brokerTimeout)
				if err := k.broker.Finish(finishCtx, task.Message, nil, 0); err != nil {
					k.logger.Error(&LogMessage{
						TopicName:     task.Message.InternalName,
						Action:        Consume,
						MessageID:     task.Message.Id,
						Message:       "Failed to finish unknown topic message: " + err.Error(),
						MessageStatus: Error,
						CreatedAt:     time.Now(),
					})
				}
				cancel()
				continue
			}

			// Handle Paused Topic: Re-queue task to delayed queue to avoid tight loop
			if topic.Paused.Load() {
				finishCtx, cancel := context.WithTimeout(context.Background(), k.brokerTimeout)
				// Re-push to delayed queue to wait (e.g. 1s) before trying again
				k.broker.PushDelayed(finishCtx, task.Message, 1*time.Second)
				// Finish (remove from processing) current task to allow other tasks or topics to be picked up
				k.broker.Finish(finishCtx, task.Message, nil, 0)
				cancel()
				continue
			}

			job := k.buildJob(topic, task.Message)

			// Acquire a semaphore slot; block if all workers are busy
			select {
			case k.sem <- struct{}{}:
			case <-k.ctx.Done():
				return
			}

			go func() {
				defer func() {
					<-k.sem
					k.activejobs.Add(-1)
				}()
				k.activejobs.Add(1)
				job()
			}()
		}
	}
}

func (k *Kabaka) buildJob(topic *Topic, msg *broker.Message) func() {
	processTimeout := topic.processTimeout
	handler := topic.handler
	retryDelay := topic.retryDelay
	stats := topic.stats // capture pointer; safe, atomic ops inside

	return func() {
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), processTimeout)
		defer cancel()

		err := handler(ctx, msg)
		duration := time.Since(start)

		// Record duration
		stats.totalDurationMs.Add(duration.Milliseconds())

		if err != nil {
			if msg.Retry > 0 {
				// Retry path
				stats.RetryTotal.Add(1)
				msg.Retry--
				backoff := retryDelay * time.Duration(math.Pow(2, float64(3-msg.Retry-1)))

				retryCtx, retryCancel := context.WithTimeout(context.Background(), k.brokerTimeout)
				if pushErr := k.broker.PushDelayed(retryCtx, msg, backoff); pushErr != nil {
					k.logger.Error(&LogMessage{
						TopicName:     topic.Name,
						Action:        Consume,
						MessageID:     msg.Id,
						Message:       "Failed to push retry message: " + pushErr.Error(),
						MessageStatus: Error,
						SpendTime:     duration.Milliseconds(),
						CreatedAt:     msg.CreatedAt,
						Headers:       msg.Headers,
					})
				}
				retryCancel()

				// Log retry
				k.logger.Warn(&LogMessage{
					TopicName:     topic.Name,
					Action:        Consume,
					MessageID:     msg.Id,
					Message:       err.Error(),
					MessageStatus: Retry,
					SpendTime:     duration.Milliseconds(),
					CreatedAt:     msg.CreatedAt,
					Headers:       msg.Headers,
				})
			} else {
				// No retries left — final failure
				stats.FailedTotal.Add(1)
				stats.ProcessedTotal.Add(1)

				// Log failure
				k.logger.Error(&LogMessage{
					TopicName:     topic.Name,
					Action:        Consume,
					MessageID:     msg.Id,
					Message:       err.Error(),
					MessageStatus: Error,
					SpendTime:     duration.Milliseconds(),
					CreatedAt:     msg.CreatedAt,
					Headers:       msg.Headers,
				})
			}
		} else {
			stats.ProcessedTotal.Add(1)

			// Log success
			k.logger.Info(&LogMessage{
				TopicName:     topic.Name,
				Action:        Consume,
				MessageID:     msg.Id,
				Message:       "Message processed successfully",
				MessageStatus: Success,
				SpendTime:     duration.Milliseconds(),
				CreatedAt:     msg.CreatedAt,
				Headers:       msg.Headers,
			})
		}

		finishCtx, finishCancel := context.WithTimeout(context.Background(), k.brokerTimeout)
		err = k.broker.Finish(finishCtx, msg, err, duration)
		if err != nil {
			// Log finish error
			k.logger.Error(&LogMessage{
				TopicName:     topic.Name,
				Action:        Consume,
				MessageID:     msg.Id,
				Message:       "Failed to acknowledge message: " + err.Error(),
				MessageStatus: Error,
				SpendTime:     duration.Milliseconds(),
				CreatedAt:     msg.CreatedAt,
				Headers:       msg.Headers,
			})
		}
		finishCancel()
	}
}
