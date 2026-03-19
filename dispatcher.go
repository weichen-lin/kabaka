package kabaka

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"time"

	"github.com/weichen-lin/kabaka/broker"
)

// Start begins the message dispatcher.
func (k *Kabaka) Start() {
	// Register with instance registry if broker supports it
	if reg, ok := k.broker.(broker.InstanceRegistry); ok {
		k.instanceID = NewUUID()
		hostname, _ := os.Hostname()
		ctx, cancel := context.WithTimeout(k.ctx, k.brokerTimeout)
		reg.Join(ctx, &broker.InstanceInfo{
			ID:            k.instanceID,
			Hostname:      hostname,
			StartedAt:     time.Now(),
			LastHeartbeat: time.Now(),
			Workers:       k.maxWorkers,
		})
		cancel()
	}

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
	maxRetries := topic.maxRetries
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
				attempt := max(maxRetries-msg.Retry, 0)
				backoff := retryDelay * time.Duration(math.Pow(2, float64(attempt)))

				retryCtx, retryCancel := context.WithTimeout(context.Background(), k.brokerTimeout)
				pushErr := k.broker.PushDelayed(retryCtx, msg, backoff)
				retryCancel()

				if pushErr != nil {
					msg.Retry++ // Restore: the retry didn't actually happen
					// PushDelayed failed — do NOT Finish so stale processing cleanup can requeue
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
					return
				}

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

				// Record Audit Result
				k.finalizeAudit(topic, msg, broker.StatusDead, err, duration)

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

			// Record Audit Result
			k.finalizeAudit(topic, msg, broker.StatusSuccess, nil, duration)

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
		finishErr := k.broker.Finish(finishCtx, msg, err, duration)
		if finishErr != nil {
			// Log finish error
			k.logger.Error(&LogMessage{
				TopicName:     topic.Name,
				Action:        Consume,
				MessageID:     msg.Id,
				Message:       "Failed to acknowledge message: " + finishErr.Error(),
				MessageStatus: Error,
				SpendTime:     duration.Milliseconds(),
				CreatedAt:     msg.CreatedAt,
				Headers:       msg.Headers,
			})
		}
		finishCancel()
	}
}

func (k *Kabaka) finalizeAudit(topic *Topic, msg *broker.Message, status broker.JobStatus, err error, duration time.Duration) {
	if topic.historyLimit <= 0 {
		return
	}

	payload := json.RawMessage(msg.Value)
	if !json.Valid(msg.Value) {
		escaped, _ := json.Marshal(string(msg.Value))
		payload = json.RawMessage(escaped)
	}

	result := &broker.JobResult{
		ID:         msg.Id,
		Topic:      topic.Name,
		Payload:    payload,
		Status:     status,
		Attempts:   topic.maxRetries - msg.Retry + 1, // Total attempts made
		DurationMs: duration.Milliseconds(),
		CreatedAt:  msg.CreatedAt,
		FinishedAt: time.Now(),
	}
	if err != nil {
		result.Error = err.Error()
	}

	ctx, cancel := context.WithTimeout(k.ctx, k.brokerTimeout)
	defer cancel()

	if storeErr := k.broker.StoreResult(ctx, result, topic.historyLimit); storeErr != nil {
		k.logger.Error(&LogMessage{
			TopicName:     topic.Name,
			Action:        Consume,
			MessageID:     msg.Id,
			Message:       "Failed to store audit result: " + storeErr.Error(),
			MessageStatus: Error,
			CreatedAt:     time.Now(),
		})
	}
}
