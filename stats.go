package kabaka

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

// TopicStats holds runtime metrics for a single topic.
type TopicStats struct {
	ProcessedTotal  atomic.Int64
	FailedTotal     atomic.Int64
	RetryTotal      atomic.Int64
	totalDurationMs atomic.Int64 // sum of all job durations in ms (unexported)
}

// AvgDuration returns the average processing duration across all processed jobs.
func (s *TopicStats) AvgDuration() time.Duration {
	processed := s.ProcessedTotal.Load()
	if processed == 0 {
		return 0
	}
	return time.Duration(s.totalDurationMs.Load()/processed) * time.Millisecond
}

// KabakaStats is a snapshot of the overall system state, returned by GetStats.
type KabakaStats struct {
	ActiveJobs int64                    `json:"active_jobs"`
	IdleSlots  int64                    `json:"idle_slots"`
	Queue      QueueStats               `json:"queue"`
	Topics     map[string]TopicSnapshot `json:"topics"`
}

// TopicSnapshot is a point-in-time copy of a single topic's metrics.
type TopicSnapshot struct {
	ProcessedTotal  int64  `json:"processed_total"`
	FailedTotal     int64  `json:"failed_total"`
	RetryTotal      int64  `json:"retry_total"`
	AvgDurationMs   int64  `json:"avg_duration"` // Milliseconds
	SuccessRate     string `json:"success_rate"`
	QueuePending    int64  `json:"queue_pending"`    // messages waiting in queue
	QueueDelayed    int64  `json:"queue_delayed"`    // messages in delayed queue
	QueueProcessing int64  `json:"queue_processing"` // messages currently being processed

	// Configuration
	MaxRetries     int    `json:"max_retries"`
	RetryDelay     int64  `json:"retry_delay"`     // Seconds
	ProcessTimeout int64  `json:"process_timeout"` // Seconds
	InternalName   string `json:"internal_name"`
}

// GetStats returns a point-in-time snapshot of all metrics.
func (k *Kabaka) GetStats() KabakaStats {
	active := k.activejobs.Load()
	stats := KabakaStats{
		ActiveJobs: active,
		IdleSlots:  int64(k.maxWorkers) - active,
		Topics:     make(map[string]TopicSnapshot),
	}

	// Collect broker queue stats
	if k.broker != nil {
		ctx, cancel := context.WithTimeout(k.ctx, k.brokerTimeout)
		defer cancel()
		qs, err := k.broker.QueueStats(ctx)
		if err == nil {
			stats.Queue = qs
		} else {
			k.logger.Error(&LogMessage{
				Action:        Subscribe,
				Message:       "Failed to get overall queue stats: " + err.Error(),
				MessageStatus: Error,
				CreatedAt:     time.Now(),
			})
		}
	}

	k.mu.RLock()
	defer k.mu.RUnlock()

	for _, topic := range k.topics {
		processed := topic.stats.ProcessedTotal.Load()
		failed := topic.stats.FailedTotal.Load()

		var successRate float64 = 100
		if processed > 0 {
			successRate = float64(processed-failed) / float64(processed) * 100
		}

		snapshot := TopicSnapshot{
			ProcessedTotal: processed,
			FailedTotal:    failed,
			RetryTotal:     topic.stats.RetryTotal.Load(),
			AvgDurationMs:  topic.stats.AvgDuration().Milliseconds(),
			SuccessRate:    fmt.Sprintf("%.2f", successRate),

			// Configuration
			MaxRetries:     topic.maxRetries,
			RetryDelay:     int64(topic.retryDelay.Seconds()),
			ProcessTimeout: int64(topic.processTimeout.Seconds()),
			InternalName:   topic.InternalName,
		}

		// Get per-topic queue stats from broker
		if k.broker != nil {
			ctx, cancel := context.WithTimeout(k.ctx, k.brokerTimeout)
			queueStats, err := k.broker.TopicQueueStats(ctx, topic.InternalName)
			cancel()
			if err == nil {
				snapshot.QueuePending = queueStats.Pending
				snapshot.QueueDelayed = queueStats.Delayed
				snapshot.QueueProcessing = queueStats.Processing
			} else {
				k.logger.Error(&LogMessage{
					TopicName:     topic.Name,
					Action:        Subscribe,
					Message:       "Failed to get queue stats: " + err.Error(),
					MessageStatus: Error,
					CreatedAt:     time.Now(),
				})
			}
		}

		stats.Topics[topic.Name] = snapshot
	}

	return stats
}
