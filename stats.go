package kabaka

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/weichen-lin/kabaka/broker"
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
	Queue      broker.QueueStats        `json:"queue"`
	Topics     map[string]TopicSnapshot `json:"topics"`
}

// TopicSnapshot is a point-in-time copy of a single topic's metrics.
type TopicSnapshot struct {
	ProcessedTotal  int64   `json:"processed_total"`
	FailedTotal     int64   `json:"failed_total"`
	RetryTotal      int64   `json:"retry_total"`
	AvgDurationMs   *int64  `json:"avg_duration"` // Milliseconds, pointer to support nil
	SuccessRate     *string `json:"success_rate"`
	Paused          bool    `json:"paused"`
	QueuePending    int64   `json:"queue_pending"`    // messages waiting in queue
	QueueDelayed    int64   `json:"queue_delayed"`    // messages in delayed queue
	QueueProcessing int64   `json:"queue_processing"` // messages currently being processed

	// Configuration
	MaxRetries     int    `json:"max_retries"`
	RetryDelay     int64  `json:"retry_delay"`     // Seconds
	ProcessTimeout int64  `json:"process_timeout"` // Seconds
	HistoryLimit   int    `json:"history_limit"`
	InternalName   string `json:"internal_name"`
	Schema         string `json:"schema"`
	SchemaType     string `json:"schema_type"`
}

// GetStats returns a point-in-time snapshot of all metrics.
func (k *Kabaka) GetStats() KabakaStats {
	active := k.activejobs.Load()
	stats := KabakaStats{
		ActiveJobs: active,
		IdleSlots:  int64(k.maxWorkers) - active,
		Topics:     make(map[string]TopicSnapshot),
	}

	// Collect broker queue stats (outside lock)
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

	// Collect topic metadata under RLock (fast, no broker calls)
	type topicInfo struct {
		name           string
		internalName   string
		processed      int64
		failed         int64
		retryTotal     int64
		avgDurationMs  *int64
		successRateStr *string
		paused         bool
		maxRetries     int
		retryDelay     int64
		processTimeout int64
		historyLimit   int
		schema         string
		schemaType     string
	}

	k.mu.RLock()
	topics := make([]topicInfo, 0, len(k.topics))
	for _, topic := range k.topics {
		processed := topic.stats.ProcessedTotal.Load()
		failed := topic.stats.FailedTotal.Load()

		var successRate *string
		var avgDuration *int64
		if processed > 0 {
			rate := float64(processed-failed) / float64(processed) * 100
			s := fmt.Sprintf("%.2f", rate)
			successRate = &s
			avg := topic.stats.AvgDuration().Milliseconds()
			avgDuration = &avg
		}

		topics = append(topics, topicInfo{
			name:           topic.Name,
			internalName:   topic.InternalName,
			processed:      processed,
			failed:         failed,
			retryTotal:     topic.stats.RetryTotal.Load(),
			avgDurationMs:  avgDuration,
			successRateStr: successRate,
			paused:         topic.Paused.Load(),
			maxRetries:     topic.maxRetries,
			retryDelay:     int64(topic.retryDelay.Seconds()),
			processTimeout: int64(topic.processTimeout.Seconds()),
			historyLimit:   topic.historyLimit,
			schema:         topic.schema,
			schemaType:     topic.schemaType,
		})
	}
	k.mu.RUnlock()

	// Now make broker calls for per-topic queue stats (outside lock)
	for _, t := range topics {
		snapshot := TopicSnapshot{
			ProcessedTotal: t.processed,
			FailedTotal:    t.failed,
			RetryTotal:     t.retryTotal,
			AvgDurationMs:  t.avgDurationMs,
			SuccessRate:    t.successRateStr,
			Paused:         t.paused,
			MaxRetries:     t.maxRetries,
			RetryDelay:     t.retryDelay,
			ProcessTimeout: t.processTimeout,
			HistoryLimit:   t.historyLimit,
			InternalName:   t.internalName,
			Schema:         t.schema,
			SchemaType:     t.schemaType,
		}

		if k.broker != nil {
			ctx, cancel := context.WithTimeout(k.ctx, k.brokerTimeout)
			queueStats, err := k.broker.TopicQueueStats(ctx, t.internalName)
			cancel()
			if err == nil {
				snapshot.QueuePending = queueStats.Pending
				snapshot.QueueDelayed = queueStats.Delayed
				snapshot.QueueProcessing = queueStats.Processing
			} else {
				k.logger.Error(&LogMessage{
					TopicName:     t.name,
					Action:        Subscribe,
					Message:       "Failed to get queue stats: " + err.Error(),
					MessageStatus: Error,
					CreatedAt:     time.Now(),
				})
			}
		}

		stats.Topics[t.name] = snapshot
	}

	return stats
}
