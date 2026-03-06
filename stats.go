package kabaka

import (
	"context"
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
	ActiveJobs int64
	IdleSlots  int64
	Queue      QueueStats
	Topics     map[string]TopicSnapshot
}

// TopicSnapshot is a point-in-time copy of a single topic's metrics.
type TopicSnapshot struct {
	ProcessedTotal  int64
	FailedTotal     int64
	RetryTotal      int64
	AvgDuration     time.Duration
	QueuePending    int64 // messages waiting in queue
	QueueDelayed    int64 // messages in delayed queue
	QueueProcessing int64 // messages currently being processed
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
		snapshot := TopicSnapshot{
			ProcessedTotal: topic.stats.ProcessedTotal.Load(),
			FailedTotal:    topic.stats.FailedTotal.Load(),
			RetryTotal:     topic.stats.RetryTotal.Load(),
			AvgDuration:    topic.stats.AvgDuration(),
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
