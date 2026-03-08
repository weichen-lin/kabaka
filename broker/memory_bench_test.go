package broker_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weichen-lin/kabaka"
	"github.com/weichen-lin/kabaka/broker"
)

// BenchmarkMetrics collects detailed performance metrics
type BenchmarkMetrics struct {
	TotalMessages      atomic.Int64
	SuccessfulMessages atomic.Int64
	FailedMessages     atomic.Int64

	// Latency tracking (in nanoseconds)
	latencies   []int64
	latenciesMu sync.Mutex
	startTime   time.Time
	endTime     time.Time
}

func newBenchmarkMetrics() *BenchmarkMetrics {
	return &BenchmarkMetrics{
		latencies: make([]int64, 0, 10000),
		startTime: time.Now(),
	}
}

func (m *BenchmarkMetrics) recordLatency(d time.Duration) {
	m.latenciesMu.Lock()
	m.latencies = append(m.latencies, d.Nanoseconds())
	m.latenciesMu.Unlock()
}

func (m *BenchmarkMetrics) getLatencyPercentile(p float64) time.Duration {
	m.latenciesMu.Lock()
	defer m.latenciesMu.Unlock()

	if len(m.latencies) == 0 {
		return 0
	}

	// Simple sort for percentile calculation
	sorted := make([]int64, len(m.latencies))
	copy(sorted, m.latencies)

	// Bubble sort (good enough for benchmarks)
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[i] > sorted[j] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	idx := int(float64(len(sorted)) * p)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}

	return time.Duration(sorted[idx])
}

func (m *BenchmarkMetrics) finalize() {
	m.endTime = time.Now()
}

func (m *BenchmarkMetrics) report(b *testing.B) {
	total := m.TotalMessages.Load()
	success := m.SuccessfulMessages.Load()
	failed := m.FailedMessages.Load()
	duration := m.endTime.Sub(m.startTime)

	if duration.Seconds() == 0 {
		return
	}

	b.Logf("\n=== Benchmark Metrics ===")
	b.Logf("Total Messages:      %d", total)
	b.Logf("Successful:          %d (%.2f%%)", success, float64(success)/float64(total)*100)
	b.Logf("Failed:              %d (%.2f%%)", failed, float64(failed)/float64(total)*100)
	b.Logf("Duration:            %s", duration)
	b.Logf("Throughput:          %.2f msg/sec", float64(total)/duration.Seconds())
	b.Logf("Latency P50:         %s", m.getLatencyPercentile(0.50))
	b.Logf("Latency P95:         %s", m.getLatencyPercentile(0.95))
	b.Logf("Latency P99:         %s", m.getLatencyPercentile(0.99))
	b.Logf("Latency Max:         %s", m.getLatencyPercentile(1.0))
}

// silentLogger suppresses logs during benchmarks
type silentLogger struct{}

func (l *silentLogger) Debug(msg *kabaka.LogMessage) {}
func (l *silentLogger) Info(msg *kabaka.LogMessage)  {}
func (l *silentLogger) Warn(msg *kabaka.LogMessage)  {}
func (l *silentLogger) Error(msg *kabaka.LogMessage) {}

// setupTestKabaka creates a Kabaka instance for benchmarking
func setupTestKabaka(b *testing.B, workers int, metrics *BenchmarkMetrics) *kabaka.Kabaka {
	b.Helper()

	memBroker := broker.NewMemoryBroker()

	k := kabaka.NewKabaka(
		kabaka.WithBroker(memBroker),
		kabaka.WithMaxWorkers(workers),
		kabaka.WithLogger(&silentLogger{}),
	)

	// Create a test topic with a simple handler
	err := k.CreateTopic("test-topic", func(ctx context.Context, msg *broker.Message) error {
		// Simulate lightweight processing
		time.Sleep(10 * time.Millisecond)

		// Record success
		metrics.SuccessfulMessages.Add(1)

		// Calculate latency (from message creation to processing)
		latency := time.Since(msg.CreatedAt)
		metrics.recordLatency(latency)

		return nil
	}, kabaka.WithProcessTimeout(30*time.Second))

	if err != nil {
		b.Fatalf("Failed to create topic: %v", err)
	}

	return k
}

// setupTestKabakaFast creates a Kabaka instance with fast handler for high-load tests
func setupTestKabakaFast(b *testing.B, workers int, metrics *BenchmarkMetrics) *kabaka.Kabaka {
	b.Helper()

	memBroker := broker.NewMemoryBroker()

	k := kabaka.NewKabaka(
		kabaka.WithBroker(memBroker),
		kabaka.WithMaxWorkers(workers),
		kabaka.WithLogger(&silentLogger{}),
	)

	// Create a test topic with a FAST handler (1ms instead of 10ms)
	err := k.CreateTopic("test-topic", func(ctx context.Context, msg *broker.Message) error {
		// Simulate very lightweight processing for high-load tests
		time.Sleep(1 * time.Millisecond)

		// Record success
		metrics.SuccessfulMessages.Add(1)

		// Calculate latency (from message creation to processing)
		latency := time.Since(msg.CreatedAt)
		metrics.recordLatency(latency)

		return nil
	}, kabaka.WithProcessTimeout(30*time.Second))

	if err != nil {
		b.Fatalf("Failed to create topic: %v", err)
	}

	return k
}

// waitForCompletion waits for all messages to be processed
func waitForCompletion(b *testing.B, k *kabaka.Kabaka, expectedCount int) {
	b.Helper()

	// Dynamic timeout based on expected messages
	// Minimum 2 minutes, add 1 second per 100 messages
	timeoutDuration := 2 * time.Minute
	if expectedCount > 1000 {
		timeoutDuration = time.Duration(expectedCount/100) * time.Second
		if timeoutDuration < 2*time.Minute {
			timeoutDuration = 2 * time.Minute
		}
		if timeoutDuration > 10*time.Minute {
			timeoutDuration = 10 * time.Minute
		}
	}

	timeout := time.After(timeoutDuration)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	lastLogTime := time.Now()
	iterCount := 0

	for {
		select {
		case <-timeout:
			stats := k.GetStats()
			b.Logf("❌ Timeout after %v waiting for completion", timeoutDuration)
			b.Logf("Expected: %d messages", expectedCount)
			b.Logf("Queue stats: Pending=%d, Delayed=%d, Processing=%d, ActiveJobs=%d",
				stats.Queue.Pending, stats.Queue.Delayed, stats.Queue.Processing, stats.ActiveJobs)
			b.Fatalf("Timeout waiting for completion. Queue stats: %+v", stats.Queue)
			return
		case <-ticker.C:
			iterCount++
			stats := k.GetStats()

			// Log progress every 5 seconds
			if time.Since(lastLogTime) > 5*time.Second {
				b.Logf("⏳ Progress: Pending=%d, Delayed=%d, Processing=%d, Active=%d",
					stats.Queue.Pending, stats.Queue.Delayed, stats.Queue.Processing, stats.ActiveJobs)
				lastLogTime = time.Now()
			}

			// Check if queue is empty and all jobs are done
			if stats.Queue.Pending == 0 &&
				stats.Queue.Processing == 0 &&
				stats.ActiveJobs == 0 {
				// Double check with delayed queue
				if stats.Queue.Delayed > 0 {
					continue // Still have delayed messages
				}
				return
			}

			// Detect stuck state: if nothing changes for 30 seconds, might be deadlock
			if iterCount%600 == 0 { // 600 * 50ms = 30 seconds
				b.Logf("⚠️  Still waiting after %d seconds...", iterCount*50/1000)
			}
		}
	}
}

// BenchmarkPublish_MemoryBroker benchmarks basic publish throughput
func BenchmarkPublish_MemoryBroker(b *testing.B) {
	metrics := newBenchmarkMetrics()
	k := setupTestKabaka(b, 50, metrics)
	defer k.Close()

	k.Start()

	// Give the dispatcher time to start
	time.Sleep(100 * time.Millisecond)

	b.ResetTimer()
	metrics.startTime = time.Now()

	for i := 0; i < b.N; i++ {
		err := k.Publish("test-topic", []byte("benchmark message"))
		if err != nil {
			b.Fatalf("Failed to publish: %v", err)
		}
		metrics.TotalMessages.Add(1)
	}

	// Wait for all messages to be processed
	waitForCompletion(b, k, b.N)

	b.StopTimer()
	metrics.finalize()
	metrics.report(b)
}

// BenchmarkWorkerPool_10Workers tests performance with 10 workers
func BenchmarkWorkerPool_10Workers(b *testing.B) {
	benchmarkWorkerPool(b, 10)
}

// BenchmarkWorkerPool_50Workers tests performance with 50 workers
func BenchmarkWorkerPool_50Workers(b *testing.B) {
	benchmarkWorkerPool(b, 50)
}

// BenchmarkWorkerPool_100Workers tests performance with 100 workers
func BenchmarkWorkerPool_100Workers(b *testing.B) {
	benchmarkWorkerPool(b, 100)
}

// BenchmarkWorkerPool_200Workers tests performance with 200 workers
func BenchmarkWorkerPool_200Workers(b *testing.B) {
	benchmarkWorkerPool(b, 200)
}

// benchmarkWorkerPool is the common implementation for worker pool tests
func benchmarkWorkerPool(b *testing.B, workers int) {
	metrics := newBenchmarkMetrics()
	k := setupTestKabaka(b, workers, metrics)
	defer k.Close()

	k.Start()
	time.Sleep(100 * time.Millisecond)

	// Publish a fixed number of messages to compare worker efficiency
	messageCount := 1000
	if messageCount > b.N {
		messageCount = b.N
	}

	b.ResetTimer()
	metrics.startTime = time.Now()

	for i := 0; i < messageCount; i++ {
		err := k.Publish("test-topic", []byte("benchmark message"))
		if err != nil {
			b.Fatalf("Failed to publish: %v", err)
		}
		metrics.TotalMessages.Add(1)
	}

	waitForCompletion(b, k, messageCount)

	b.StopTimer()
	metrics.finalize()
	metrics.report(b)
}

// BenchmarkConcurrentPublish_10Goroutines tests concurrent publishing with 10 goroutines
func BenchmarkConcurrentPublish_10Goroutines(b *testing.B) {
	benchmarkConcurrentPublish(b, 10)
}

// BenchmarkConcurrentPublish_100Goroutines tests concurrent publishing with 100 goroutines
func BenchmarkConcurrentPublish_100Goroutines(b *testing.B) {
	benchmarkConcurrentPublish(b, 100)
}

// BenchmarkConcurrentPublish_1000Goroutines tests concurrent publishing with 1000 goroutines
func BenchmarkConcurrentPublish_1000Goroutines(b *testing.B) {
	benchmarkConcurrentPublish(b, 1000)
}

// benchmarkConcurrentPublish is the common implementation for concurrent publish tests
func benchmarkConcurrentPublish(b *testing.B, goroutines int) {
	metrics := newBenchmarkMetrics()

	// Scale workers based on goroutines to avoid bottleneck
	// For 1000 goroutines, we need at least 500-1000 workers
	workers := 100
	useFastHandler := false

	if goroutines >= 1000 {
		workers = 1000
		useFastHandler = true // Use 1ms handler instead of 10ms
		b.Logf("⚠️  High concurrency test: using %d workers, fast handler (1ms)", workers)
	} else if goroutines >= 100 {
		workers = 200
	}

	var k *kabaka.Kabaka
	if useFastHandler {
		k = setupTestKabakaFast(b, workers, metrics)
	} else {
		k = setupTestKabaka(b, workers, metrics)
	}
	defer k.Close()

	k.Start()
	time.Sleep(100 * time.Millisecond)

	// Reduce messages per goroutine for high concurrency tests to avoid timeout
	messagesPerGoroutine := 100
	if goroutines >= 1000 {
		messagesPerGoroutine = 5 // 1000 * 5 = 5,000 messages (more realistic)
		b.Logf("⚠️  High concurrency: %d goroutines × %d messages = %d total",
			goroutines, messagesPerGoroutine, goroutines*messagesPerGoroutine)
	} else if goroutines >= 100 {
		messagesPerGoroutine = 50 // 100 * 50 = 5,000 messages
	}

	totalMessages := goroutines * messagesPerGoroutine

	b.ResetTimer()
	metrics.startTime = time.Now()

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()

			for i := 0; i < messagesPerGoroutine; i++ {
				err := k.Publish("test-topic", []byte("concurrent message"))
				if err != nil {
					b.Errorf("Goroutine %d failed to publish: %v", id, err)
					return
				}
				metrics.TotalMessages.Add(1)
			}
		}(g)
	}

	wg.Wait()
	waitForCompletion(b, k, totalMessages)

	b.StopTimer()
	metrics.finalize()
	metrics.report(b)
}

// BenchmarkMultiTopic_5Topics tests performance with 5 topics
func BenchmarkMultiTopic_5Topics(b *testing.B) {
	benchmarkMultiTopic(b, 5)
}

// BenchmarkMultiTopic_20Topics tests performance with 20 topics
func BenchmarkMultiTopic_20Topics(b *testing.B) {
	benchmarkMultiTopic(b, 20)
}

// BenchmarkMultiTopic_100Topics tests performance with 100 topics
func BenchmarkMultiTopic_100Topics(b *testing.B) {
	benchmarkMultiTopic(b, 100)
}

// benchmarkMultiTopic is the common implementation for multi-topic tests
func benchmarkMultiTopic(b *testing.B, topicCount int) {
	metrics := newBenchmarkMetrics()

	// Scale workers for large topic counts
	workers := 100
	if topicCount >= 100 {
		workers = 200
		b.Logf("Increased workers to %d for %d topics", workers, topicCount)
	}

	memBroker := broker.NewMemoryBroker()
	k := kabaka.NewKabaka(
		kabaka.WithBroker(memBroker),
		kabaka.WithMaxWorkers(workers),
		kabaka.WithLogger(&silentLogger{}),
	)
	defer k.Close()

	// Create multiple topics
	topicNames := make([]string, topicCount)
	for i := 0; i < topicCount; i++ {
		topicName := generateTopicName(i)
		topicNames[i] = topicName
		err := k.CreateTopic(topicName, func(ctx context.Context, msg *broker.Message) error {
			time.Sleep(10 * time.Millisecond)
			metrics.SuccessfulMessages.Add(1)
			latency := time.Since(msg.CreatedAt)
			metrics.recordLatency(latency)
			return nil
		})
		if err != nil {
			b.Fatalf("Failed to create topic %s: %v", topicName, err)
		}
	}

	k.Start()
	time.Sleep(100 * time.Millisecond)

	// Reduce messages for large topic counts to avoid timeout
	messagesPerTopic := 100
	if topicCount >= 100 {
		messagesPerTopic = 50 // 100 topics * 50 = 5000 messages
		b.Logf("Reduced messages per topic to %d for %d topics", messagesPerTopic, topicCount)
	}

	totalMessages := topicCount * messagesPerTopic

	b.ResetTimer()
	metrics.startTime = time.Now()

	// Distribute messages across topics
	for i := 0; i < messagesPerTopic; i++ {
		for t := 0; t < topicCount; t++ {
			err := k.Publish(topicNames[t], []byte("multi-topic message"))
			if err != nil {
				b.Fatalf("Failed to publish to topic %s: %v", topicNames[t], err)
			}
			metrics.TotalMessages.Add(1)
		}
	}

	waitForCompletion(b, k, totalMessages)

	b.StopTimer()
	metrics.finalize()
	metrics.report(b)
}

// generateTopicName creates a unique topic name
func generateTopicName(index int) string {
	// Generate names like "topic-0", "topic-1", etc.
	return "topic-" + string(rune('0'+index%10)) + string(rune('0'+index/10))
}

// ============================================================================
// Phase 3: Handler Complexity Tests
// ============================================================================

// setupTestKabakaWithHandler creates a Kabaka instance with custom handler
func setupTestKabakaWithHandler(b *testing.B, workers int, handler kabaka.HandleFunc, metrics *BenchmarkMetrics) *kabaka.Kabaka {
	b.Helper()

	memBroker := broker.NewMemoryBroker()

	k := kabaka.NewKabaka(
		kabaka.WithBroker(memBroker),
		kabaka.WithMaxWorkers(workers),
		kabaka.WithLogger(&silentLogger{}),
	)

	// Create a test topic with custom handler
	err := k.CreateTopic("test-topic", handler, kabaka.WithProcessTimeout(5*time.Minute))
	if err != nil {
		b.Fatalf("Failed to create topic: %v", err)
	}

	return k
}

// BenchmarkHandlerLatency_Fast tests with 1ms handler
func BenchmarkHandlerLatency_Fast(b *testing.B) {
	benchmarkHandlerLatency(b, 1*time.Millisecond, "Fast")
}

// BenchmarkHandlerLatency_Medium tests with 50ms handler
func BenchmarkHandlerLatency_Medium(b *testing.B) {
	benchmarkHandlerLatency(b, 50*time.Millisecond, "Medium")
}

// BenchmarkHandlerLatency_Slow tests with 200ms handler
func BenchmarkHandlerLatency_Slow(b *testing.B) {
	benchmarkHandlerLatency(b, 200*time.Millisecond, "Slow")
}

// benchmarkHandlerLatency is the common implementation for handler latency tests
func benchmarkHandlerLatency(b *testing.B, processingTime time.Duration, label string) {
	metrics := newBenchmarkMetrics()

	handler := func(ctx context.Context, msg *broker.Message) error {
		// Simulate processing
		time.Sleep(processingTime)

		metrics.SuccessfulMessages.Add(1)
		latency := time.Since(msg.CreatedAt)
		metrics.recordLatency(latency)

		return nil
	}

	k := setupTestKabakaWithHandler(b, 100, handler, metrics)
	defer k.Close()

	k.Start()
	time.Sleep(100 * time.Millisecond)

	messageCount := 500
	if messageCount > b.N {
		messageCount = b.N
	}

	b.Logf("Testing with %s handler (%v processing time)", label, processingTime)

	b.ResetTimer()
	metrics.startTime = time.Now()

	for i := 0; i < messageCount; i++ {
		err := k.Publish("test-topic", []byte("latency test message"))
		if err != nil {
			b.Fatalf("Failed to publish: %v", err)
		}
		metrics.TotalMessages.Add(1)
	}

	waitForCompletion(b, k, messageCount)

	b.StopTimer()
	metrics.finalize()
	metrics.report(b)
}

// BenchmarkHandlerLatency_Mixed tests with mixed processing times
func BenchmarkHandlerLatency_Mixed(b *testing.B) {
	metrics := newBenchmarkMetrics()
	var counter atomic.Int64

	handler := func(ctx context.Context, msg *broker.Message) error {
		// Alternate between fast, medium, and slow
		count := counter.Add(1)
		var processingTime time.Duration

		switch count % 3 {
		case 0:
			processingTime = 1 * time.Millisecond // Fast
		case 1:
			processingTime = 50 * time.Millisecond // Medium
		case 2:
			processingTime = 200 * time.Millisecond // Slow
		}

		time.Sleep(processingTime)

		metrics.SuccessfulMessages.Add(1)
		latency := time.Since(msg.CreatedAt)
		metrics.recordLatency(latency)

		return nil
	}

	k := setupTestKabakaWithHandler(b, 100, handler, metrics)
	defer k.Close()

	k.Start()
	time.Sleep(100 * time.Millisecond)

	messageCount := 300 // 100 of each type
	if messageCount > b.N {
		messageCount = b.N
	}

	b.ResetTimer()
	metrics.startTime = time.Now()

	for i := 0; i < messageCount; i++ {
		err := k.Publish("test-topic", []byte("mixed latency test"))
		if err != nil {
			b.Fatalf("Failed to publish: %v", err)
		}
		metrics.TotalMessages.Add(1)
	}

	waitForCompletion(b, k, messageCount)

	b.StopTimer()
	metrics.finalize()
	metrics.report(b)
}

// BenchmarkHandler_DatabaseAccess simulates database queries
func BenchmarkHandler_DatabaseAccess(b *testing.B) {
	metrics := newBenchmarkMetrics()

	handler := func(ctx context.Context, msg *broker.Message) error {
		// Simulate database query (5-15ms)
		simulateDBQuery := time.Duration(5+len(msg.Value)%10) * time.Millisecond
		time.Sleep(simulateDBQuery)

		metrics.SuccessfulMessages.Add(1)
		latency := time.Since(msg.CreatedAt)
		metrics.recordLatency(latency)

		return nil
	}

	k := setupTestKabakaWithHandler(b, 100, handler, metrics)
	defer k.Close()

	k.Start()
	time.Sleep(100 * time.Millisecond)

	messageCount := 500
	if messageCount > b.N {
		messageCount = b.N
	}

	b.ResetTimer()
	metrics.startTime = time.Now()

	for i := 0; i < messageCount; i++ {
		payload := []byte("db-query-" + string(rune('0'+(i%10))))
		err := k.Publish("test-topic", payload)
		if err != nil {
			b.Fatalf("Failed to publish: %v", err)
		}
		metrics.TotalMessages.Add(1)
	}

	waitForCompletion(b, k, messageCount)

	b.StopTimer()
	metrics.finalize()
	metrics.report(b)
}

// BenchmarkHandler_HTTPRequest simulates HTTP API calls
func BenchmarkHandler_HTTPRequest(b *testing.B) {
	metrics := newBenchmarkMetrics()

	handler := func(ctx context.Context, msg *broker.Message) error {
		// Simulate HTTP request (20-100ms)
		simulateHTTPCall := time.Duration(20+len(msg.Value)%80) * time.Millisecond
		time.Sleep(simulateHTTPCall)

		metrics.SuccessfulMessages.Add(1)
		latency := time.Since(msg.CreatedAt)
		metrics.recordLatency(latency)

		return nil
	}

	k := setupTestKabakaWithHandler(b, 150, handler, metrics)
	defer k.Close()

	k.Start()
	time.Sleep(100 * time.Millisecond)

	messageCount := 300
	if messageCount > b.N {
		messageCount = b.N
	}

	b.ResetTimer()
	metrics.startTime = time.Now()

	for i := 0; i < messageCount; i++ {
		payload := []byte("http-request-" + string(rune('0'+(i%10))))
		err := k.Publish("test-topic", payload)
		if err != nil {
			b.Fatalf("Failed to publish: %v", err)
		}
		metrics.TotalMessages.Add(1)
	}

	waitForCompletion(b, k, messageCount)

	b.StopTimer()
	metrics.finalize()
	metrics.report(b)
}

// ============================================================================
// Phase 4: Delayed Messages Tests
// ============================================================================

// BenchmarkDelayedMessages_ShortDelay tests with 100ms delay
func BenchmarkDelayedMessages_ShortDelay(b *testing.B) {
	benchmarkDelayedMessages(b, 100*time.Millisecond, 200)
}

// BenchmarkDelayedMessages_MediumDelay tests with 1s delay
func BenchmarkDelayedMessages_MediumDelay(b *testing.B) {
	benchmarkDelayedMessages(b, 1*time.Second, 100)
}

// BenchmarkDelayedMessages_LongDelay tests with 5s delay
func BenchmarkDelayedMessages_LongDelay(b *testing.B) {
	benchmarkDelayedMessages(b, 5*time.Second, 50)
}

// benchmarkDelayedMessages is the common implementation for delayed message tests
func benchmarkDelayedMessages(b *testing.B, delay time.Duration, messageCount int) {
	metrics := newBenchmarkMetrics()

	handler := func(ctx context.Context, msg *broker.Message) error {
		time.Sleep(10 * time.Millisecond)
		metrics.SuccessfulMessages.Add(1)
		latency := time.Since(msg.CreatedAt)
		metrics.recordLatency(latency)
		return nil
	}

	k := setupTestKabakaWithHandler(b, 50, handler, metrics)
	defer k.Close()

	k.Start()
	time.Sleep(100 * time.Millisecond)

	b.Logf("Testing delayed messages with %v delay, %d messages", delay, messageCount)

	b.ResetTimer()
	metrics.startTime = time.Now()

	for i := 0; i < messageCount; i++ {
		err := k.PublishDelayed("test-topic", []byte("delayed message"), delay)
		if err != nil {
			b.Fatalf("Failed to publish delayed message: %v", err)
		}
		metrics.TotalMessages.Add(1)
	}

	// Wait for delay + processing time
	waitTime := delay + time.Duration(messageCount/50+1)*time.Second
	time.Sleep(waitTime)

	waitForCompletion(b, k, messageCount)

	b.StopTimer()
	metrics.finalize()
	metrics.report(b)
}

// BenchmarkMixedDelayed_50Percent tests 50% immediate, 50% delayed
func BenchmarkMixedDelayed_50Percent(b *testing.B) {
	metrics := newBenchmarkMetrics()

	handler := func(ctx context.Context, msg *broker.Message) error {
		time.Sleep(10 * time.Millisecond)
		metrics.SuccessfulMessages.Add(1)
		latency := time.Since(msg.CreatedAt)
		metrics.recordLatency(latency)
		return nil
	}

	k := setupTestKabakaWithHandler(b, 100, handler, metrics)
	defer k.Close()

	k.Start()
	time.Sleep(100 * time.Millisecond)

	messageCount := 200
	delay := 500 * time.Millisecond

	b.ResetTimer()
	metrics.startTime = time.Now()

	for i := 0; i < messageCount; i++ {
		if i%2 == 0 {
			// Immediate
			err := k.Publish("test-topic", []byte("immediate message"))
			if err != nil {
				b.Fatalf("Failed to publish: %v", err)
			}
		} else {
			// Delayed
			err := k.PublishDelayed("test-topic", []byte("delayed message"), delay)
			if err != nil {
				b.Fatalf("Failed to publish delayed: %v", err)
			}
		}
		metrics.TotalMessages.Add(1)
	}

	// Wait for delayed messages to be processed
	time.Sleep(delay + 3*time.Second)
	waitForCompletion(b, k, messageCount)

	b.StopTimer()
	metrics.finalize()
	metrics.report(b)
}

// ============================================================================
// Phase 5: Error Handling and Retry Tests
// ============================================================================

// BenchmarkRetry_30PercentFailure tests 30% failure rate
func BenchmarkRetry_30PercentFailure(b *testing.B) {
	benchmarkRetry(b, 30, 200)
}

// BenchmarkRetry_50PercentFailure tests 50% failure rate
func BenchmarkRetry_50PercentFailure(b *testing.B) {
	benchmarkRetry(b, 50, 200)
}

// BenchmarkRetry_70PercentFailure tests 70% failure rate
func BenchmarkRetry_70PercentFailure(b *testing.B) {
	benchmarkRetry(b, 70, 200)
}

// benchmarkRetry is the common implementation for retry tests
func benchmarkRetry(b *testing.B, failurePercent int, messageCount int) {
	metrics := newBenchmarkMetrics()
	var counter atomic.Int64

	handler := func(ctx context.Context, msg *broker.Message) error {
		time.Sleep(10 * time.Millisecond)

		// Simulate failure based on percentage
		count := counter.Add(1)
		if int(count)%100 < failurePercent && msg.Retry > 0 {
			metrics.FailedMessages.Add(1)
			return kabaka.ErrTopicNotFound // Any error to trigger retry
		}

		metrics.SuccessfulMessages.Add(1)
		latency := time.Since(msg.CreatedAt)
		metrics.recordLatency(latency)
		return nil
	}

	memBroker := broker.NewMemoryBroker()
	k := kabaka.NewKabaka(
		kabaka.WithBroker(memBroker),
		kabaka.WithMaxWorkers(100),
		kabaka.WithLogger(&silentLogger{}),
	)
	defer k.Close()

	// Create topic with retry settings
	err := k.CreateTopic("test-topic", handler,
		kabaka.WithMaxRetries(3),
		kabaka.WithRetryDelay(100*time.Millisecond),
		kabaka.WithProcessTimeout(5*time.Minute))
	if err != nil {
		b.Fatalf("Failed to create topic: %v", err)
	}

	k.Start()
	time.Sleep(100 * time.Millisecond)

	b.Logf("Testing with %d%% failure rate, %d messages", failurePercent, messageCount)

	b.ResetTimer()
	metrics.startTime = time.Now()

	for i := 0; i < messageCount; i++ {
		err := k.Publish("test-topic", []byte("retry test message"))
		if err != nil {
			b.Fatalf("Failed to publish: %v", err)
		}
		metrics.TotalMessages.Add(1)
	}

	// Extra time for retries
	time.Sleep(5 * time.Second)
	waitForCompletion(b, k, messageCount)

	b.StopTimer()
	metrics.finalize()
	metrics.report(b)
}
