package kabaka

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisBroker_DistributedCompetition(t *testing.T) {
	// 1. Setup Redis Client
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	ctx := context.Background()

	// Check if redis is available, skip if not
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skip("Redis not available, skipping integration test")
	}

	topicName := "distributed-test-" + NewUUID()[:8]
	// Cleanup after test
	defer func() {
		client.Del(ctx, "kabaka:"+topicName, "kabaka:"+topicName+":processing")
		client.Close()
	}()

	const (
		numInstances = 3   // Simulate 3 independent Kabaka services
		numMessages  = 50  // Total messages to process
	)

	var processedCount int32
	var mu sync.Mutex
	processedMessages := make(map[string]bool)

	// 2. Create multiple instances
	instances := make([]*Kabaka, numInstances)
	for i := 0; i < numInstances; i++ {
		// Each instance gets its own broker wrapper but shares the same Redis client
		broker := NewRedisBrokerWithClient(client)
		k := NewKabaka(WithBroker(broker))
		
		err := k.CreateTopic(topicName, func(ctx context.Context, msg *Message) error {
			// Simulate some work
			time.Sleep(10 * time.Millisecond)
			
			mu.Lock()
			if processedMessages[msg.ID] {
				t.Errorf("Duplicate message processing detected: %s", msg.ID)
			}
			processedMessages[msg.ID] = true
			mu.Unlock()
			
			atomic.AddInt32(&processedCount, 1)
			return nil
		}, WithMaxWorkers(5))
		
		if err != nil {
			t.Fatalf("Failed to create topic for instance %d: %v", i, err)
		}
		instances[i] = k
	}

	// 3. Publish messages using a separate publisher instance
	publisherBroker := NewRedisBrokerWithClient(client)
	// We need to register the topic in the publisher to use Publish, 
	// or we could use the internal broker directly. 
	// Let's use the broker directly to simplify.
	for i := 0; i < numMessages; i++ {
		msg := &Message{
			ID:        NewUUID(),
			Value:     []byte(fmt.Sprintf("payload-%d", i)),
			CreatedAt: time.Now(),
		}
		err := publisherBroker.Push(ctx, topicName, msg)
		if err != nil {
			t.Errorf("Failed to push message %d: %v", i, err)
		}
	}

	// 4. Wait for all messages to be processed
	timeout := 15 * time.Second
	start := time.Now()
	for atomic.LoadInt32(&processedCount) < numMessages {
		if time.Since(start) > timeout {
			t.Errorf("Timed out waiting for messages. Got %d/%d", processedCount, numMessages)
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// 5. Assertions
	finalCount := atomic.LoadInt32(&processedCount)
	t.Logf("Total processed: %d", finalCount)
	
	if finalCount != numMessages {
		t.Errorf("Expected %d processed messages, got %d", numMessages, finalCount)
	}

	// Verify processing queue is empty (all Acked)
	processingLen, _ := client.LLen(ctx, "kabaka:"+topicName+":processing").Result()
	if processingLen != 0 {
		t.Errorf("Processing queue not empty: %d messages remaining", processingLen)
	}

	// 6. Cleanup
	for _, k := range instances {
		k.Close()
	}
}
