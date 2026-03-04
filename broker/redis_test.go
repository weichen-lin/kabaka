package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/weichen-lin/kabaka"
)

const (
	testRedisAddr = "localhost:6379"
	testPrefix    = "kabaka-test:"
)

func skipIfRedisUnavailable(t *testing.T) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr: testRedisAddr,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := client.Ping(ctx).Err()
	if err != nil {
		t.Skip("Redis not available, skipping test")
	}

	return client
}

func cleanupRedis(t *testing.T, client *redis.Client) {
	ctx := context.Background()
	pattern := testPrefix + "*"
	keys, err := client.Keys(ctx, pattern).Result()
	if err != nil {
		t.Logf("Warning: failed to scan keys: %v", err)
		return
	}

	if len(keys) > 0 {
		err = client.Del(ctx, keys...).Err()
		if err != nil {
			t.Logf("Warning: failed to delete keys: %v", err)
		}
	}
}

func TestNewRedisBroker(t *testing.T) {
	broker := NewRedisBroker(testRedisAddr, "", 0, RedisBrokerOptions{Prefix: testPrefix})
	if broker == nil {
		t.Fatal("NewRedisBroker returned nil")
	}
	defer broker.Close()

	if broker.prefix != testPrefix {
		t.Errorf("Expected prefix %s, got %s", testPrefix, broker.prefix)
	}
}

func TestNewRedisBrokerWithClient(t *testing.T) {
	client := skipIfRedisUnavailable(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := NewRedisBrokerWithClient(client, RedisBrokerOptions{Prefix: testPrefix})
	if broker == nil {
		t.Fatal("NewRedisBrokerWithClient returned nil")
	}
	defer broker.Close()

	if broker.client != client {
		t.Error("Broker client doesn't match provided client")
	}
}

func TestRedisBroker_Push(t *testing.T) {
	client := skipIfRedisUnavailable(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := NewRedisBrokerWithClient(client, RedisBrokerOptions{Prefix: testPrefix})
	defer broker.Close()

	ctx := context.Background()
	topic := "test-push"
	msg := &kabaka.Message{
		Id:    "msg-1",
		Value: []byte("test message"),
	}

	err := broker.Push(ctx, topic, msg)
	if err != nil {
		t.Fatalf("Push failed: %v", err)
	}

	// Verify ID is in list
	key := testPrefix + topic
	id, err := client.LIndex(ctx, key, 0).Result()
	if err != nil {
		t.Fatalf("Failed to get message ID from list: %v", err)
	}

	if id != msg.Id {
		t.Errorf("Expected ID %s in list, got %s", msg.Id, id)
	}

	// Verify message is in hash warehouse
	msgKey := key + ":messages"
	exists, _ := client.HExists(ctx, msgKey, msg.Id).Result()
	if !exists {
		t.Errorf("Message ID %s not found in hash warehouse", msg.Id)
	}
}

func TestRedisBroker_PushDelayed(t *testing.T) {
	client := skipIfRedisUnavailable(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := NewRedisBrokerWithClient(client, RedisBrokerOptions{Prefix: testPrefix})
	defer broker.Close()

	ctx := context.Background()
	topic := "test-delayed"
	msg := &kabaka.Message{
		Id:    "msg-delayed-1",
		Value: []byte("delayed message"),
	}

	delay := 100 * time.Millisecond
	err := broker.PushDelayed(ctx, topic, msg, delay)
	if err != nil {
		t.Fatalf("PushDelayed failed: %v", err)
	}

	// Verify ID is in delayed queue (sorted set)
	key := testPrefix + topic + ":delayed"
	ids, err := client.ZRange(ctx, key, 0, 0).Result()
	if err != nil {
		t.Fatalf("Failed to get message ID from delayed queue: %v", err)
	}

	if len(ids) == 0 || ids[0] != msg.Id {
		t.Errorf("Expected ID %s in delayed queue, got %v", msg.Id, ids)
	}

	// Verify message is in hash warehouse
	msgKey := testPrefix + topic + ":messages"
	exists, _ := client.HExists(ctx, msgKey, msg.Id).Result()
	if !exists {
		t.Errorf("Message ID %s not found in hash warehouse", msg.Id)
	}
}

func TestRedisBroker_Register(t *testing.T) {
	client := skipIfRedisUnavailable(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := NewRedisBrokerWithClient(client, RedisBrokerOptions{Prefix: testPrefix})
	defer broker.Close()

	ctx := context.Background()
	topic := "test-register"

	err := broker.Register(ctx, topic)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	broker.mu.Lock()
	_, exists := broker.pollers[topic]
	topicInList := false
	for _, t := range broker.allTopics {
		if t == topic {
			topicInList = true
			break
		}
	}
	broker.mu.Unlock()

	if !exists {
		t.Error("Topic should be in pollers map")
	}
	if !topicInList {
		t.Error("Topic should be in allTopics list")
	}
}

func TestRedisBroker_Unregister(t *testing.T) {
	client := skipIfRedisUnavailable(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := NewRedisBrokerWithClient(client, RedisBrokerOptions{Prefix: testPrefix})
	defer broker.Close()

	ctx := context.Background()
	topic := "test-unregister"

	broker.Register(ctx, topic)
	msg := &kabaka.Message{Id: "msg-1", Value: []byte("test")}
	broker.Push(ctx, topic, msg)

	err := broker.Unregister(ctx, topic)
	if err != nil {
		t.Fatalf("Unregister failed: %v", err)
	}

	broker.mu.Lock()
	_, exists := broker.pollers[topic]
	broker.mu.Unlock()

	if exists {
		t.Error("Topic should not be in pollers map")
	}

	// Verify Redis data is NOT cleaned up
	key := testPrefix + topic
	length, _ := client.LLen(ctx, key).Result()
	if length != 1 {
		t.Errorf("Expected 1 message ID in Redis after Unregister, got %d", length)
	}
}

func TestRedisBroker_UnregisterAndCleanup(t *testing.T) {
	client := skipIfRedisUnavailable(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := NewRedisBrokerWithClient(client, RedisBrokerOptions{Prefix: testPrefix})
	defer broker.Close()

	ctx := context.Background()
	topic := "test-unregister-cleanup"

	broker.Register(ctx, topic)
	msg := &kabaka.Message{Id: "msg-1", Value: []byte("test")}
	broker.Push(ctx, topic, msg)

	err := broker.UnregisterAndCleanup(ctx, topic)
	if err != nil {
		t.Fatalf("UnregisterAndCleanup failed: %v", err)
	}

	mainKey := testPrefix + topic
	msgKey := testPrefix + topic + ":messages"

	mainLen, _ := client.LLen(ctx, mainKey).Result()
	msgExists, _ := client.Exists(ctx, msgKey).Result()

	if mainLen != 0 {
		t.Errorf("Main queue should be empty, got length %d", mainLen)
	}
	if msgExists != 0 {
		t.Error("Message warehouse should be deleted")
	}
}

func TestRedisBroker_Watch(t *testing.T) {
	client := skipIfRedisUnavailable(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := NewRedisBrokerWithClient(client, RedisBrokerOptions{Prefix: testPrefix})
	defer broker.Close()

	ctx := context.Background()
	topic := "test-watch"

	broker.Register(ctx, topic)
	watchCh, _ := broker.Watch(ctx, topic)

	msg := &kabaka.Message{Id: "msg-watch", Value: []byte("watch test")}
	broker.Push(ctx, topic, msg)

	select {
	case task := <-watchCh:
		if task.Topic != topic {
			t.Errorf("Expected topic %s, got %s", topic, task.Topic)
		}
		if string(task.Message.Value) != "watch test" {
			t.Errorf("Expected 'watch test', got %s", string(task.Message.Value))
		}
	case <-time.After(5 * time.Second):
		t.Error("Timeout waiting for message")
	}
}

func TestRedisBroker_Finish(t *testing.T) {
	client := skipIfRedisUnavailable(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := NewRedisBrokerWithClient(client, RedisBrokerOptions{Prefix: testPrefix})
	defer broker.Close()

	ctx := context.Background()
	topic := "test-finish"
	msg := &kabaka.Message{Id: "msg-finish", Value: []byte("finish test")}

	// Simulate processing state
	processingKey := testPrefix + topic + ":processing"
	msgKey := testPrefix + topic + ":messages"
	data, _ := json.Marshal(msg)
	client.HSet(ctx, msgKey, msg.Id, data)
	client.RPush(ctx, processingKey, msg.Id)

	err := broker.Finish(ctx, topic, msg, nil, 0)
	if err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	length, _ := client.LLen(ctx, processingKey).Result()
	if length != 0 {
		t.Errorf("Expected 0 in processing queue, got %d", length)
	}

	exists, _ := client.HExists(ctx, msgKey, msg.Id).Result()
	if exists {
		t.Error("Message should be removed from warehouse")
	}
}

func TestRedisBroker_Len(t *testing.T) {
	client := skipIfRedisUnavailable(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := NewRedisBrokerWithClient(client, RedisBrokerOptions{Prefix: testPrefix})
	defer broker.Close()

	ctx := context.Background()
	topic := "test-len"
	msg := &kabaka.Message{Id: "msg-1", Value: []byte("test")}
	broker.Push(ctx, topic, msg)

	length, _ := broker.Len(ctx, topic)
	if length != 1 {
		t.Errorf("Expected length 1, got %d", length)
	}
}

func TestRedisBroker_ConcurrentPush(t *testing.T) {
	client := skipIfRedisUnavailable(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := NewRedisBrokerWithClient(client, RedisBrokerOptions{Prefix: testPrefix})
	defer broker.Close()

	ctx := context.Background()
	topic := "test-concurrent"
	goroutines := 10
	messagesPerGoroutine := 50
	var pushedCount int32

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for j := 0; j < messagesPerGoroutine; j++ {
				msg := &kabaka.Message{
					Id:    fmt.Sprintf("msg-%d-%d", g, j),
					Value: []byte("test"),
				}
				if err := broker.Push(ctx, topic, msg); err == nil {
					atomic.AddInt32(&pushedCount, 1)
				}
			}
		}(i)
	}
	wg.Wait()

	length, _ := broker.Len(ctx, topic)
	if length != int64(goroutines*messagesPerGoroutine) {
		t.Errorf("Expected %d, got %d", goroutines*messagesPerGoroutine, length)
	}
}

func TestRedisBroker_Close(t *testing.T) {
	client := skipIfRedisUnavailable(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := NewRedisBrokerWithClient(client, RedisBrokerOptions{Prefix: testPrefix})
	broker.Close()

	select {
	case <-broker.ctx.Done():
	default:
		t.Error("Context should be cancelled")
	}
}

func TestRedisBroker_Finish_With_Modified_Message(t *testing.T) {
	client := skipIfRedisUnavailable(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := NewRedisBrokerWithClient(client, RedisBrokerOptions{Prefix: testPrefix})
	defer broker.Close()

	ctx := context.Background()
	topic := "test-finish-bug"
	msg := &kabaka.Message{Id: "msg-1", Value: []byte("test"), Retry: 3}

	processingKey := testPrefix + topic + ":processing"
	msgKey := testPrefix + topic + ":messages"
	data, _ := json.Marshal(msg)
	client.HSet(ctx, msgKey, msg.Id, data)
	client.RPush(ctx, processingKey, msg.Id)

	msg.Retry = 2 // Modify content

	err := broker.Finish(ctx, topic, msg, nil, 0)
	if err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	length, _ := client.LLen(ctx, processingKey).Result()
	if length != 0 {
		t.Errorf("FAIL: ID-based Ack should have removed message even if modified")
	}
}
