package kabaka

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestDelayedTasks_Memory(t *testing.T) {
	k := NewKabaka(WithBroker(NewMemoryBroker(10)))
	defer k.Close()

	received := make(chan bool, 1)
	handler := func(ctx context.Context, msg *Message) error {
		received <- true
		return nil
	}

	topic := "test-delayed-memory"
	err := k.CreateTopic(topic, handler)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	startTime := time.Now()
	delay := 2 * time.Second
	err = k.PublishDelayed(topic, []byte("hello delayed"), delay)
	if err != nil {
		t.Fatalf("failed to publish delayed message: %v", err)
	}

	select {
	case <-received:
		elapsed := time.Since(startTime)
		if elapsed < delay {
			t.Errorf("received message too early: got %v, expected at least %v", elapsed, delay)
		}
		t.Logf("received delayed message after %v", elapsed)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for delayed message")
	}
}

func TestDelayedTasks_Redis(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	ctx := context.Background()

	// Check if redis is available, skip if not
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skip("Redis not available, skipping integration test")
	}

	broker := NewRedisBrokerWithClient(client)
	k := NewKabaka(WithBroker(broker))
	defer k.Close()

	received := make(chan bool, 1)
	handler := func(ctx context.Context, msg *Message) error {
		received <- true
		return nil
	}

	topic := "test-delayed-redis-" + NewUUID()[:8]
	err := k.CreateTopic(topic, handler)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	startTime := time.Now()
	delay := 2 * time.Second
	err = k.PublishDelayed(topic, []byte("hello delayed redis"), delay)
	if err != nil {
		t.Fatalf("failed to publish delayed message: %v", err)
	}

	select {
	case <-received:
		elapsed := time.Since(startTime)
		if elapsed < delay {
			t.Errorf("received message too early: got %v, expected at least %v", elapsed, delay)
		}
		t.Logf("received delayed message after %v", elapsed)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for delayed message")
	}
}
