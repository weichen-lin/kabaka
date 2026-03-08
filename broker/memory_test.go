package broker

import (
	"context"
	"testing"
	"time"
)

func TestNewMemoryBroker(t *testing.T) {
	broker := NewMemoryBroker()
	defer broker.Close()

	if broker == nil {
		t.Fatal("Expected non-nil broker")
	}

	if broker.messages == nil {
		t.Error("Expected messages queue to be initialized")
	}

	if broker.delayed == nil {
		t.Error("Expected delayed heap to be initialized")
	}

	if broker.processing == nil {
		t.Error("Expected processing map to be initialized")
	}

	if broker.metadata == nil {
		t.Error("Expected metadata map to be initialized")
	}
}

func TestRegisterAndUnregister(t *testing.T) {
	broker := NewMemoryBroker()
	defer broker.Close()

	ctx := context.Background()
	meta := &TopicMetadata{
		Name:         "test-topic",
		InternalName: "internal-test",
	}

	err := broker.Register(ctx, meta)
	if err != nil {
		t.Fatalf("Failed to register topic: %v", err)
	}

	err = broker.Register(ctx, meta)
	if err == nil {
		t.Error("Expected error when registering duplicate topic")
	}

	retrieved, err := broker.GetTopicMetadata(ctx, "test-topic")
	if err != nil {
		t.Fatalf("Failed to get topic metadata: %v", err)
	}
	if retrieved.Name != meta.Name {
		t.Errorf("Expected topic name %s, got %s", meta.Name, retrieved.Name)
	}

	err = broker.Unregister(ctx, "test-topic")
	if err != nil {
		t.Fatalf("Failed to unregister topic: %v", err)
	}

	err = broker.Unregister(ctx, "non-existent")
	if err == nil {
		t.Error("Expected error when unregistering non-existent topic")
	}
}

func TestPushAndWatch(t *testing.T) {
	broker := NewMemoryBroker()
	defer broker.Close()

	ctx := context.Background()

	taskCh, err := broker.Watch(ctx)
	if err != nil {
		t.Fatalf("Failed to start watching: %v", err)
	}

	msg := &Message{
		Id:           "msg-1",
		InternalName: "test-topic",
		Value:        []byte("test message"),
	}

	err = broker.Push(ctx, msg)
	if err != nil {
		t.Fatalf("Failed to push message: %v", err)
	}

	select {
	case task := <-taskCh:
		if task.Message.Id != msg.Id {
			t.Errorf("Expected message ID %s, got %s", msg.Id, task.Message.Id)
		}
		if string(task.Message.Value) != "test message" {
			t.Errorf("Expected message value 'test message', got %s", string(task.Message.Value))
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
}

func TestPushDelayed(t *testing.T) {
	broker := NewMemoryBroker()
	defer broker.Close()

	ctx := context.Background()

	taskCh, err := broker.Watch(ctx)
	if err != nil {
		t.Fatalf("Failed to start watching: %v", err)
	}

	msg := &Message{
		Id:           "msg-delayed",
		InternalName: "test-topic",
		Value:        []byte("delayed message"),
	}

	delay := 200 * time.Millisecond
	err = broker.PushDelayed(ctx, msg, delay)
	if err != nil {
		t.Fatalf("Failed to push delayed message: %v", err)
	}

	select {
	case <-taskCh:
		t.Error("Message should not be dispatched immediately")
	case <-time.After(50 * time.Millisecond):
	}

	select {
	case task := <-taskCh:
		if task.Message.Id != msg.Id {
			t.Errorf("Expected message ID %s, got %s", msg.Id, task.Message.Id)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Timeout waiting for delayed message")
	}
}

func TestFinish(t *testing.T) {
	broker := NewMemoryBroker()
	defer broker.Close()

	ctx := context.Background()

	taskCh, err := broker.Watch(ctx)
	if err != nil {
		t.Fatalf("Failed to start watching: %v", err)
	}

	msg := &Message{
		Id:           "msg-finish",
		InternalName: "test-topic",
		Value:        []byte("test"),
	}

	err = broker.Push(ctx, msg)
	if err != nil {
		t.Fatalf("Failed to push message: %v", err)
	}

	var receivedMsg *Message
	select {
	case task := <-taskCh:
		receivedMsg = task.Message
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for message")
	}

	err = broker.Finish(ctx, receivedMsg, nil, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to finish message: %v", err)
	}

	stats, err := broker.QueueStats(ctx)
	if err != nil {
		t.Fatalf("Failed to get queue stats: %v", err)
	}
	if stats.Processing != 0 {
		t.Errorf("Expected 0 processing messages, got %d", stats.Processing)
	}
}

func TestQueueStats(t *testing.T) {
	broker := NewMemoryBroker()
	defer broker.Close()

	ctx := context.Background()

	for i := 0; i < 3; i++ {
		msg := &Message{
			Id:           string(rune('a' + i)),
			InternalName: "test-topic",
			Value:        []byte("test"),
		}
		err := broker.Push(ctx, msg)
		if err != nil {
			t.Fatalf("Failed to push message: %v", err)
		}
	}

	delayedMsg := &Message{
		Id:           "delayed",
		InternalName: "test-topic",
		Value:        []byte("delayed"),
	}
	err := broker.PushDelayed(ctx, delayedMsg, 1*time.Second)
	if err != nil {
		t.Fatalf("Failed to push delayed message: %v", err)
	}

	stats, err := broker.QueueStats(ctx)
	if err != nil {
		t.Fatalf("Failed to get queue stats: %v", err)
	}

	if stats.Pending != 3 {
		t.Errorf("Expected 3 pending messages, got %d", stats.Pending)
	}
	if stats.Delayed != 1 {
		t.Errorf("Expected 1 delayed message, got %d", stats.Delayed)
	}
}

func TestClose(t *testing.T) {
	broker := NewMemoryBroker()

	ctx := context.Background()

	_, err := broker.Watch(ctx)
	if err != nil {
		t.Fatalf("Failed to start watching: %v", err)
	}

	err = broker.Close()
	if err != nil {
		t.Fatalf("Failed to close broker: %v", err)
	}

	if !broker.isClosed() {
		t.Error("Expected broker to be closed")
	}

	err = broker.Push(ctx, &Message{Id: "test"})
	if err == nil {
		t.Error("Expected error when pushing to closed broker")
	}

	err = broker.Register(ctx, &TopicMetadata{Name: "test"})
	if err == nil {
		t.Error("Expected error when registering to closed broker")
	}
}

func TestCleanupStaleProcessing(t *testing.T) {
	broker := NewMemoryBroker()
	defer broker.Close()

	ctx := context.Background()

	taskCh, err := broker.Watch(ctx)
	if err != nil {
		t.Fatalf("Failed to start watching: %v", err)
	}

	msg := &Message{
		Id:             "stale-msg",
		InternalName:   "test-topic",
		Value:          []byte("test"),
		ProcessTimeout: 100 * time.Millisecond,
		Retry:          0,
	}

	err = broker.Push(ctx, msg)
	if err != nil {
		t.Fatalf("Failed to push message: %v", err)
	}

	select {
	case <-taskCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message")
	}

	time.Sleep(1500 * time.Millisecond)

	select {
	case task := <-taskCh:
		if task.Message.Id != msg.Id {
			t.Errorf("Expected requeued message ID %s, got %s", msg.Id, task.Message.Id)
		}
		if task.Message.Retry < 1 {
			t.Errorf("Expected retry count >= 1, got %d", task.Message.Retry)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for requeued message")
	}
}

func TestUnregisterAndCleanup(t *testing.T) {
	broker := NewMemoryBroker()
	defer broker.Close()

	ctx := context.Background()

	meta := &TopicMetadata{
		Name:         "cleanup-topic",
		InternalName: "internal-cleanup",
	}
	err := broker.Register(ctx, meta)
	if err != nil {
		t.Fatalf("Failed to register topic: %v", err)
	}

	for i := 0; i < 3; i++ {
		msg := &Message{
			Id:           "cleanup-pending-" + string(rune('a'+i)),
			InternalName: "internal-cleanup",
			Value:        []byte("test"),
		}
		err := broker.Push(ctx, msg)
		if err != nil {
			t.Fatalf("Failed to push message: %v", err)
		}
	}

	delayedMsg := &Message{
		Id:           "cleanup-delayed",
		InternalName: "internal-cleanup",
		Value:        []byte("delayed"),
	}
	err = broker.PushDelayed(ctx, delayedMsg, 1*time.Second)
	if err != nil {
		t.Fatalf("Failed to push delayed message: %v", err)
	}

	stats, _ := broker.TopicQueueStats(ctx, "internal-cleanup")
	if stats.Pending != 3 {
		t.Errorf("Expected 3 pending messages before cleanup, got %d", stats.Pending)
	}
	if stats.Delayed != 1 {
		t.Errorf("Expected 1 delayed message before cleanup, got %d", stats.Delayed)
	}

	err = broker.UnregisterAndCleanup(ctx, "cleanup-topic")
	if err != nil {
		t.Fatalf("Failed to unregister and cleanup: %v", err)
	}

	stats, _ = broker.TopicQueueStats(ctx, "internal-cleanup")
	if stats.Pending != 0 {
		t.Errorf("Expected 0 pending messages after cleanup, got %d", stats.Pending)
	}
	if stats.Delayed != 0 {
		t.Errorf("Expected 0 delayed messages after cleanup, got %d", stats.Delayed)
	}

	err = broker.UnregisterAndCleanup(ctx, "non-existent")
	if err == nil {
		t.Error("Expected error when cleaning up non-existent topic")
	}
}

func TestTopicQueueStats(t *testing.T) {
	broker := NewMemoryBroker()
	defer broker.Close()

	ctx := context.Background()

	for i := 0; i < 2; i++ {
		msg := &Message{
			Id:           "topic1-" + string(rune('a'+i)),
			InternalName: "topic-1",
			Value:        []byte("test"),
		}
		err := broker.Push(ctx, msg)
		if err != nil {
			t.Fatalf("Failed to push message: %v", err)
		}
	}

	for i := 0; i < 3; i++ {
		msg := &Message{
			Id:           "topic2-" + string(rune('a'+i)),
			InternalName: "topic-2",
			Value:        []byte("test"),
		}
		err := broker.Push(ctx, msg)
		if err != nil {
			t.Fatalf("Failed to push message: %v", err)
		}
	}

	delayedMsg := &Message{
		Id:           "topic1-delayed",
		InternalName: "topic-1",
		Value:        []byte("delayed"),
	}
	err := broker.PushDelayed(ctx, delayedMsg, 1*time.Second)
	if err != nil {
		t.Fatalf("Failed to push delayed message: %v", err)
	}

	stats, err := broker.TopicQueueStats(ctx, "topic-1")
	if err != nil {
		t.Fatalf("Failed to get topic queue stats: %v", err)
	}

	if stats.Pending != 2 {
		t.Errorf("Expected 2 pending messages for topic-1, got %d", stats.Pending)
	}
	if stats.Delayed != 1 {
		t.Errorf("Expected 1 delayed message for topic-1, got %d", stats.Delayed)
	}
}

func TestFinishNonExistentMessage(t *testing.T) {
	broker := NewMemoryBroker()
	defer broker.Close()

	ctx := context.Background()

	msg := &Message{
		Id:           "non-existent",
		InternalName: "test-topic",
		Value:        []byte("test"),
	}

	err := broker.Finish(ctx, msg, nil, 0)
	if err == nil {
		t.Error("Expected error when finishing non-existent message")
	}
}

func TestGetTopicMetadataNotFound(t *testing.T) {
	broker := NewMemoryBroker()
	defer broker.Close()

	ctx := context.Background()

	_, err := broker.GetTopicMetadata(ctx, "non-existent")
	if err == nil {
		t.Error("Expected error when getting non-existent topic metadata")
	}
}

func TestPushDelayedClosedBroker(t *testing.T) {
	broker := NewMemoryBroker()
	broker.Close()

	ctx := context.Background()

	msg := &Message{
		Id:           "test",
		InternalName: "test-topic",
		Value:        []byte("test"),
	}

	err := broker.PushDelayed(ctx, msg, 1*time.Second)
	if err == nil {
		t.Error("Expected error when pushing delayed message to closed broker")
	}
}

func TestWatchClosedBroker(t *testing.T) {
	broker := NewMemoryBroker()
	broker.Close()

	ctx := context.Background()

	_, err := broker.Watch(ctx)
	if err == nil {
		t.Error("Expected error when watching closed broker")
	}
}

func TestMultipleDelayedMessages(t *testing.T) {
	broker := NewMemoryBroker()
	defer broker.Close()

	ctx := context.Background()

	taskCh, err := broker.Watch(ctx)
	if err != nil {
		t.Fatalf("Failed to start watching: %v", err)
	}

	messages := []struct {
		id    string
		delay time.Duration
	}{
		{"msg-3", 300 * time.Millisecond},
		{"msg-1", 100 * time.Millisecond},
		{"msg-2", 200 * time.Millisecond},
	}

	for _, m := range messages {
		msg := &Message{
			Id:           m.id,
			InternalName: "test-topic",
			Value:        []byte("test"),
		}
		err := broker.PushDelayed(ctx, msg, m.delay)
		if err != nil {
			t.Fatalf("Failed to push delayed message: %v", err)
		}
	}

	receivedOrder := []string{}
	for i := 0; i < 3; i++ {
		select {
		case task := <-taskCh:
			receivedOrder = append(receivedOrder, task.Message.Id)
		case <-time.After(1 * time.Second):
			t.Fatalf("Timeout waiting for message %d", i+1)
		}
	}

	expectedOrder := []string{"msg-1", "msg-2", "msg-3"}
	for i, id := range expectedOrder {
		if receivedOrder[i] != id {
			t.Errorf("Expected message %d to be %s, got %s", i+1, id, receivedOrder[i])
		}
	}
}

func TestRegisterClosedBroker(t *testing.T) {
	broker := NewMemoryBroker()
	broker.Close()

	ctx := context.Background()

	meta := &TopicMetadata{
		Name:         "test-topic",
		InternalName: "internal-test",
	}

	err := broker.Register(ctx, meta)
	if err == nil {
		t.Error("Expected error when registering to closed broker")
	}
}

func TestUnregisterClosedBroker(t *testing.T) {
	broker := NewMemoryBroker()
	ctx := context.Background()

	meta := &TopicMetadata{
		Name:         "test-topic",
		InternalName: "internal-test",
	}

	err := broker.Register(ctx, meta)
	if err != nil {
		t.Fatalf("Failed to register topic: %v", err)
	}

	broker.Close()

	err = broker.Unregister(ctx, "test-topic")
	if err == nil {
		t.Error("Expected error when unregistering from closed broker")
	}
}

func TestWatchMultipleTimes(t *testing.T) {
	broker := NewMemoryBroker()
	defer broker.Close()

	ctx := context.Background()

	ch1, err := broker.Watch(ctx)
	if err != nil {
		t.Fatalf("Failed to watch first time: %v", err)
	}

	ch2, err := broker.Watch(ctx)
	if err != nil {
		t.Fatalf("Failed to watch second time: %v", err)
	}

	if ch1 != ch2 {
		t.Error("Expected same channel on multiple Watch calls")
	}
}

func TestProcessingWithQueueStats(t *testing.T) {
	broker := NewMemoryBroker()
	defer broker.Close()

	ctx := context.Background()

	taskCh, err := broker.Watch(ctx)
	if err != nil {
		t.Fatalf("Failed to start watching: %v", err)
	}

	msg := &Message{
		Id:           "processing-msg",
		InternalName: "test-topic",
		Value:        []byte("test"),
	}

	err = broker.Push(ctx, msg)
	if err != nil {
		t.Fatalf("Failed to push message: %v", err)
	}

	select {
	case <-taskCh:
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for message")
	}

	stats, err := broker.QueueStats(ctx)
	if err != nil {
		t.Fatalf("Failed to get queue stats: %v", err)
	}

	if stats.Processing != 1 {
		t.Errorf("Expected 1 processing message, got %d", stats.Processing)
	}
}
