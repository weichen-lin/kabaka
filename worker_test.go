package kabaka

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestWorker_ProcessMessage(t *testing.T) {
	// Channel to signal that the handler has been called
	handlerCalled := make(chan *Message, 1)

	// Define the handler function for the topic
	handler := func(ctx context.Context, msg *Message) error {
		handlerCalled <- msg
		return nil
	}

	// Create a new topic with the test handler
	// The topic will internally create and manage workers
	topic := newTopic("test-worker-process", handler, WithMaxWorkers(1), WithBufferSize(1))
	defer topic.stop()

	// Publish a message to the topic
	testPayload := []byte("hello worker")
	err := topic.publish(testPayload)
	if err != nil {
		t.Fatalf("publish failed unexpectedly: %v", err)
	}

	// Wait for the worker to process the message or timeout
	select {
	case receivedMsg := <-handlerCalled:
		// Verify the message content
		if string(receivedMsg.Value) != string(testPayload) {
			t.Errorf("handler received message with value '%s', want '%s'", string(receivedMsg.Value), string(testPayload))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker to process message")
	}
}

func TestWorker_Stop(t *testing.T) {
	// 1. Setup
	// Use a WaitGroup to block the handler, simulating a long-running task
	var handlerWg sync.WaitGroup
	handlerWg.Add(1)

	handler := func(ctx context.Context, msg *Message) error {
		// The handler will wait until the test allows it to complete
		handlerWg.Wait()
		return nil
	}

	topic := newTopic("test-worker-stop", handler, WithMaxWorkers(1), WithBufferSize(1))

	// 2. Action
	// Publish a message that the worker will pick up and get stuck on
	err := topic.publish([]byte("message to block worker"))
	if err != nil {
		t.Fatalf("publish failed unexpectedly: %v", err)
	}

	// Give the dispatcher time to assign the message to the worker
	time.Sleep(100 * time.Millisecond)

	// Now, stop the topic. This should trigger the worker's stop logic.
	stopDone := make(chan struct{})
	go func() {
		topic.stop()
		close(stopDone)
	}()

	// 3. Assert
	// The worker is busy. The stop process should wait.
	// We verify that stop() doesn't complete immediately.
	select {
	case <-stopDone:
		t.Fatal("topic.stop() returned before handler finished, which is unexpected if worker is busy.")
	case <-time.After(100 * time.Millisecond):
		// This is expected. The stop is waiting for the worker.
	}

	// Allow the handler to complete
	handlerWg.Done()

	// Now, the stop() function should be able to complete.
	select {
	case <-stopDone:
		// This is the expected outcome.
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for topic.stop() to complete after handler finished")
	}
}
