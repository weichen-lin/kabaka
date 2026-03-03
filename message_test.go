package kabaka

import (
	"testing"
	"time"
)

func TestMessage_Get(t *testing.T) {
	msg := &Message{
		Id:        "test-id",
		Value:     []byte("test"),
		CreatedAt: time.Now(),
		Headers:   map[string]string{"key1": "value1", "key2": "value2"},
	}

	// Test existing key
	value := msg.Get("key1")
	if value != "value1" {
		t.Errorf("Expected value1, got %s", value)
	}

	// Test non-existing key
	value = msg.Get("non-existing")
	if value != "" {
		t.Errorf("Expected empty string, got %s", value)
	}

	// Test with nil headers
	msgNil := &Message{Id: "test", Value: []byte("test")}
	value = msgNil.Get("key")
	if value != "" {
		t.Errorf("Expected empty string for nil headers, got %s", value)
	}
}

func TestMessage_Set(t *testing.T) {
	msg := &Message{
		Id:        "test-id",
		Value:     []byte("test"),
		CreatedAt: time.Now(),
	}

	// Test setting on nil headers
	msg.Set("key1", "value1")
	if msg.Headers == nil {
		t.Error("Headers should be initialized")
	}
	if msg.Get("key1") != "value1" {
		t.Errorf("Expected value1, got %s", msg.Get("key1"))
	}

	// Test updating existing key
	msg.Set("key1", "updated")
	if msg.Get("key1") != "updated" {
		t.Errorf("Expected updated, got %s", msg.Get("key1"))
	}

	// Test setting new key
	msg.Set("key2", "value2")
	if msg.Get("key2") != "value2" {
		t.Errorf("Expected value2, got %s", msg.Get("key2"))
	}
}

func TestMessage_Keys(t *testing.T) {
	msg := &Message{
		Id:        "test-id",
		Value:     []byte("test"),
		CreatedAt: time.Now(),
		Headers:   map[string]string{"key1": "value1", "key2": "value2", "key3": "value3"},
	}

	keys := msg.Keys()
	if len(keys) != 3 {
		t.Errorf("Expected 3 keys, got %d", len(keys))
	}

	// Verify all keys are present
	keyMap := make(map[string]bool)
	for _, key := range keys {
		keyMap[key] = true
	}

	expectedKeys := []string{"key1", "key2", "key3"}
	for _, expectedKey := range expectedKeys {
		if !keyMap[expectedKey] {
			t.Errorf("Expected key %s not found", expectedKey)
		}
	}

	// Test with nil headers
	msgNil := &Message{Id: "test", Value: []byte("test")}
	keys = msgNil.Keys()
	if len(keys) != 0 {
		t.Errorf("Expected 0 keys for nil headers, got %d", len(keys))
	}
}

func TestMessage_Complete(t *testing.T) {
	// Test creating a complete message
	msg := &Message{
		Id:        "msg-123",
		Value:     []byte("test message"),
		Retry:     3,
		CreatedAt: time.Now(),
		Headers:   make(map[string]string),
	}

	msg.Set("trace-id", "trace-123")
	msg.Set("user-id", "user-456")

	if msg.Id != "msg-123" {
		t.Errorf("Expected msg-123, got %s", msg.Id)
	}

	if string(msg.Value) != "test message" {
		t.Errorf("Expected 'test message', got %s", string(msg.Value))
	}

	if msg.Retry != 3 {
		t.Errorf("Expected retry count 3, got %d", msg.Retry)
	}

	if msg.Get("trace-id") != "trace-123" {
		t.Errorf("Expected trace-123, got %s", msg.Get("trace-id"))
	}

	if msg.Get("user-id") != "user-456" {
		t.Errorf("Expected user-456, got %s", msg.Get("user-id"))
	}

	keys := msg.Keys()
	if len(keys) != 2 {
		t.Errorf("Expected 2 keys, got %d", len(keys))
	}
}
