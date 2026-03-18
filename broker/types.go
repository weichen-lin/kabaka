package broker

import (
	"context"
	"encoding/json"
	"time"
)

// Message represents a task message in the queue.
type Message struct {
	Id             string
	InternalName   string // Stores the hashed internal name for routing
	Value          []byte
	Retry          int
	ProcessTimeout time.Duration // How long the handler has to process this message
	CreatedAt      time.Time
	Headers        map[string]string
}

func (m *Message) Get(key string) string {
	if m.Headers == nil {
		return ""
	}
	return m.Headers[key]
}

func (m *Message) Set(key string, value string) {
	if m.Headers == nil {
		m.Headers = make(map[string]string)
	}
	m.Headers[key] = value
}

// TopicMetadata contains all metadata for a topic stored in the broker.
type TopicMetadata struct {
	// Identity
	Name         string    `json:"name"`          // User-facing topic name
	InternalName string    `json:"internal_name"` // Internal identifier (hash or same as name)
	Description  string    `json:"description"`   // Human-readable description
	CreatedAt    time.Time `json:"created_at"`    // Topic creation time

	// Configuration
	ProcessTimeout time.Duration `json:"process_timeout"` // Handler execution timeout
	RetryDelay     time.Duration `json:"retry_delay"`     // Base retry delay (exponential backoff)
	MaxRetries     int           `json:"max_retries"`     // Maximum retry attempts
	Schema         string        `json:"schema"`          // JSON Schema definition
	SchemaType     string        `json:"schema_type"`     // Type of schema (e.g., "json")
}

// NewTopicMetadata creates default metadata for a topic.
func NewTopicMetadata(name string, internalName string) *TopicMetadata {
	return &TopicMetadata{
		Name:           name,
		InternalName:   internalName,
		CreatedAt:      time.Now(),
		ProcessTimeout: 30 * time.Second,
		RetryDelay:     5 * time.Second,
		MaxRetries:     3,
	}
}

// QueueStats is a point-in-time snapshot of all queue lengths from the broker.
type QueueStats struct {
	Pending    int64 // tasks in the main queue waiting to be picked up
	Delayed    int64 // tasks in the delayed queue waiting for their schedule time
	Processing int64 // tasks currently being processed (in-flight)
}

// JobStatus represents the final outcome of a job.
type JobStatus string

const (
	StatusSuccess JobStatus = "success"
	StatusDead    JobStatus = "dead" // Max retries exceeded
)

// JobResult is a historical record of a processed job for auditability.
type JobResult struct {
	ID         string    `json:"id"`
	Topic      string    `json:"topic"`
	Payload    json.RawMessage `json:"payload"`
	Status     JobStatus `json:"status"`
	Error      string    `json:"error,omitempty"`
	Attempts   int       `json:"attempts"`
	DurationMs int64     `json:"duration_ms"`
	CreatedAt  time.Time `json:"created_at"`
	FinishedAt time.Time `json:"finished_at"`
}

// Task represents a task to be processed.
type Task struct {
	InternalName string
	Message      *Message
}

// InstanceInfo represents a running Kabaka instance in a distributed setup.
type InstanceInfo struct {
	ID            string    `json:"id"`
	Hostname      string    `json:"hostname"`
	StartedAt     time.Time `json:"started_at"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	Workers       int       `json:"workers"`
}

// InstanceRegistry is an optional interface for brokers that support
// multi-instance discovery. Kabaka checks this at startup via type assertion.
type InstanceRegistry interface {
	Join(ctx context.Context, info *InstanceInfo) error
	Leave(ctx context.Context, instanceID string) error
	Instances(ctx context.Context) ([]*InstanceInfo, error)
}

// Broker defines the interface for message queue brokers.
type Broker interface {
	// Topic/Metadata management
	Register(ctx context.Context, meta *TopicMetadata) error
	Unregister(ctx context.Context, topic string) error
	UnregisterAndCleanup(ctx context.Context, topic string) error
	GetTopicMetadata(ctx context.Context, name string) (*TopicMetadata, error)

	// Shared queue operations
	Push(ctx context.Context, msg *Message) error
	PushDelayed(ctx context.Context, msg *Message, delay time.Duration) error
	Watch(ctx context.Context) (<-chan *Task, error)
	Finish(ctx context.Context, msg *Message, processErr error, duration time.Duration) error

	// Audit/Traceability
	StoreResult(ctx context.Context, result *JobResult, limit int) error
	FetchResults(ctx context.Context, topic string, limit int) ([]*JobResult, error)

	// Stats and management
	QueueStats(ctx context.Context) (QueueStats, error)
	TopicQueueStats(ctx context.Context, internalName string) (QueueStats, error)
	Purge(ctx context.Context, internalName string) error
	Close() error
}
