package kabaka

import "time"

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
