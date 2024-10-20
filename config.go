package kabaka

import (
	"time"

	"github.com/google/uuid"
)

type Action string

const (
	Subscribe Action = "subscribe"
	Publish   Action = "publish"
	Consume   Action = "consume"
	Cancelled Action = "cancelled"
)

type MessageStatus string

const (
	Success MessageStatus = "success"
	Retry   MessageStatus = "retry"
	Error   MessageStatus = "error"
)

type LogMessage struct {
	TopicName     string            `json:"topic_name"`
	Action        Action            `json:"action"`
	MessageID     uuid.UUID         `json:"message_id"`
	Message       string            `json:"message"`
	MessageStatus MessageStatus     `json:"message_status"`
	SubScriber    uuid.UUID         `json:"subscriber"`
	SpendTime     int64             `json:"spend_time"`
	CreatedAt     time.Time         `json:"created_at"`
	Headers       map[string]string `json:"headers"`
}

type Logger interface {
	Debug(args *LogMessage)
	Info(args *LogMessage)
	Warn(args *LogMessage)
	Error(args *LogMessage)
}

type Config struct {
	Logger Logger
	TimeOut time.Duration
	RetryDelay time.Duration
}

type Options struct {
	Name       string
	BufferSize int
	DefaultTimeout time.Duration
	DefaultRetryDelay time.Duration
}

func NewOptions(name string) *Options {
	return &Options{
		Name:       name,
		BufferSize: 24,
		DefaultTimeout: 5 * time.Second,
		DefaultRetryDelay: 5 * time.Second,
	}
}
