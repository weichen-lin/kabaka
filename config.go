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
)

type MessageStatus string

const (
	Success MessageStatus = "success"
	Retry   MessageStatus = "retry"
	Error   MessageStatus = "error"
)

type LogMessage struct {
	TopicName     string        `json:"topic_name"`
	Action        Action        `json:"action"`
	MessageID     uuid.UUID     `json:"message_id"`
	Message       string        `json:"message"`
	MessageStatus MessageStatus `json:"message_status"`
	SubScriber    uuid.UUID     `json:"subscriber"`
	SpendTime     int64         `json:"spend_time"`
	CreatedAt     time.Time     `json:"created_at"`
}

type Logger interface {
	Debug(args *LogMessage)
	Info(args *LogMessage)
	Warn(args *LogMessage)
	Error(args *LogMessage)
}

type Config struct {
	logger Logger
}

type Options struct {
	Name       string
	BufferSize int
}

func NewOptions(name string) *Options {
	return &Options{
		Name:       name,
		BufferSize: 24,
	}
}
