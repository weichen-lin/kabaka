package kabaka

import (
	"log"
	"time"

	"github.com/google/uuid"
)

// ANSI 顏色代碼
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorPurple = "\033[35m"
	colorCyan   = "\033[36m"
	colorWhite  = "\033[37m"
)

type Logger interface {
	Debug(args *LogMessage)
	Info(args *LogMessage)
	Warn(args *LogMessage)
	Error(args *LogMessage)
}

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
	SpendTime     int64             `json:"spend_time"`
	CreatedAt     time.Time         `json:"created_at"`
	Headers       map[string]string `json:"headers"`
}

type DefaultLogger struct{}

func (l *DefaultLogger) Debug(args *LogMessage) {
	log.Printf("%s[DEBUG]%s %s: %s - Action: %s, Status: %s, SpendTime: %dms, CreatedAt: %s, Headers: %v\n",
		colorCyan, colorReset, args.TopicName, args.Message, args.Action, args.MessageStatus, args.SpendTime, args.CreatedAt.Format(time.RFC3339), args.Headers)
}

func (l *DefaultLogger) Info(args *LogMessage) {
	log.Printf("%s[INFO]%s %s: %s - Action: %s, Status: %s, SpendTime: %dms, CreatedAt: %s, Headers: %v\n",
		colorGreen, colorReset, args.TopicName, args.Message, args.Action, args.MessageStatus, args.SpendTime, args.CreatedAt.Format(time.RFC3339), args.Headers)
}

func (l *DefaultLogger) Warn(args *LogMessage) {
	log.Printf("%s[WARN]%s %s: %s - Action: %s, Status: %s, SpendTime: %dms, CreatedAt: %s, Headers: %v\n",
		colorYellow, colorReset, args.TopicName, args.Message, args.Action, args.MessageStatus, args.SpendTime, args.CreatedAt.Format(time.RFC3339), args.Headers)
}

func (l *DefaultLogger) Error(args *LogMessage) {
	log.Printf("%s[ERROR]%s %s: %s - Action: %s, Status: %s, SpendTime: %dms, CreatedAt: %s, Headers: %v\n",
		colorRed, colorReset, args.TopicName, args.Message, args.Action, args.MessageStatus, args.SpendTime, args.CreatedAt.Format(time.RFC3339), args.Headers)
}
