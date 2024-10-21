package kabaka

import (
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
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
	SubScriber    uuid.UUID         `json:"subscriber"`
	SpendTime     int64             `json:"spend_time"`
	CreatedAt     time.Time         `json:"created_at"`
	Headers       map[string]string `json:"headers"`
}

var (
	loggerInstance Logger
	once           sync.Once
)

func setKabakaLogger(logger Logger) {
	once.Do(func() {
		loggerInstance = logger
	})
}

func getKabakaLogger() Logger {
	return loggerInstance
}

type DefaultLogger struct{}

func (l *DefaultLogger) Debug(args *LogMessage) {
	log.Printf("[DEBUG] %s: %s - Action: %s, Status: %s, Subscriber: %s, SpendTime: %dms, CreatedAt: %s, Headers: %v",
		args.TopicName, args.Message, args.Action, args.MessageStatus, args.SubScriber, args.SpendTime, args.CreatedAt.Format(time.RFC3339), args.Headers)
}

func (l *DefaultLogger) Info(args *LogMessage) {
	log.Printf("[INFO] %s: %s - Action: %s, Status: %s, Subscriber: %s, SpendTime: %dms, CreatedAt: %s, Headers: %v",
		args.TopicName, args.Message, args.Action, args.MessageStatus, args.SubScriber, args.SpendTime, args.CreatedAt.Format(time.RFC3339), args.Headers)
}

func (l *DefaultLogger) Warn(args *LogMessage) {
	log.Printf("[WARN] %s: %s - Action: %s, Status: %s, Subscriber: %s, SpendTime: %dms, CreatedAt: %s, Headers: %v",
		args.TopicName, args.Message, args.Action, args.MessageStatus, args.SubScriber, args.SpendTime, args.CreatedAt.Format(time.RFC3339), args.Headers)
}

func (l *DefaultLogger) Error(args *LogMessage) {
	log.Printf("[ERROR] %s: %s - Action: %s, Status: %s, Subscriber: %s, SpendTime: %dms, CreatedAt: %s, Headers: %v",
		args.TopicName, args.Message, args.Action, args.MessageStatus, args.SubScriber, args.SpendTime, args.CreatedAt.Format(time.RFC3339), args.Headers)
}

func defaultLogger() Logger {
	var instance *DefaultLogger
	once.Do(func() {
		instance = &DefaultLogger{}
	})
	return instance
}
