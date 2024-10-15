package kabaka

import (
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

type Message struct {
	ID       uuid.UUID
	Value    []byte
	Retry    int
	CreateAt time.Time
	UpdateAt time.Time
	Headers  map[string]string
	RootSpan trace.Span
}

func (l *Message) Get(key string) string {
	for mapkey, value := range l.Headers {
		if key == mapkey {
			return value
		}
	}

	return ""
}
func (l *Message) Set(key string, value string) {
	l.Headers[key] = value
}

func (l *Message) Keys() []string {
	var keys []string

	for key := range l.Headers {
		keys = append(keys, l.Headers[key])
	}

	return keys
}
