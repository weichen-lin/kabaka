package kabaka

import (
	"time"
)

type Message struct {
	ID        string
	Value     []byte
	Retry     int
	CreatedAt time.Time
	Headers   map[string]string
}

func (m *Message) Get(key string) string {
	for mapkey, value := range m.Headers {
		if key == mapkey {
			return value
		}
	}

	return ""
}

func (m *Message) Set(key string, value string) {
	m.Headers[key] = value
}

func (m *Message) Keys() []string {
	var keys []string

	for key := range m.Headers {
		keys = append(keys, m.Headers[key])
	}

	return keys
}
