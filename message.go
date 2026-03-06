package kabaka

import (
	"time"
)

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

func (m *Message) Keys() []string {
	var keys []string
	for key := range m.Headers {
		keys = append(keys, key)
	}
	return keys
}
