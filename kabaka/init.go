package kabaka

import "sync"

type Kabaka struct {
	sync.RWMutex
	topics map[string]*Topic
	logger Logger
}

func NewKabaka(config *Config) *Kabaka {
	return &Kabaka{
		topics: make(map[string]*Topic),
		logger: config.logger,
	}
}
