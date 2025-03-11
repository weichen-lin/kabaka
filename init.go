package kabaka

import (
	"sync"
)

type Kabaka struct {
	mu sync.RWMutex
	topics  map[string]*Topic
	options *Options
}

func NewKabaka(options *Options) *Kabaka {
	if options == nil {
		options = getDefaultOptions()
	}

	return &Kabaka{
		topics:  make(map[string]*Topic),
		options: options,
	}
}

func (k *Kabaka) CreateTopic(name string, handler HandleFunc) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if _, ok := k.topics[name]; ok {
		return ErrTopicAlreadyCreated
	}

	topic := NewTopic(name, k.options, handler)

	k.topics[name] = topic

	return nil
}

func (k *Kabaka) Publish(name string, message []byte) error {
	topic, ok := k.topics[name]
	if !ok {
		return ErrTopicNotFound
	}

	err := topic.Publish(message)
	if err != nil {
		return err
	}

	return nil
}

func (k *Kabaka) CloseTopic(name string) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	topic, ok := k.topics[name]
	if !ok {
		return ErrTopicNotFound
	}

	topic.Stop()

	delete(k.topics, name)
	return nil
}

func (k *Kabaka) Close() error {
	k.mu.Lock()
	defer k.mu.Unlock()

	for _, topic := range k.topics {
		topic.Stop()
	}
	k.topics = nil
	return nil
}

type Metric struct {
	TopicName     string
	ActiveWorkers int32
	BusyWorkers   int32
	OnGoingJobs   int32
}

func (k *Kabaka) GetMetrics() []*Metric {
	k.mu.RLock()
	defer k.mu.RUnlock()

	metrics := make([]*Metric, 0)

	for _, topic := range k.topics {
		metrics = append(metrics, &Metric{
			TopicName:     topic.Name,
			ActiveWorkers: topic.activeWorkers,
			BusyWorkers:   topic.busyWorkers,
			OnGoingJobs:   topic.onGoingJobs,
		})
	}

	return metrics
}
