package kabaka

import (
	"context"
	"sync"
	"sync/atomic"
)

type Kabaka struct {
	mu     sync.RWMutex
	topics map[string]*Topic
	broker Broker
}

type KabakaOption func(*Kabaka)

func NewKabaka(options ...KabakaOption) *Kabaka {
	k := &Kabaka{
		topics: make(map[string]*Topic),
		broker: NewMemoryBroker(24), // Default memory broker
	}

	for _, opt := range options {
		opt(k)
	}

	return k
}

func WithBroker(broker Broker) KabakaOption {
	return func(k *Kabaka) {
		k.broker = broker
	}
}

func (k *Kabaka) CreateTopic(name string, handler HandleFunc, options ...Option) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if _, ok := k.topics[name]; ok {
		return ErrTopicAlreadyCreated
	}

	topic := newTopic(name, k.broker, handler, options...)

	k.topics[name] = topic

	return nil
}

func (k *Kabaka) Publish(name string, message []byte) error {
	topic, ok := k.topics[name]
	if !ok {
		return ErrTopicNotFound
	}

	err := topic.publish(message)
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

	topic.stop()

	delete(k.topics, name)
	return nil
}

func (k *Kabaka) Close() error {
	k.mu.Lock()
	defer k.mu.Unlock()

	for _, topic := range k.topics {
		topic.stop()
	}

	if k.broker != nil {
		return k.broker.Close()
	}

	return nil
}

type Metric struct {
	TopicName     string
	ActiveWorkers int32
	BusyWorkers   int32
	OnGoingJobs   int32
	PendingJobs   int64
	TotalSuccess  int64
	TotalFailed   int64
	TotalRetried  int64
	P95           float64
	P99           float64
}

func (k *Kabaka) GetMetrics() []*Metric {
	k.mu.RLock()
	defer k.mu.RUnlock()

	metrics := make([]*Metric, 0)

	for _, topic := range k.topics {
		ctx := context.Background()
		pending, _ := k.broker.Len(ctx, topic.Name)
		success, failed, retried, p95, p99, _ := k.broker.GetStats(ctx, topic.Name)

		metrics = append(metrics, &Metric{
			TopicName:     topic.Name,
			ActiveWorkers: atomic.LoadInt32(&topic.activeWorkers),
			BusyWorkers:   atomic.LoadInt32(&topic.busyWorkers),
			OnGoingJobs:   atomic.LoadInt32(&topic.onGoingJobs),
			PendingJobs:   pending,
			TotalSuccess:  success,
			TotalFailed:   failed,
			TotalRetried:  retried,
			P95:           p95,
			P99:           p99,
		})
	}

	return metrics
}

func (k *Kabaka) ResetMetrics(name string) error {
	k.mu.RLock()
	defer k.mu.RUnlock()

	if _, ok := k.topics[name]; !ok {
		return ErrTopicNotFound
	}

	return k.broker.ResetStats(context.Background(), name)
}
