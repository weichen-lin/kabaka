package kabaka

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"sync"
	"time"
)

type Kabaka struct {
	mu     sync.RWMutex
	topics map[string]*Topic
	broker Broker

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type KabakaOption func(*Kabaka)

func NewKabaka(options ...KabakaOption) *Kabaka {
	ctx, cancel := context.WithCancel(context.Background())
	k := &Kabaka{
		topics: make(map[string]*Topic),
		ctx:    ctx,
		cancel: cancel,
	}

	for _, opt := range options {
		opt(k)
	}

	return k
}

func (k *Kabaka) Start() {
	k.wg.Add(1)
	go k.dispatch()
}

func (k *Kabaka) dispatch() {
	defer k.wg.Done()

	k.mu.RLock()
	var internalNames []string
	for name := range k.topics {
		internalNames = append(internalNames, name)
	}
	k.mu.RUnlock()

	if len(internalNames) == 0 {
		return // Or handle dynamic topic registration later
	}

	taskCh, err := k.broker.Watch(k.ctx, internalNames...)
	if err != nil {
		return
	}

	for {
		select {
		case <-k.ctx.Done():
			return
		case task, ok := <-taskCh:
			if !ok {
				return
			}
			k.mu.RLock()
			topic, ok := k.topics[task.Topic]
			k.mu.RUnlock()

			if ok {
				topic.receive(task.Message)
			}
		}
	}
}

func WithBroker(broker Broker) KabakaOption {
	return func(k *Kabaka) {
		k.broker = broker
	}
}

func (k *Kabaka) generateInternalName(name string) string {
	h := sha1.New()
	h.Write([]byte(name))
	return hex.EncodeToString(h.Sum(nil))
}

func (k *Kabaka) CreateTopic(name string, handler HandleFunc, options ...Option) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	internalName := k.generateInternalName(name)

	if _, ok := k.topics[internalName]; ok {
		return ErrTopicAlreadyCreated
	}

	// Explicitly register the topic in the broker
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := k.broker.Register(ctx, internalName); err != nil {
		return err
	}

	topic := newTopic(name, internalName, k.broker, handler, options...)
	k.topics[internalName] = topic

	return nil
}

func (k *Kabaka) Publish(name string, message []byte) error {
	internalName := k.generateInternalName(name)
	topic, ok := k.topics[internalName]
	if !ok {
		return ErrTopicNotFound
	}

	err := topic.publish(message)
	if err != nil {
		return err
	}

	return nil
}

func (k *Kabaka) PublishDelayed(name string, message []byte, delay time.Duration) error {
	topic, ok := k.topics[name]
	if !ok {
		return ErrTopicNotFound
	}

	err := topic.publishDelayed(message, delay)
	if err != nil {
		return err
	}

	return nil
}

func (k *Kabaka) CloseTopic(name string) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	internalName := k.generateInternalName(name)
	topic, ok := k.topics[internalName]
	if !ok {
		return ErrTopicNotFound
	}

	topic.stop()

	// Unregister from broker to clean up resources
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = k.broker.Unregister(ctx, topic.InternalName)

	delete(k.topics, internalName)
	return nil
}

func (k *Kabaka) Close() error {
	k.cancel()
	k.wg.Wait()

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

func (k *Kabaka) GetTopicSchema(name string) (string, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	internalName := k.generateInternalName(name)
	topic, ok := k.topics[internalName]
	if !ok {
		return "", ErrTopicNotFound
	}

	return topic.schema, nil
}
