package kabaka

import (
	"errors"

	"github.com/google/uuid"
)

type HandleFunc func(msg *Message) error

type Message struct {
	Value []byte
}

type Subscriber struct {
}

type Topic struct {
	Name        string
	Subscribers map[uuid.UUID]chan *Message
}

func (t *Topic) subscribe(handler HandleFunc) chan *Message {
	ch := make(chan *Message, 20)
	id := uuid.New()

	t.Subscribers[id] = ch

	go func() {
		for {
			select {
			case msg := <-ch:
				err := handler(msg)
				if err != nil {
					return
				}
			}
		}
	}()

	return ch
}

func (t *Topic) publish(msg *Message) {
	for _, ch := range t.Subscribers {
		ch <- msg
	}
}

func (t *Kabaka) CreateTopic(name string) error {
	if _, ok := t.topics[name]; ok {
		return errors.New("topic already exists")
	}

	topic := &Topic{
		Name:        name,
		Subscribers: make(map[uuid.UUID]chan *Message),
	}

	t.topics[name] = topic

	return nil
}

func (t *Kabaka) Subscribe(name string, handler HandleFunc) (chan *Message, error) {
	topic, ok := t.topics[name]
	if !ok {
		return nil, errors.New("topic not found")
	}

	return topic.subscribe(handler), nil
}

func (t *Kabaka) Publish(name string, msg *Message) error {
	topic, ok := t.topics[name]
	if !ok {
		return errors.New("topic not found")
	}

	topic.publish(msg)

	return nil
}