package kabaka

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func Test_NewKabaKa(t *testing.T) {
	config := &Config{}
	kb := NewKabaka(config)

	require.NotNil(t, kb)
}
func TestCreateTopic(t *testing.T) {
	kb := NewKabaka(&Config{})
	err := kb.CreateTopic("topic1")
	require.NoError(t, err)
	require.Len(t, kb.topics, 1)
	require.Len(t, kb.topics["topic1"].subscribers, 0)
	require.Len(t, kb.topics["topic1"].activeSubscribers, 0)
	// require.Equal(t, kb.topics["topic1"].Messages.size, 20)

	err = kb.CreateTopic("topic1")
	require.ErrorIs(t, err, ErrTopicAlreadyCreated)
}

func TestSubscribe(t *testing.T) {
	kb := NewKabaka(&Config{
		Logger: &MockLogger{},
	})
	err := kb.CreateTopic("topic1")
	require.NoError(t, err)

	id, err := kb.Subscribe("topic1", func(msg *Message) error {
		return nil
	})

	require.NoError(t, err)

	err = uuid.Validate(id.String())
	require.NoError(t, err)

	require.NotEqual(t, id, "")
	require.Equal(t, kb.topics["topic1"].subscribers[0].id, id)
	require.Len(t, kb.topics["topic1"].subscribers, 1)
	require.Equal(t, kb.topics["topic1"].subscribers[0].active, true)
	require.Equal(t, kb.topics["topic1"].activeSubscribers[0].id, id)
	require.Len(t, kb.topics["topic1"].activeSubscribers, 1)

	_, err = kb.Subscribe("topic_not_exist", func(msg *Message) error {
		return nil
	})
	require.ErrorIs(t, err, ErrTopicNotFound)
}

func TestUnSubscribe(t *testing.T) {
	k := NewKabaka(&Config{Logger: new(MockLogger)})
	err := k.CreateTopic("test-topic")
	require.NoError(t, err)

	id, _ := k.Subscribe("test-topic", func(msg *Message) error {
		return nil
	})

	err = k.UnSubscribe("test-topic", id)
	require.NoError(t, err)

	// Try to unsubscribe from a non-existent topic
	err = k.UnSubscribe("non-existent", id)
	require.Error(t, err, ErrTopicNotFound)
}

func TestUnSubscribeEmptySubscriber(t *testing.T) {
	k := NewKabaka(&Config{Logger: new(MockLogger)})
	err := k.CreateTopic("test-topic")
	require.NoError(t, err)

	id, _ := k.Subscribe("test-topic", func(msg *Message) error {
		return nil
	})

	err = k.UnSubscribe("test-topic", id)
	require.NoError(t, err)

	topic, ok := k.topics["test-topic"]
	require.True(t, ok)
	require.Len(t, topic.activeSubscribers, 0)
	require.Len(t, topic.subscribers, 1)

	topic.subscribers = nil
	err = k.UnSubscribe("test-topic", id)
	require.Error(t, err, ErrSubscriberNotFound)
}

func TestPublish(t *testing.T) {
	k := NewKabaka(&Config{Logger: new(MockLogger)})

	err := k.CreateTopic("test-topic")
	require.NoError(t, err)

	id, err := k.Subscribe("test-topic", func(msg *Message) error {
		return errors.New("test")
	})
	require.NotEmpty(t, id)
	require.NoError(t, err)

	err = k.Publish("test-topic", []byte("test message"))
	require.NoError(t, err)

	err = k.Publish("non-existent", []byte("test message"))
	require.Error(t, err, ErrTopicNotFound)
}

func TestPublishEmptySubscriber(t *testing.T) {
	k := NewKabaka(&Config{Logger: new(MockLogger)})
	err := k.CreateTopic("test-topic")
	require.NoError(t, err)

	id, err := k.Subscribe("test-topic", func(msg *Message) error {
		return nil
	})
	require.NotEmpty(t, id)
	require.NoError(t, err)

	err = k.Publish("test-topic", []byte("test message"))
	require.NoError(t, err)

	err = k.Publish("non-existent", []byte("test message"))
	require.Error(t, err, ErrTopicNotFound)

	topic, ok := k.topics["test-topic"]
	require.True(t, ok)
	require.Len(t, topic.activeSubscribers, 1)

	topic.activeSubscribers = nil
	err = k.Publish("test-topic", []byte("test message"))
	require.Error(t, err, ErrNoActiveSubscribers)
}

func TestCloseTopic(t *testing.T) {
	k := NewKabaka(&Config{Logger: new(MockLogger)})
	err := k.CreateTopic("test-topic")
	require.NoError(t, err)

	err = k.CloseTopic("test-topic")
	require.NoError(t, err)
	require.NotContains(t, k.topics, "test-topic")

	err = k.CloseTopic("non-existent")
	require.Error(t, err, ErrTopicNotFound)
}

func TestClose(t *testing.T) {
	k := NewKabaka(&Config{Logger: new(MockLogger)})
	err := k.CreateTopic("test-topic")
	require.NoError(t, err)

	err = k.CreateTopic("test-topic-2")
	require.NoError(t, err)

	err = k.Close()
	require.NoError(t, err)
	require.Nil(t, k.topics)
}
