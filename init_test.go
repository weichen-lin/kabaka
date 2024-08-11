package kabaka

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func Test_NewKabaKa(t *testing.T) {
	config := &Config{}
	kb := NewKabaka(config)

	require.NotNil(t, kb)
}
func Test_CreateTopic(t *testing.T) {
	kb := NewKabaka(&Config{})
	err := kb.CreateTopic("topic1")
	require.NoError(t, err)
	require.Len(t, kb.topics, 1)
	require.Len(t, kb.topics["topic1"].Subscribers, 0)
	require.Len(t, kb.topics["topic1"].ActiveSubscribers, 0)
	// require.Equal(t, kb.topics["topic1"].Messages.size, 20)

	err = kb.CreateTopic("topic1")
	require.ErrorIs(t, err, ErrTopicAlreadyCreated)
}

func Test_Subscribe(t *testing.T) {
	kb := NewKabaka(&Config{})
	err := kb.CreateTopic("topic1")
	require.NoError(t, err)

	id, err := kb.Subscribe("topic1", func(msg *Message) error {
		return nil
	})

	require.NoError(t, err)

	err = uuid.Validate(id.String())
	require.NoError(t, err)

	require.NotEqual(t, id, "")
	require.Equal(t, kb.topics["topic1"].Subscribers[0].ID, id)
	require.Len(t, kb.topics["topic1"].Subscribers, 1)
	require.Equal(t, kb.topics["topic1"].Subscribers[0].Active, true)
	require.Equal(t, kb.topics["topic1"].ActiveSubscribers[0].ID, id)
	require.Len(t, kb.topics["topic1"].ActiveSubscribers, 1)

	_, err = kb.Subscribe("topic_not_exist", func(msg *Message) error {
		return nil
	})
	require.ErrorIs(t, err, ErrTopicNotFound)
}