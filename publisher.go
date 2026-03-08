package kabaka

import (
	"context"
	"time"

	"github.com/weichen-lin/kabaka/broker"
)

// Publish publishes a message to the specified topic.
func (k *Kabaka) Publish(topicName string, message []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), k.brokerTimeout)
	defer cancel()

	meta, err := k.getOrFetchMetadata(ctx, topicName)
	if err != nil {
		return ErrTopicNotFound
	}

	msg := &broker.Message{
		Id:             NewUUID(),
		InternalName:   meta.InternalName,
		Value:          message,
		Retry:          meta.MaxRetries,
		ProcessTimeout: meta.ProcessTimeout,
		CreatedAt:      time.Now(),
	}

	err = k.broker.Push(ctx, msg)

	if err != nil {
		k.logger.Error(&LogMessage{
			TopicName:     topicName,
			Action:        Publish,
			MessageID:     msg.Id,
			Message:       "Failed to publish message: " + err.Error(),
			MessageStatus: Error,
			CreatedAt:     time.Now(),
		})
	} else {
		k.logger.Info(&LogMessage{
			TopicName:     topicName,
			Action:        Publish,
			MessageID:     msg.Id,
			Message:       "Message published",
			MessageStatus: Success,
			CreatedAt:     time.Now(),
		})
	}

	return err
}

// PublishDelayed publishes a message to the specified topic with a delay.
func (k *Kabaka) PublishDelayed(topicName string, message []byte, delay time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), k.brokerTimeout)
	defer cancel()

	meta, err := k.getOrFetchMetadata(ctx, topicName)
	if err != nil {
		return ErrTopicNotFound
	}

	msg := &broker.Message{
		Id:             NewUUID(),
		InternalName:   meta.InternalName,
		Value:          message,
		Retry:          meta.MaxRetries,
		ProcessTimeout: meta.ProcessTimeout,
		CreatedAt:      time.Now(),
	}

	err = k.broker.PushDelayed(ctx, msg, delay)

	if err != nil {
		k.logger.Error(&LogMessage{
			TopicName:     topicName,
			Action:        Publish,
			MessageID:     msg.Id,
			Message:       "Failed to publish delayed message: " + err.Error(),
			MessageStatus: Error,
			CreatedAt:     time.Now(),
		})
	} else {
		k.logger.Info(&LogMessage{
			TopicName:     topicName,
			Action:        Publish,
			MessageID:     msg.Id,
			Message:       "Delayed message published",
			MessageStatus: Success,
			CreatedAt:     time.Now(),
		})
	}

	return err
}
