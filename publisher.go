package kabaka

import (
	"context"
	"fmt"
	"time"

	"github.com/weichen-lin/kabaka/broker"
)

// prepareMessage validates the message against the topic's schema and builds a broker.Message.
func (k *Kabaka) prepareMessage(ctx context.Context, topicName string, message []byte) (*broker.Message, error) {
	meta, err := k.getOrFetchMetadata(ctx, topicName)
	if err != nil {
		return nil, ErrTopicNotFound
	}

	if meta.Schema != "" {
		if err := k.validateMessage(topicName, meta.Schema, message); err != nil {
			k.logger.Error(&LogMessage{
				TopicName:     topicName,
				Action:        Publish,
				Message:       fmt.Sprintf("Schema validation failed: %v", err),
				MessageStatus: Error,
				CreatedAt:     time.Now(),
			})
			return nil, fmt.Errorf("%w: %v", ErrInvalidSchema, err)
		}
	}

	return &broker.Message{
		Id:             NewUUID(),
		InternalName:   meta.InternalName,
		Value:          message,
		Retry:          meta.MaxRetries,
		ProcessTimeout: meta.ProcessTimeout,
		CreatedAt:      time.Now(),
	}, nil
}

// logPublishResult logs the outcome of a publish operation.
func (k *Kabaka) logPublishResult(topicName string, msgID string, err error, delayed bool) {
	if err != nil {
		action := "Failed to publish message: "
		if delayed {
			action = "Failed to publish delayed message: "
		}
		k.logger.Error(&LogMessage{
			TopicName:     topicName,
			Action:        Publish,
			MessageID:     msgID,
			Message:       action + err.Error(),
			MessageStatus: Error,
			CreatedAt:     time.Now(),
		})
	} else {
		msg := "Message published"
		if delayed {
			msg = "Delayed message published"
		}
		k.logger.Info(&LogMessage{
			TopicName:     topicName,
			Action:        Publish,
			MessageID:     msgID,
			Message:       msg,
			MessageStatus: Success,
			CreatedAt:     time.Now(),
		})
	}
}

// Publish publishes a message to the specified topic.
func (k *Kabaka) Publish(topicName string, message []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), k.brokerTimeout)
	defer cancel()

	msg, err := k.prepareMessage(ctx, topicName, message)
	if err != nil {
		return err
	}

	err = k.broker.Push(ctx, msg)
	k.logPublishResult(topicName, msg.Id, err, false)
	return err
}

// PublishDelayed publishes a message to the specified topic with a delay.
func (k *Kabaka) PublishDelayed(topicName string, message []byte, delay time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), k.brokerTimeout)
	defer cancel()

	msg, err := k.prepareMessage(ctx, topicName, message)
	if err != nil {
		return err
	}

	err = k.broker.PushDelayed(ctx, msg, delay)
	k.logPublishResult(topicName, msg.Id, err, true)
	return err
}
