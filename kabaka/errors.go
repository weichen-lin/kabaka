package kabaka

import "errors"

var (
	ErrTopicNotFound           = errors.New("topic not found")
	ErrTopicSubScriberNotFound = errors.New("topic subscriber not found")
	ErrTopicAlreadyCreated     = errors.New("topic already exists")
)

var (
	ErrNoActiveSubscribers = errors.New("no active subscribers for topic")
	ErrSubscriberNotFound  = errors.New("subscriber not found")
)
