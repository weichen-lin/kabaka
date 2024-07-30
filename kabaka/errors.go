package kabaka

import "errors"

var ErrTopicNotFound = errors.New("topic not found")
var ErrTopicSubScriberNotFound = errors.New("topic subscriber not found")

var (
	ErrNoActiveSubscribers = errors.New("no active subscribers for topic")
	ErrSubscriberNotFound  = errors.New("subscriber not found")
)
