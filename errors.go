package kabaka

import "errors"

var (
	ErrTopicNotFound           = errors.New("topic not found")
	ErrTopicAlreadyCreated     = errors.New("topic already exists")
)