package kabaka

import "errors"

var (
	ErrTopicNotFound       = errors.New("topic not found")
	ErrTopicAlreadyCreated = errors.New("topic already exists")
	ErrInvalidSchema       = errors.New("message value does not match schema")
)
