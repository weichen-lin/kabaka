package kabaka

import (
	"time"
)

type Options struct {
	BufferSize            int
	DefaultMaxRetries     int
	DefaultRetryDelay     time.Duration
	DefaultProcessTimeout time.Duration
	Logger                Logger
}

func getDefaultOptions() *Options {
	return &Options{
		BufferSize:            24,
		DefaultMaxRetries:     3,
		DefaultRetryDelay:     5 * time.Second,
		DefaultProcessTimeout: 10 * time.Second,
		Logger:                defaultLogger(),
	}
}
