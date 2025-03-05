package kabaka

import (
	"time"
)

type Options struct {
	BufferSize        int
	MaxRetries        int
	MaxWorkers        int
	MaxActiveHandlers int
	RetryDelay        time.Duration
	ProcessTimeout    time.Duration
	Logger            Logger
	Tracer            Tracer
}

func getDefaultOptions() *Options {
	return &Options{
		BufferSize:        24,
		MaxRetries:        3,
		MaxWorkers:        20,
		MaxActiveHandlers: 40,
		RetryDelay:        5 * time.Second,
		ProcessTimeout:    10 * time.Second,
		Logger:            &DefaultLogger{},
		Tracer:            &DefaultTracer{},
	}
}
