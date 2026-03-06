package kabaka

import "time"

// WithBroker sets the broker for Kabaka.
func WithBroker(broker Broker) KabakaOption {
	return func(k *Kabaka) {
		k.broker = broker
	}
}

// WithLogger sets the logger for Kabaka.
// If nil is passed, a NoOpLogger will be used instead.
func WithLogger(logger Logger) KabakaOption {
	return func(k *Kabaka) {
		if logger == nil {
			k.logger = &NoOpLogger{}
		} else {
			k.logger = logger
		}
	}
}

// WithMaxWorkers sets the maximum number of concurrent workers.
func WithMaxWorkers(n int) KabakaOption {
	return func(k *Kabaka) {
		k.maxWorkers = n
	}
}

// WithBrokerTimeout sets the timeout for broker operations.
func WithBrokerTimeout(d time.Duration) KabakaOption {
	return func(k *Kabaka) {
		k.brokerTimeout = d
	}
}

// WithMetaCacheTTL sets the TTL for metadata cache entries.
func WithMetaCacheTTL(ttl time.Duration) KabakaOption {
	return func(k *Kabaka) {
		k.metaCacheTTL = ttl
	}
}
