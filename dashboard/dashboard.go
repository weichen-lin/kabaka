package dashboard

import (
	"fmt"
	"net/http"

	"github.com/weichen-lin/kabaka"
)

// StartEmbedded starts the dashboard in blocking mode.
func StartEmbedded(k *kabaka.Kabaka, addr string) error {
	srv := NewServer(k, DefaultConfig())
	return srv.Start(addr)
}

// StartEmbeddedAsync starts the dashboard in non-blocking mode.
func StartEmbeddedAsync(k *kabaka.Kabaka, addr string) (*Server, error) {
	srv := NewServer(k, DefaultConfig())

	go func() {
		if err := srv.Start(addr); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Dashboard server error: %v\n", err)
		}
	}()
	return srv, nil
}

// StartEmbeddedWithOptions starts the dashboard with custom options.
func StartEmbeddedWithOptions(k *kabaka.Kabaka, addr string, opts ...Option) error {
	cfg := DefaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	srv := NewServer(k, cfg)
	return srv.Start(addr)
}
