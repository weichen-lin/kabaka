package dashboard

import "time"

// Config Dashboard configuration options
type Config struct {
	// Base configuration
	EnableCORS     bool     // Whether to enable CORS
	AllowedOrigins []string // Allowed origins for CORS

	// Feature flags
	EnableAuth bool   // Whether to enable authentication
	AuthToken  string // API Token for authentication

	// Performance and intervals
	StatsInterval  time.Duration // Interval for collecting stats
	LogsBufferSize int           // Size of the logs buffer
	MaxConnections int           // Maximum WebSocket connections

	// UI configuration
	Title string // Dashboard title
	Theme string // Default theme (dark/light)

	// Advanced configuration
	ReadTimeout  time.Duration // HTTP read timeout
	WriteTimeout time.Duration // HTTP write timeout
	IdleTimeout  time.Duration // HTTP idle timeout
}

// DefaultConfig returns the default configuration for Dashboard
func DefaultConfig() Config {
	return Config{
		EnableCORS:     true,
		AllowedOrigins: []string{"*"},
		EnableAuth:     false,
		StatsInterval:  1 * time.Second,
		LogsBufferSize: 1000,
		MaxConnections: 100,
		Title:          "Kabaka Dashboard",
		Theme:          "dark",
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		IdleTimeout:    60 * time.Second,
	}
}

// Option is a function type for configuring the Dashboard
type Option func(*Config)

// WithAuth enables authentication with the given token
func WithAuth(token string) Option {
	return func(c *Config) {
		c.EnableAuth = true
		c.AuthToken = token
	}
}

// WithStatsInterval sets the interval for stats collection
func WithStatsInterval(d time.Duration) Option {
	return func(c *Config) {
		c.StatsInterval = d
	}
}

// WithTitle sets the dashboard title
func WithTitle(title string) Option {
	return func(c *Config) {
		c.Title = title
	}
}

// WithCORS configures CORS origins
func WithCORS(origins ...string) Option {
	return func(c *Config) {
		c.EnableCORS = true
		c.AllowedOrigins = origins
	}
}
