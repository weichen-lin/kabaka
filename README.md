# Kabaka

<p align="center">
  <img src="dashboard/frontend/public/icon.svg" width="128" height="128" alt="Kabaka Logo">
</p>

**Kabaka** is a high-performance, observable message queue and task distribution system for Go. It features a modern, real-time dashboard for monitoring and managing your message flow with a focus on developer experience and system transparency.

## 🚀 Features

- **Multi-Broker Support**:
  - **Redis**: Persistent, distributed production workloads with Lua-based atomic operations and reliable queue pattern.
  - **In-Memory**: Lightning-fast development and testing with background cleanup.
- **JSON Schema Validation**: Built-in validation using JSON Schema for both Go structs (via reflection) and raw JSON strings.
- **Task Scheduling**: Immediate publishing, delayed publishing with arbitrary duration, and automatic retries with exponential backoff.
- **Audit Trail (History)**: Per-topic execution history with configurable LRU limits — records payload, status, duration, attempts, and errors.
- **Pause / Resume / Purge**: Dynamically pause message consumption per topic, resume it, or purge all pending and delayed messages.
- **Concurrency Control**: Global worker pool with semaphore-based limiting across all topics.
- **Modern Dashboard**: Real-time UI built with React, Framer Motion, and Tailwind CSS — includes WebSocket-powered live stats, topic management, and schema-based publish forms.
- **Graceful Shutdown**: Waits for all in-flight jobs to complete before closing.

## 📦 Installation

```bash
go get github.com/weichen-lin/kabaka
```

## ⚡ Quick Start

```go
package main

import (
	"context"
	"fmt"

	"github.com/weichen-lin/kabaka"
	"github.com/weichen-lin/kabaka/broker"
	"github.com/weichen-lin/kabaka/dashboard"
)

func main() {
	// 1. Initialize Kabaka
	k := kabaka.NewKabaka(
		kabaka.WithMaxWorkers(10),
		kabaka.WithBroker(broker.NewMemoryBroker()), // Use Redis for production
	)

	// 2. Define a Topic with Schema Validation
	userSchema := `{
		"type": "object",
		"required": ["user_id", "email"],
		"properties": {
			"user_id": { "type": "string" },
			"email": { "type": "string", "format": "email" }
		}
	}`

	k.CreateTopic("user.signup", func(ctx context.Context, msg *broker.Message) error {
		fmt.Printf("Processing signup for: %s\n", string(msg.Value))
		return nil
	},
		kabaka.WithSchema(userSchema),
		kabaka.WithMaxRetries(5),
		kabaka.WithHistoryLimit(100),
	)

	// 3. Start Processing
	k.Start()
	defer k.Close()

	// 4. Start the Dashboard
	dashboard.StartEmbeddedAsync(k, "0.0.0.0:8787")

	// 5. Publish Messages
	k.Publish("user.signup", []byte(`{"user_id": "USR-001", "email": "dev@example.com"}`))

	// Keep the process running
	select {}
}
```

## ⚙️ Configuration

### Global Options

Pass these to `kabaka.NewKabaka(...)`:

| Option                 | Description                              | Default         |
| ---------------------- | ---------------------------------------- | --------------- |
| `WithBroker(b)`        | Set the message broker (Memory or Redis) | In-Memory       |
| `WithMaxWorkers(n)`    | Maximum concurrent worker goroutines     | `10`            |
| `WithLogger(l)`        | Custom logger implementation             | `DefaultLogger` |
| `WithBrokerTimeout(d)` | Timeout for broker operations            | `2s`            |
| `WithMetaCacheTTL(d)`  | TTL for topic metadata cache entries     | `5m`            |

### Per-Topic Options

Pass these to `k.CreateTopic(...)`:

| Option                  | Description                                           | Default |
| ----------------------- | ----------------------------------------------------- | ------- |
| `WithMaxRetries(n)`     | Maximum retry attempts before marking as dead         | `3`     |
| `WithRetryDelay(d)`     | Base delay for exponential backoff retries            | `1s`    |
| `WithProcessTimeout(d)` | Execution timeout for the handler function            | `30s`   |
| `WithHistoryLimit(n)`   | Number of audit records to keep (0 = disabled)        | `0`     |
| `WithSchema(v)`         | JSON Schema for payload validation (string or struct) | none    |

## 📡 Core API

```go
// Topic management
k.CreateTopic(name, handler, options...)

// Publishing
k.Publish(topicName, payload)
k.PublishDelayed(topicName, payload, delay)

// Topic control
k.SetTopicPaused(name, true)   // pause consumption
k.SetTopicPaused(name, false)  // resume consumption
k.PurgeTopic(name, internalName)

// Audit trail
k.GetTopicHistory(name, limit)

// Metrics
k.GetStats()

// Lifecycle
k.Start()
k.Close()
```

## 📊 Real-time Dashboard

Kabaka comes with a built-in dashboard accessible at the configured address. It provides:

- **System Overview**: Goroutine count, memory usage, CPU count, uptime, and worker fleet allocation.
- **Topic Registry**: Per-topic stats including Processed, Failed, Retries, Success Rate, and Avg Duration.
- **Queue Status**: Live Pending / Delayed / Processing counts per topic.
- **Interactive Management**: Pause, resume, and purge queues directly from the UI.
- **Schema Forms**: Publish messages through auto-generated forms based on your JSON Schema.
- **Audit Trail Viewer**: Browse execution history with payload, status, duration, and error details.
- **WebSocket Live Updates**: Stats refresh in real-time without polling.

### Starting the Dashboard

```go
// Non-blocking (recommended)
dashboard.StartEmbeddedAsync(k, "0.0.0.0:8787")

// Blocking
dashboard.StartEmbedded(k, "0.0.0.0:8787")

// With custom options
dashboard.StartEmbeddedWithOptions(k, "0.0.0.0:8787",
    dashboard.WithAuth("my-secret-token"),
    dashboard.WithStatsInterval(2 * time.Second),
    dashboard.WithCORS("https://example.com"),
    dashboard.WithTitle("My App Dashboard"),
)
```

### Dashboard Options

| Option                 | Description                     | Default              |
| ---------------------- | ------------------------------- | -------------------- |
| `WithAuth(token)`      | Enable API token authentication | disabled             |
| `WithStatsInterval(d)` | Stats broadcast interval        | `1s`                 |
| `WithCORS(origins...)` | Allowed CORS origins            | `*`                  |
| `WithTitle(title)`     | Dashboard page title            | `"Kabaka Dashboard"` |

### REST API Endpoints

| Method | Path                            | Description                   |
| ------ | ------------------------------- | ----------------------------- |
| `GET`  | `/api/v1/health`                | Health check                  |
| `GET`  | `/api/v1/stats`                 | System-wide metrics           |
| `GET`  | `/api/v1/topics`                | List all topics               |
| `GET`  | `/api/v1/topics/{name}`         | Topic detail                  |
| `GET`  | `/api/v1/topics/{name}/history` | Audit trail                   |
| `POST` | `/api/v1/topics/{name}/pause`   | Pause a topic                 |
| `POST` | `/api/v1/topics/{name}/resume`  | Resume a topic                |
| `POST` | `/api/v1/topics/{name}/purge`   | Purge queues                  |
| `POST` | `/api/v1/topics/{name}/publish` | Manually publish a message    |
| `GET`  | `/api/v1/ws`                    | WebSocket for real-time stats |

## 🛠 Brokers

### In-Memory

Ideal for local development or CI/CD pipelines. Includes background goroutines for delayed message scheduling and stale processing cleanup.

```go
kabaka.WithBroker(broker.NewMemoryBroker())
```

### Redis

Production-ready broker with persistence and high availability. Uses Lua scripts for atomic queue operations (reliable queue pattern) and sorted sets for delayed message scheduling.

```go
kabaka.WithBroker(broker.NewRedisBroker(&redis.Options{
    Addr: "localhost:6379",
}))
```

## 🔄 Retry & Backoff

When a handler returns an error, Kabaka automatically retries with **exponential backoff**:

```
Attempt 1 fails → wait retryDelay × 2⁰
Attempt 2 fails → wait retryDelay × 2¹
Attempt 3 fails → wait retryDelay × 2²
...
All retries exhausted → marked as "dead", recorded in audit trail
```

## 📝 License

Distributed under the MIT License. See `LICENSE` for more information.
