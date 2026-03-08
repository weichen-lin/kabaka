package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/weichen-lin/kabaka"
	"github.com/weichen-lin/kabaka/broker"
	"github.com/weichen-lin/kabaka/dashboard"
)

func main() {
	// 1. Initialize Kabaka with a visible worker pool
	k := kabaka.NewKabaka(
		kabaka.WithBroker(broker.NewMemoryBroker()),
		kabaka.WithMaxWorkers(20),
	)

	// 2. Heavy Handler: Takes 1-3 seconds to process
	k.CreateTopic("heavy.task", func(ctx context.Context, msg *broker.Message) error {
		// Long processing time to observe Active Workers
		delay := time.Duration(1000+rand.Intn(2000)) * time.Millisecond
		time.Sleep(delay)

		if rand.Float32() < 0.15 {
			return fmt.Errorf("simulated processing failure")
		}
		return nil
	})

	// 3. Fast Handler: Small noise
	k.CreateTopic("fast.event", func(ctx context.Context, msg *broker.Message) error {
		delay := time.Duration(50+rand.Intn(200)) * time.Millisecond
		time.Sleep(delay)
		if rand.Float32() < 0.05 {
			return fmt.Errorf("transient error")
		}
		return nil
	})

	// 4. Unstable Handler: High error rate, No retries for observation
	k.CreateTopic("unstable.api", func(ctx context.Context, msg *broker.Message) error {
		delay := time.Duration(300+rand.Intn(1200)) * time.Millisecond
		time.Sleep(delay)
		if rand.Float32() < 0.4 {
			return fmt.Errorf("external api timeout")
		}
		return nil
	}, kabaka.WithMaxRetries(0))

	// 6. Schema Handler: Validated input example
	userSchema := `{
		"type": "object",
		"required": ["user_id", "email"],
		"properties": {
			"user_id": { "type": "string", "title": "User ID", "pattern": "^USR-[0-9]+$" },
			"email": { "type": "string", "title": "Email Address", "format": "email" },
			"plan": { 
				"type": "string", 
				"title": "Subscription Plan",
				"enum": ["basic", "pro", "enterprise"],
				"default": "basic"
			},
			"marketing": { "type": "boolean", "title": "Opt-in Marketing", "default": true }
		}
	}`
	k.CreateTopic("user.signup", func(ctx context.Context, msg *broker.Message) error {
		delay := time.Duration(200+rand.Intn(800)) * time.Millisecond
		time.Sleep(delay)
		return nil
	}, kabaka.WithSchema(userSchema))

	// 7. Complex Form Handler: Multi-type validation
	complexSchema := `{
		"title": "Industrial_Protocol_Config",
		"description": "Configure advanced message parameters for high-priority payloads",
		"type": "object",
		"required": ["node_id", "priority", "payload"],
		"properties": {
			"node_id": {
				"type": "string",
				"title": "Node Identifier",
				"description": "The target physical node cluster ID",
				"default": "NODE-01"
			},
			"priority": {
				"type": "integer",
				"title": "Execution Priority",
				"minimum": 0,
				"maximum": 100,
				"default": 50
			},
			"payload": {
				"type": "string",
				"title": "Message Payload",
				"description": "The main data content of the message",
				"default": "SYNC_DATA_001"
			},
			"comment": {
				"type": "string",
				"title": "Additional Comment",
				"description": "Optional notes for the audit log"
			},
			"tags": {
				"type": "array",
				"title": "Classification Tags",
				"items": {
					"type": "string"
				},
				"default": ["urgent", "telemetry"]
			},
			"schedule": {
				"type": "object",
				"title": "Dispatch Schedule",
				"properties": {
					"start_at": {
						"type": "string",
						"format": "date-time",
						"title": "Deployment Window Start"
					},
					"retry_strategy": {
						"type": "string",
						"enum": ["exponential", "fixed", "immediate"],
						"default": "fixed"
					}
				}
			},
			"advanced": {
				"type": "object",
				"title": "Hardware_Acceleration",
				"properties": {
					"use_gpu": { "type": "boolean", "title": "GPU Acceleration", "default": false },
					"threads": { "type": "integer", "title": "Thread Count", "minimum": 1, "maximum": 64, "default": 4 }
				}
			}
		}
	}`
	k.CreateTopic("complex.form", func(ctx context.Context, msg *broker.Message) error {
		time.Sleep(500 * time.Millisecond)
		return nil
	}, kabaka.WithSchema(complexSchema))

	// 8. Struct-based Schema Handler
	type OrderPayload struct {
		OrderID  string   `json:"order_id" jsonschema:"title=Order ID,pattern=^ORD-[0-9]+$"`
		Amount   float64  `json:"amount" jsonschema:"title=Total Amount,minimum=0.01"`
		Items    []string `json:"items" jsonschema:"title=Item List"`
		Priority string   `json:"priority" jsonschema:"title=Order Priority,enum=high,enum=medium,enum=low,default=medium"`
		Notify   bool     `json:"notify" jsonschema:"title=Send Notification,default=true"`
	}

	k.CreateTopic("struct.form", func(ctx context.Context, msg *broker.Message) error {
		fmt.Printf("Received Order: %s\n", string(msg.Value))
		return nil
	}, kabaka.WithSchema(OrderPayload{}))

	k.Start()
	defer k.Close()

	// 7. Traffic Generator: Multi-Topic Burst Mode
	go func() {
		for {
			// Heavy Burst: Send 25 messages
			for i := 0; i < 25; i++ {
				k.Publish("heavy.task", []byte(fmt.Sprintf(`{"id": %d, "timestamp": %d}`, i, time.Now().Unix())))
			}

			// Schema-validated messages
			for i := 0; i < 5; i++ {
				k.Publish("user.signup", []byte(fmt.Sprintf(`{"user_id": "USR-%d", "email": "user%d@example.com", "plan": "pro"}`, i, i)))
			}

			// Fast Noise
			for i := 0; i < 15; i++ {
				k.Publish("fast.event", []byte(`{"type": "ping"}`))
			}

			// Unstable traffic
			for i := 0; i < 10; i++ {
				k.Publish("unstable.api", []byte(`{"action": "sync"}`))
			}

			// Occasional cleanup
			if rand.Intn(3) == 0 {
				k.Publish("cleanup.worker", []byte(`{"target": "all"}`))
			}

			time.Sleep(5 * time.Second)
		}
	}()

	// 7. Start Dashboard
	dashboardAddr := "0.0.0.0:8787"
	dashServer, err := dashboard.StartEmbeddedAsync(k, dashboardAddr)
	if err != nil {
		fmt.Printf("Failed to start dashboard: %v\n", err)
		return
	}

	fmt.Printf("\n✨ Kabaka Observable Test App Running!\n")
	fmt.Printf("📊 Dashboard: http://127.0.0.1:3000\n")
	fmt.Printf("💡 Multiple topics are now processing with randomized delays and errors.\n")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	dashServer.Stop(context.Background())
}
