package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/weichen-lin/kabaka"
)

// emailHandler is a simple handler function that simulates processing a message.
// It logs the received message and simulates work with a short delay.
func emailHandler(ctx context.Context, msg *kabaka.Message) error {
	log.Printf("📧 Processing message ID: %s, Body: %s\n", msg.ID, string(msg.Value))

	// Simulate some work, e.g., sending an email
	select {
	case <-time.After(1 * time.Second):
		// Work done
		log.Printf("✅ Successfully processed message ID: %s\n", msg.ID)
		return nil
	case <-ctx.Done():
		// Context was cancelled, e.g., due to timeout or shutdown
		log.Printf("⚠️ Processing cancelled for message ID: %s, Error: %v\n", msg.ID, ctx.Err())
		return ctx.Err()
	}
}


func main() {
	// 1. Initialize Kabaka
	k := kabaka.NewKabaka()
	log.Println("Kabaka instance created.")

	// 2. Define a handler for our topic
	// This function will be executed by workers for each message.
	paymentHandler := func(ctx context.Context, msg *kabaka.Message) error {
		log.Printf("💳 Processing payment message ID: %s, Body: %s\n", msg.ID, string(msg.Value))

		// Simulate a task that might fail
		if string(msg.Value) == "payment_4" {
			log.Printf("❌ Failed to process payment ID: %s. Simulating an error.\n", msg.ID)
			return fmt.Errorf("invalid payment data for message %s", msg.ID)
		}

		// Simulate work
		time.Sleep(500 * time.Millisecond)

		log.Printf("✅ Successfully processed payment ID: %s\n", msg.ID)
		return nil
	}

	// 3. Create a Topic with custom options.
	// We are setting up a topic with 5 workers.
	err := k.CreateTopic(
		"payment-service",
		paymentHandler,
		kabaka.WithMaxWorkers(5),
		kabaka.WithMaxRetries(2), // Set max retries to 2
	)
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}
	log.Println("Topic 'payment-service' created with 5 workers.")

	// You can create multiple topics with different handlers
	err = k.CreateTopic("email-service", emailHandler, kabaka.WithMaxWorkers(3))
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}
	log.Println("Topic 'email-service' created with 3 workers.")

	// 4. Publish messages to the topics in a separate goroutine.
	go func() {
		for i := 0; i < 10; i++ {

			paymentMsg := fmt.Sprintf("payment_%d", i)
			err := k.Publish("payment-service", []byte(paymentMsg))
			if err != nil {
				log.Printf("Failed to publish to payment-service: %v", err)
			} else {
				log.Printf("Published message to payment-service: %s", paymentMsg)
			}

			emailMsg := fmt.Sprintf("email_notification_%d", i)
			err = k.Publish("email-service", []byte(emailMsg))
			if err != nil {
				log.Printf("Failed to publish to email-service: %v", err)
			} else {
				log.Printf("Published message to email-service: %s", emailMsg)
			}

			time.Sleep(200 * time.Millisecond)
		}
	}()

	// 5. Monitor metrics in another goroutine
	metricsCtx, cancelMetrics := context.WithCancel(context.Background())
	defer cancelMetrics()
	go func() {
		for {
			select {
			case <-time.After(3 * time.Second):
				metrics := k.GetMetrics()
				log.Println("--- Kabaka Metrics ---")
				for _, m := range metrics {
					log.Printf(
						"Topic: %s | Active Workers: %d | Busy Workers: %d | Ongoing Jobs: %d",
						m.TopicName, m.ActiveWorkers, m.BusyWorkers, m.OnGoingJobs,
					)
				}
				log.Println("----------------------")
			case <-metricsCtx.Done():
				return
			}
		}
	}()

	// 6. Wait for a shutdown signal for graceful shutdown.
	log.Println("Application started. Press Ctrl+C to shut down.")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	// 7. Gracefully shut down Kabaka
	log.Println("Shutting down server...")
	cancelMetrics() // Stop the metrics goroutine

	// Close the specific topic if you want.
	// k.CloseTopic("payment-service")

	// Close all topics and workers
	if err := k.Close(); err != nil {
		log.Fatalf("Failed to gracefully shut down Kabaka: %v", err)
	}

	// A short delay to allow logs to be printed before exit.
	time.Sleep(1 * time.Second)
	log.Println("Server gracefully stopped.")
}
