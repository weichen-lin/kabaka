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
	k.CreateTopic("heavy.task", func(ctx context.Context, msg *kabaka.Message) error {
		// Long processing time to observe Active Workers
		delay := time.Duration(1000+rand.Intn(2000)) * time.Millisecond
		time.Sleep(delay)

		if rand.Float32() < 0.15 {
			return fmt.Errorf("simulated processing failure")
		}
		return nil
	})

	// 3. Fast Handler: Small noise
	k.CreateTopic("fast.event", func(ctx context.Context, msg *kabaka.Message) error {
		delay := time.Duration(50+rand.Intn(200)) * time.Millisecond
		time.Sleep(delay)
		if rand.Float32() < 0.05 {
			return fmt.Errorf("transient error")
		}
		return nil
	})

	// 4. Unstable Handler: High error rate, No retries for observation
	k.CreateTopic("unstable.api", func(ctx context.Context, msg *kabaka.Message) error {
		delay := time.Duration(300+rand.Intn(1200)) * time.Millisecond
		time.Sleep(delay)
		if rand.Float32() < 0.4 {
			return fmt.Errorf("external api timeout")
		}
		return nil
	}, kabaka.WithMaxRetries(0))

	// 5. Cleanup Handler: Long-running background task
	k.CreateTopic("cleanup.worker", func(ctx context.Context, msg *kabaka.Message) error {
		delay := time.Duration(5000+rand.Intn(5000)) * time.Millisecond
		time.Sleep(delay)
		return nil
	})

	k.Start()
	defer k.Close()

	// 6. Traffic Generator: Multi-Topic Burst Mode
	go func() {
		for {
			// Heavy Burst: Send 25 messages
			for i := 0; i < 25; i++ {
				k.Publish("heavy.task", []byte(fmt.Sprintf(`{"id": %d, "timestamp": %d}`, i, time.Now().Unix())))
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
