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
		kabaka.WithMaxWorkers(50),
	)

	// 2. Heavy Handler: Takes 1-3 seconds to process
	k.CreateTopic("heavy.task", func(ctx context.Context, msg *kabaka.Message) error {
		// Long processing time to observe Active Workers
		delay := time.Duration(1000+rand.Intn(2000)) * time.Millisecond
		time.Sleep(delay)
		
		if rand.Float32() < 0.1 {
			return fmt.Errorf("simulated processing failure")
		}
		return nil
	})

	// 3. Fast Handler: Small noise
	k.CreateTopic("fast.event", func(ctx context.Context, msg *kabaka.Message) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	})

	k.Start()
	defer k.Close()

	// 4. Traffic Generator: Burst Mode
	go func() {
		for {
			// Burst: Send 25 messages at once every 5 seconds
			fmt.Println("🚀 Dispatching burst of tasks...")
			for i := 0; i < 25; i++ {
				k.Publish("heavy.task", []byte(fmt.Sprintf(`{"id": %d}`, i)))
			}
			
			// Constant noise
			for i := 0; i < 5; i++ {
				k.Publish("fast.event", []byte(`{"type": "ping"}`))
			}
			
			time.Sleep(5 * time.Second)
		}
	}()

	// 5. Start Dashboard
	dashboardAddr := "0.0.0.0:3000"
	dashServer, err := dashboard.StartEmbeddedAsync(k, dashboardAddr)
	if err != nil {
		fmt.Printf("Failed to start dashboard: %v\n", err)
		return
	}

	fmt.Printf("\n✨ Kabaka Observable Test App Running!\n")
	fmt.Printf("📊 Dashboard: http://127.0.0.1:3000\n")
	fmt.Printf("💡 Watch the 'Worker Fleet Status' dots light up during bursts!\n")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	dashServer.Stop(context.Background())
}
