package dashboard

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/weichen-lin/kabaka"
	"github.com/weichen-lin/kabaka/dashboard/ws"
)

//go:embed dist/*
var frontendFS embed.FS

// Server is the dashboard HTTP server
type Server struct {
	kabaka *kabaka.Kabaka
	config Config
	mux    *http.ServeMux
	server *http.Server
	wsHub  *ws.Hub

	// Life cycle control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	startTime time.Time
}

// NewServer creates a new dashboard server instance
func NewServer(k *kabaka.Kabaka, cfg Config) *Server {
	s := &Server{
		kabaka:    k,
		config:    cfg,
		mux:       http.NewServeMux(),
		wsHub:     ws.NewHub(),
		startTime: time.Now(),
	}

	s.setupRoutes()

	return s
}

// Start starts the dashboard server
func (s *Server) Start(addr string) error {
	s.ctx, s.cancel = context.WithCancel(context.Background())

	// Start WS Hub
	go s.wsHub.Run(s.ctx)

	// Start Stats Broadcaster
	go s.broadcastStats()

	s.server = &http.Server{
		Addr:    addr,
		Handler: s.applyMiddleware(s.mux),
	}

	fmt.Printf("🚀 Kabaka Dashboard starting on http://%s\n", addr)
	return s.server.ListenAndServe()
}

// Stop stops the dashboard server gracefully
func (s *Server) Stop(ctx context.Context) error {
	if s.cancel != nil {
		s.cancel()
	}
	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

// broadcastStats periodically pushes metrics via websocket
func (s *Server) broadcastStats() {
	ticker := time.NewTicker(s.config.StatsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			stats := s.kabaka.GetStats()

			// Add System-level runtime stats
			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			s.wsHub.Broadcast(ws.Message{
				Type:      "stats",
				Channel:   "all",
				Timestamp: time.Now().Unix(),
				Data: map[string]interface{}{
					"stats": stats,
					"system": map[string]interface{}{
						"goroutines": runtime.NumGoroutine(),
						"memory":     fmt.Sprintf("%.2f MB", float64(m.Alloc)/1024/1024),
						"go_version": runtime.Version(),
						"num_cpu":    runtime.NumCPU(),
					},
					"uptime": int64(time.Since(s.startTime).Seconds()),
				},
			})
		}
	}
}

// setupRoutes initializes all dashboard routes
func (s *Server) setupRoutes() {
	// API Routes
	s.mux.HandleFunc("GET /api/v1/health", s.handleHealth)
	s.mux.HandleFunc("GET /api/v1/stats", s.handleStats)
	s.mux.HandleFunc("GET /api/v1/topics", s.handleTopics)

	// WebSocket Route
	s.mux.HandleFunc("GET /api/v1/ws", func(w http.ResponseWriter, r *http.Request) {
		ws.ServeWs(s.wsHub, w, r)
	})

	// Static files
	s.setupFrontendRoutes()
}

// setupFrontendRoutes sets up static file serving from embedded dist folder
func (s *Server) setupFrontendRoutes() {
	distFS, _ := fs.Sub(frontendFS, "dist")
	fileServer := http.FileServer(http.FS(distFS))

	s.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// 1. Skip API routes
		if strings.HasPrefix(r.URL.Path, "/api/") {
			http.NotFound(w, r)
			return
		}

		// 2. Clean path
		path := strings.TrimPrefix(r.URL.Path, "/")

		// 3. If root or index.html is requested, serve it directly
		if path == "" || path == "index.html" {
			indexData, err := fs.ReadFile(distFS, "index.html")
			if err != nil {
				http.Error(w, "Index not found", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			w.Write(indexData)
			return
		}

		// 4. Check if the specific file exists (for assets like .js, .css, .png)
		f, err := distFS.Open(path)
		if err != nil {
			// 5. SPA fallback: If file not found, serve index.html
			indexData, err := fs.ReadFile(distFS, "index.html")
			if err != nil {
				http.Error(w, "SPA fallback failed", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			w.Write(indexData)
			return
		}
		f.Close()

		// 6. Serve the static asset
		fileServer.ServeHTTP(w, r)
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := s.kabaka.GetStats()

	// Add System-level runtime stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	response := map[string]interface{}{
		"stats": stats,
		"system": map[string]interface{}{
			"goroutines": runtime.NumGoroutine(),
			"memory":     fmt.Sprintf("%.2f MB", float64(m.Alloc)/1024/1024),
			"go_version": runtime.Version(),
			"num_cpu":    runtime.NumCPU(),
		},
		"timestamp": time.Now().Unix(),
		"uptime":    int64(time.Since(s.startTime).Seconds()),
	}

	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleTopics(w http.ResponseWriter, r *http.Request) {
	stats := s.kabaka.GetStats()

	// Transform map to slice for frontend consistency
	topics := make([]map[string]interface{}, 0, len(stats.Topics))
	for name, snapshot := range stats.Topics {
		// Calculate success rate
		var successRate float64 = 100
		if snapshot.ProcessedTotal > 0 {
			successRate = float64(snapshot.ProcessedTotal-snapshot.FailedTotal) / float64(snapshot.ProcessedTotal) * 100
		}

		topics = append(topics, map[string]interface{}{
			"name":           name,
			"processedTotal": snapshot.ProcessedTotal,
			"failedTotal":    snapshot.FailedTotal,
			"retryTotal":     snapshot.RetryTotal,
			"avgDuration":    snapshot.AvgDuration.Milliseconds(),
			"queueStats": map[string]int64{
				"pending":    snapshot.QueuePending,
				"delayed":    snapshot.QueueDelayed,
				"processing": snapshot.QueueProcessing,
			},
			"successRate": fmt.Sprintf("%.2f", successRate),
		})
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"topics": topics,
		"total":  len(topics),
	})
}

func (s *Server) writeJSON(w http.ResponseWriter, code int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(data)
}

// applyMiddleware applies CORS, Auth, etc.
func (s *Server) applyMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simple CORS for now
		if s.config.EnableCORS {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		}

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		h.ServeHTTP(w, r)
	})
}

// Handler returns the http.Handler for the dashboard
func (s *Server) Handler() http.Handler {
	return s.applyMiddleware(s.mux)
}
