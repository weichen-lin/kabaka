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
	s.mux.HandleFunc("GET /api/v1/topics/{name}", s.handleTopicDetail)
	s.mux.HandleFunc("GET /api/v1/topics/{name}/history", s.handleTopicHistory)
	s.mux.HandleFunc("POST /api/v1/topics/{name}/pause", s.handleTopicPause)
	s.mux.HandleFunc("POST /api/v1/topics/{name}/resume", s.handleTopicResume)
	s.mux.HandleFunc("POST /api/v1/topics/{name}/purge", s.handleTopicPurge)
	s.mux.HandleFunc("POST /api/v1/topics/{name}/publish", s.handleTopicPublish)
	s.mux.HandleFunc("GET /api/v1/instances", s.handleInstances)

	// WebSocket Route
	s.mux.HandleFunc("GET /api/v1/ws", func(w http.ResponseWriter, r *http.Request) {
		ws.ServeWs(s.wsHub, w, r)
	})

	// Static files
	s.setupFrontendRoutes()
}

func (s *Server) handleTopicPause(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.kabaka.SetTopicPaused(name, true); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "paused", "topic": name})
}

func (s *Server) handleTopicResume(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.kabaka.SetTopicPaused(name, false); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "resumed", "topic": name})
}

func (s *Server) handleTopicPurge(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	stats := s.kabaka.GetStats()
	snapshot, ok := stats.Topics[name]
	if !ok {
		http.Error(w, "Topic not found", http.StatusNotFound)
		return
	}

	if err := s.kabaka.PurgeTopic(name, snapshot.InternalName); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "purged", "topic": name})
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

func (s *Server) handleInstances(w http.ResponseWriter, r *http.Request) {
	instances := s.kabaka.GetInstances()
	if instances == nil {
		s.writeJSON(w, http.StatusOK, map[string]interface{}{
			"supported": false,
			"instances": []interface{}{},
			"total":     0,
		})
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"supported": true,
		"instances": instances,
		"total":     len(instances),
	})
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
		topics = append(topics, map[string]interface{}{
			"name":             name,
			"processed_total":  snapshot.ProcessedTotal,
			"failed_total":     snapshot.FailedTotal,
			"retry_total":      snapshot.RetryTotal,
			"avg_duration":     snapshot.AvgDurationMs,
			"queue_pending":    snapshot.QueuePending,
			"queue_delayed":    snapshot.QueueDelayed,
			"queue_processing": snapshot.QueueProcessing,
			"success_rate":     snapshot.SuccessRate,
			"paused":           snapshot.Paused,
			"max_retries":      snapshot.MaxRetries,
			"retry_delay":      snapshot.RetryDelay,
			"process_timeout":  snapshot.ProcessTimeout,
			"internal_name":    snapshot.InternalName,
		})
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"topics": topics,
		"total":  len(topics),
	})
}

func (s *Server) handleTopicDetail(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	stats := s.kabaka.GetStats()
	snapshot, ok := stats.Topics[name]
	if !ok {
		http.Error(w, "Topic not found", http.StatusNotFound)
		return
	}

	s.writeJSON(w, http.StatusOK, snapshot)
}

func (s *Server) handleTopicHistory(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	// Get limit from query parameter
	limit := 50 // default
	if lStr := r.URL.Query().Get("limit"); lStr != "" {
		fmt.Sscanf(lStr, "%d", &limit)
	}

	history, err := s.kabaka.GetTopicHistory(name, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"history": history,
		"total":   len(history),
	})
}

func (s *Server) handleTopicPublish(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	var payload interface{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	data, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, "Failed to encode payload", http.StatusInternalServerError)
		return
	}

	if err := s.kabaka.Publish(name, data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "published", "topic": name})
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
