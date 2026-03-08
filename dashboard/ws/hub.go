package ws

import (
	"context"
	"sync"
)

// Message is the WebSocket message format
type Message struct {
	Type      string      `json:"type"`      // stats, logs, etc.
	Channel   string      `json:"channel"`
	Timestamp int64       `json:"timestamp"`
	Data      interface{} `json:"data"`
}

// Hub manages all active WebSocket connections
type Hub struct {
	// Registered clients
	clients map[*Client]bool

	// Inbound messages from the clients
	broadcast chan Message

	// Register requests from the clients
	register chan *Client

	// Unregister requests from clients
	unregister chan *Client

	mu sync.RWMutex
}

func NewHub() *Hub {
	return &Hub{
		broadcast:  make(chan Message),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		clients:    make(map[*Client]bool),
	}
}

func (h *Hub) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			h.closeAll()
			return
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
			h.mu.Unlock()
		case message := <-h.broadcast:
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					// If client buffer is full, drop connection
					close(client.send)
					delete(h.clients, client)
				}
			}
			h.mu.RUnlock()
		}
	}
}

func (h *Hub) Broadcast(msg Message) {
	select {
	case h.broadcast <- msg:
	default:
		// Drop if busy
	}
}

func (h *Hub) closeAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for client := range h.clients {
		close(client.send)
		delete(h.clients, client)
	}
}
