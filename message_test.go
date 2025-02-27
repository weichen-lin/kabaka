package kabaka

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestMessage(t *testing.T) {
	id := uuid.New()
	createdAt := time.Now()
	message := &Message{
		ID:        id,
		Value:     []byte("hello"),
		Retry:     3,
		CreatedAt: createdAt,
		Headers:   map[string]string{"Content-Type": "application/json"},
	}

	t.Run("Get existing header", func(t *testing.T) {
		require.Equal(t, "application/json", message.Get("Content-Type"))
	})

	t.Run("Get non-existing header", func(t *testing.T) {
		require.Equal(t, "", message.Get("Authorization"))
	})

	t.Run("Set header", func(t *testing.T) {
		message.Set("Authorization", "Bearer token")
		require.Equal(t, "Bearer token", message.Get("Authorization"))
	})
}
