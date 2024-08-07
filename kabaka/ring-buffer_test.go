package kabaka

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRingBuffer(t *testing.T) {
	t.Run("NewRingBuffer", func(t *testing.T) {
		rb := NewRingBuffer(5)
		require.NotNil(t, rb)
		require.Equal(t, 5, rb.size)
		require.Equal(t, 0, rb.count)
	})

	t.Run("Push and Pop", func(t *testing.T) {
		rb := NewRingBuffer(3)
		msg1 := &Message{} // 假設 Message 結構已定義
		msg2 := &Message{}
		msg3 := &Message{}

		// 測試 Push
		require.True(t, rb.Push(msg1))
		require.True(t, rb.Push(msg2))
		require.True(t, rb.Push(msg3))

		// 測試 Pop
		require.Equal(t, msg1, rb.Pop())
		require.Equal(t, msg2, rb.Pop())
		require.Equal(t, msg3, rb.Pop())
	})

	t.Run("Full Buffer", func(t *testing.T) {
		rb := NewRingBuffer(2)
		msg1 := &Message{}
		msg2 := &Message{}

		require.True(t, rb.Push(msg1))
		require.True(t, rb.Push(msg2))

		// 啟動一個 goroutine 來 Push，它應該會被阻塞
		pushed := make(chan bool)
		go func() {
			pushed <- rb.Push(&Message{})
		}()

		// 確保 Push 被阻塞
		select {
		case <-pushed:
			t.Fatal("Push should block when buffer is full")
		case <-time.After(100 * time.Millisecond):
			// 這是預期的行為
		}

		// Pop 一個消息，應該允許被阻塞的 Push 完成
		rb.Pop()
		require.True(t, <-pushed)
	})

	t.Run("Empty Buffer", func(t *testing.T) {
		rb := NewRingBuffer(2)

		// 啟動一個 goroutine 來 Pop，它應該會被阻塞
		popped := make(chan *Message)
		go func() {
			popped <- rb.Pop()
		}()

		// 確保 Pop 被阻塞
		select {
		case <-popped:
			t.Fatal("Pop should block when buffer is empty")
		case <-time.After(100 * time.Millisecond):
			// 這是預期的行為
		}

		// Push 一個消息，應該允許被阻塞的 Pop 完成
		msg := &Message{}
		rb.Push(msg)
		require.Equal(t, msg, <-popped)
	})

	t.Run("Circular Behavior", func(t *testing.T) {
		rb := NewRingBuffer(3)
		msg1 := &Message{}
		msg2 := &Message{}
		msg3 := &Message{}
		msg4 := &Message{}

		rb.Push(msg1)
		rb.Push(msg2)
		rb.Push(msg3)

		require.Equal(t, msg1, rb.Pop())
		require.True(t, rb.Push(msg4))

		require.Equal(t, msg2, rb.Pop())
		require.Equal(t, msg3, rb.Pop())
		require.Equal(t, msg4, rb.Pop())
	})
}
