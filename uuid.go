package kabaka

import (
	"crypto/rand"
	"fmt"
)

// NewUUID generates a new UUID version 4.
func NewUUID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}

	// xxxxxxxx-xxxx-4xxx-8xxx-xxxxxxxxxxxx

	b[6] = (b[6] & 0x0f) | 0x40
	// Set variant to RFC 4122
	b[8] = (b[8] & 0x3f) | 0x80

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
