package kabaka

import (
	"crypto/sha256"
	"encoding/hex"
)

// generateInternalName generates a SHA256 hash for the topic name.
func (k *Kabaka) generateInternalName(name string) string {
	h := sha256.New()
	h.Write([]byte(name))
	return hex.EncodeToString(h.Sum(nil))
}
