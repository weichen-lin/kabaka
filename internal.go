package kabaka

import (
	"crypto/sha1"
	"encoding/hex"
)

// generateInternalName generates a SHA1 hash for the topic name.
func (k *Kabaka) generateInternalName(name string) string {
	h := sha1.New()
	h.Write([]byte(name))
	return hex.EncodeToString(h.Sum(nil))
}
