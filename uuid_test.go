package kabaka

import (
	"regexp"
	"testing"
)

func TestNewUUID(t *testing.T) {
	// Test UUID generation
	uuid := NewUUID()

	// UUID format: xxxxxxxx-xxxx-4xxx-8xxx-xxxxxxxxxxxx
	uuidPattern := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	if !uuidPattern.MatchString(uuid) {
		t.Errorf("UUID %s does not match expected format", uuid)
	}

	// Test that version is 4
	if uuid[14] != '4' {
		t.Errorf("UUID version should be 4, got %c", uuid[14])
	}

	// Test that variant is RFC 4122 (8, 9, a, or b)
	variantChar := uuid[19]
	if variantChar != '8' && variantChar != '9' && variantChar != 'a' && variantChar != 'b' {
		t.Errorf("UUID variant should be 8, 9, a, or b, got %c", variantChar)
	}
}

func TestNewUUID_Uniqueness(t *testing.T) {
	// Generate multiple UUIDs and ensure they're unique
	uuids := make(map[string]bool)
	count := 1000

	for i := 0; i < count; i++ {
		uuid := NewUUID()
		if uuids[uuid] {
			t.Errorf("Duplicate UUID generated: %s", uuid)
		}
		uuids[uuid] = true
	}

	if len(uuids) != count {
		t.Errorf("Expected %d unique UUIDs, got %d", count, len(uuids))
	}
}

func TestNewUUID_Length(t *testing.T) {
	uuid := NewUUID()

	// UUID format with hyphens: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx = 36 characters
	expectedLength := 36
	if len(uuid) != expectedLength {
		t.Errorf("Expected UUID length %d, got %d", expectedLength, len(uuid))
	}
}

func TestNewUUID_Format(t *testing.T) {
	uuid := NewUUID()

	// Check hyphen positions (indices 8, 13, 18, 23)
	if uuid[8] != '-' || uuid[13] != '-' || uuid[18] != '-' || uuid[23] != '-' {
		t.Errorf("UUID %s has incorrect hyphen positions", uuid)
	}

	// Check all other characters are hexadecimal
	hexPattern := regexp.MustCompile(`^[0-9a-f-]+$`)
	if !hexPattern.MatchString(uuid) {
		t.Errorf("UUID %s contains non-hexadecimal characters", uuid)
	}
}
