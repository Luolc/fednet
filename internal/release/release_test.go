package release

import "testing"

func TestCompatible(t *testing.T) {
	tests := []struct {
		min, client, hub string
		ok               bool
	}{
		{"v0.1.0", "v0.1.0", "v0.2.0", true},
		{"v0.1.0", "v0.1.1", "v0.2.0", true},
		{"v0.1.0", "v1.0.0", "v0.2.0", true},
		{"v0.1.0", "v0.0.9", "v0.2.0", false},
		{"v0.10.0", "v0.9.0", "v0.10.0", false},
		{"v0.1.0", "dev", "v0.2.0", false},
		{"v0.1.0", "dev", "dev", true},
		{"v0.1.0", "v0.2.0", "dev", true},
		{"v0.1.0", "0.1.0", "v0.2.0", false},
		{"v0.1.0", "v0.1", "v0.2.0", false},
		{"v0.1.0", "v0.1.0-rc1", "v0.2.0", false},
		{"v0.1.0", "v0.01.0", "v0.2.0", false},
	}
	for _, tt := range tests {
		err := Compatible(tt.min, tt.client, tt.hub)
		if (err == nil) != tt.ok {
			t.Errorf("Compatible(%q, %q, %q) = %v, want ok=%v", tt.min, tt.client, tt.hub, err, tt.ok)
		}
	}
}
