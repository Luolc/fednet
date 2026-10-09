package upgrade

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/Luolc/fednet/internal/payload"
)

// Divert takes every upgrade notice and nothing else; it writes the
// request only for a release newer than the client's, and only when the
// client has a request path.
func TestClientDivert(t *testing.T) {
	notice := func(v string) []byte {
		b, _ := json.Marshal(payload.Message{Type: payload.Upgrade, Version: v})
		return b
	}
	post, _ := json.Marshal(payload.Message{Type: payload.Inbound, Thread: "C1/1.0", Text: "hi"})
	c := &Client{Version: "v0.1.0", Request: filepath.Join(t.TempDir(), "upgrade")}
	if c.Divert(t.Context(), post) || c.Divert(t.Context(), []byte("not json")) {
		t.Fatal("a message that is not an upgrade notice was taken")
	}
	if !c.Divert(t.Context(), notice("v0.1.0")) || !c.Divert(t.Context(), notice("v0.0.9")) || !c.Divert(t.Context(), notice("dev")) {
		t.Fatal("a notice for a release that is not newer was not taken")
	}
	if _, err := ReadRequest(c.Request); !errors.Is(err, ErrNoRequest) {
		t.Fatalf("a request was written for a release that is not newer: %v", err)
	}
	if !c.Divert(t.Context(), notice("v0.2.0")) {
		t.Fatal("the notice was not taken")
	}
	if v, err := ReadRequest(c.Request); err != nil || v != "v0.2.0" {
		t.Fatalf("the request is %q, %v; want v0.2.0", v, err)
	}
	none := &Client{Version: "v0.1.0"}
	if !none.Divert(t.Context(), notice("v0.2.0")) {
		t.Fatal("without a request path the notice was not taken")
	}
}
