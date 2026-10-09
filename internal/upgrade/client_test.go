package upgrade

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
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
	// A notice that cannot become a request is taken and reported to the
	// hub once per release: without a request path, or with one that
	// cannot be written.
	var alerts []string
	none := &Client{Version: "v0.1.0", Alert: func(_ context.Context, text string) error {
		alerts = append(alerts, text)
		return nil
	}}
	for range 2 {
		if !none.Divert(t.Context(), notice("v0.2.0")) {
			t.Fatal("without a request path the notice was not taken")
		}
	}
	if len(alerts) != 1 || !strings.Contains(alerts[0], "cannot upgrade from v0.1.0 to v0.2.0") || !strings.Contains(alerts[0], "-upgrade-request") {
		t.Fatalf("alerts = %q, want one about the missing request path", alerts)
	}
	none.Divert(t.Context(), notice("v0.3.0"))
	if len(alerts) != 2 || !strings.Contains(alerts[1], "to v0.3.0") {
		t.Fatalf("alerts = %q, want a second one for the other release", alerts)
	}
	alerts = nil
	bad := &Client{Version: "v0.1.0", Request: filepath.Join(t.TempDir(), "no-such-dir", "upgrade"), Alert: none.Alert}
	for range 2 {
		if !bad.Divert(t.Context(), notice("v0.2.0")) {
			t.Fatal("a notice whose request cannot be written was not taken")
		}
	}
	if len(alerts) != 1 || !strings.Contains(alerts[0], "writing the request") {
		t.Fatalf("alerts = %q, want one about the request that cannot be written", alerts)
	}
}
