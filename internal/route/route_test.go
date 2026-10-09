package route

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Luolc/fednet/internal/store"
)

func openHub(t *testing.T) *store.Hub {
	t.Helper()
	h, err := store.OpenHub(t.Context(), filepath.Join(t.TempDir(), "hub.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

// queued returns the payloads queued for client, in order.
func queued(t *testing.T, h *store.Hub, client string) []string {
	t.Helper()
	ds, err := h.Outbox.After(t.Context(), client, 0)
	if err != nil {
		t.Fatal(err)
	}
	var ps []string
	for _, d := range ds {
		ps = append(ps, string(d.Payload))
	}
	return ps
}

func route(t *testing.T, r *Router, channel, thread, payload string) string {
	t.Helper()
	client, err := r.Route(t.Context(), channel, thread, []byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// No client is connected in these tests: Route queues regardless, and the
// messages wait in the outbox.
func TestRoute(t *testing.T) {
	ctx := t.Context()
	h := openHub(t)
	r := New(h, Config{Defaults: map[string]string{"dev": "workstation", "data": "datamachine"}})

	// A new thread goes to its channel's default machine, which becomes the owner.
	if got := route(t, r, "data", "t1", "first"); got != "datamachine" {
		t.Fatalf("new thread in data went to %q, want datamachine", got)
	}
	if owner, err := h.Owner(ctx, "t1"); err != nil || owner != "datamachine" {
		t.Fatalf("Owner(t1) = %q, %v; want datamachine", owner, err)
	}

	// A reply goes to the owner even after the channel's default changes.
	r = New(h, Config{Defaults: map[string]string{"data": "workstation"}})
	if got := route(t, r, "data", "t1", "reply"); got != "datamachine" {
		t.Fatalf("reply in t1 went to %q, want the owner datamachine", got)
	}
	if got := queued(t, h, "datamachine"); len(got) != 2 || got[1] != "reply" {
		t.Fatalf("queued for datamachine = %v, want [first reply]", got)
	}

	// After an explicit reassignment, replies follow the new owner.
	if err := h.Reassign(ctx, "t1", "workstation"); err != nil {
		t.Fatal(err)
	}
	if got := route(t, r, "data", "t1", "after"); got != "workstation" {
		t.Fatalf("reply after Reassign went to %q, want workstation", got)
	}

	// A channel with no default machine takes nothing.
	if _, err := r.Route(ctx, "random", "t2", []byte("x")); !errors.Is(err, ErrNoMachine) {
		t.Fatalf("Route in a channel with no default: err = %v, want ErrNoMachine", err)
	}
	if _, err := h.Owner(ctx, "t2"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Owner(t2) err = %v, want ErrNotFound", err)
	}
}

func TestRouteConcurrentFirstMessages(t *testing.T) {
	h := openHub(t)
	// Two routers with different defaults for the same channel race on one
	// new thread; both messages must end up with the single owner.
	routers := []*Router{
		New(h, Config{Defaults: map[string]string{"dev": "a"}}),
		New(h, Config{Defaults: map[string]string{"dev": "b"}}),
	}
	got := make([]string, len(routers))
	var wg sync.WaitGroup
	for i, r := range routers {
		wg.Go(func() {
			client, err := r.Route(t.Context(), "dev", "t1", []byte("m"))
			if err != nil {
				t.Error(err)
			}
			got[i] = client
		})
	}
	wg.Wait()
	if got[0] != got[1] {
		t.Fatalf("concurrent first messages went to %v, want one client", got)
	}
	if n := len(queued(t, h, got[0])); n != 2 {
		t.Fatalf("owner %s has %d queued, want 2", got[0], n)
	}
}
