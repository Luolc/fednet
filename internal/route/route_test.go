package route

import (
	"errors"
	"path/filepath"
	"slices"
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

func routeNew(t *testing.T, r *Router, channel, thread, payload string) string {
	t.Helper()
	client, err := r.RouteNew(t.Context(), channel, "", thread, []byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func routeReply(t *testing.T, r *Router, thread, payload string) string {
	t.Helper()
	client, err := r.RouteReply(t.Context(), thread, []byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// No client is connected in these tests: messages are queued regardless and
// wait in the outbox.
func TestRoute(t *testing.T) {
	ctx := t.Context()
	h := openHub(t)
	r := New(h, Config{Defaults: map[string]string{"dev": "workstation", "data": "datamachine"}})

	// A new thread goes to its channel's default machine, which becomes the owner.
	if got := routeNew(t, r, "data", "t1", "first"); got != "datamachine" {
		t.Fatalf("new thread in data went to %q, want datamachine", got)
	}
	if owner, err := h.Owner(ctx, "t1"); err != nil || owner != "datamachine" {
		t.Fatalf("Owner(t1) = %q, %v; want datamachine", owner, err)
	}

	// A reply goes to the owner even after the channel's default changes.
	r = New(h, Config{Defaults: map[string]string{"data": "workstation"}})
	if got := routeReply(t, r, "t1", "reply"); got != "datamachine" {
		t.Fatalf("reply in t1 went to %q, want the owner datamachine", got)
	}
	if got := queued(t, h, "datamachine"); len(got) != 2 || got[1] != "reply" {
		t.Fatalf("queued for datamachine = %v, want [first reply]", got)
	}

	// After an explicit reassignment, replies follow the new owner.
	if err := h.Reassign(ctx, "t1", "workstation"); err != nil {
		t.Fatal(err)
	}
	if got := routeReply(t, r, "t1", "after"); got != "workstation" {
		t.Fatalf("reply after Reassign went to %q, want workstation", got)
	}

	// A new thread in a channel with no default machine goes nowhere.
	if _, err := r.RouteNew(ctx, "random", "", "t2", []byte("x")); !errors.Is(err, ErrNoMachine) {
		t.Fatalf("RouteNew in a channel with no default: err = %v, want ErrNoMachine", err)
	}
	if _, err := h.Owner(ctx, "t2"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Owner(t2) err = %v, want ErrNotFound", err)
	}

	// A redelivered first message still reaches the owner after its channel
	// lost its default machine.
	if got := routeNew(t, r, "dev", "t1", "redelivered"); got != "workstation" {
		t.Fatalf("redelivered first message of t1 went to %q, want the owner workstation", got)
	}

	// A reply in a thread with no owner goes nowhere, not to the default machine.
	if _, err := r.RouteReply(ctx, "t3", []byte("x")); !errors.Is(err, ErrNoOwner) {
		t.Fatalf("RouteReply in a thread with no owner: err = %v, want ErrNoOwner", err)
	}
	if _, err := h.Owner(ctx, "t3"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Owner(t3) err = %v, want ErrNotFound", err)
	}
	if got := queued(t, h, "workstation"); len(got) != 2 || got[1] != "redelivered" {
		t.Fatalf("queued for workstation = %v, want [after redelivered]", got)
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
			client, err := r.RouteNew(t.Context(), "dev", "", "t1", []byte("m"))
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

func TestRouteDM(t *testing.T) {
	ctx := t.Context()
	h := openHub(t)
	r := New(h, Config{DM: "workstation"})

	// Each message that starts a direct message thread goes to the DM
	// machine, which owns the thread; replies in it follow the owner.
	if got, err := r.RouteNewDM(ctx, "D1/t1", []byte("first")); err != nil || got != "workstation" {
		t.Fatalf("RouteNewDM = %q, %v; want workstation", got, err)
	}
	if got := routeReply(t, r, "D1/t1", "reply"); got != "workstation" {
		t.Fatalf("reply in a DM thread went to %q, want workstation", got)
	}
	if err := h.Reassign(ctx, "D1/t1", "datamachine"); err != nil {
		t.Fatal(err)
	}
	r = New(h, Config{})
	if got := routeReply(t, r, "D1/t1", "after"); got != "datamachine" {
		t.Fatalf("reply after Reassign went to %q, want datamachine", got)
	}
	// With no DM machine, a new DM thread goes nowhere.
	if _, err := r.RouteNewDM(ctx, "D1/t2", []byte("x")); !errors.Is(err, ErrNoMachine) {
		t.Fatalf("RouteNewDM with no DM machine: err = %v, want ErrNoMachine", err)
	}
	if got := queued(t, h, "workstation"); !slices.Equal(got, []string{"first", "reply"}) {
		t.Fatalf("queued for workstation = %v, want [first reply]", got)
	}
}

// A Router on a Hub bound to a transaction queues in that transaction:
// when the transaction fails, nothing it queued, and no ownership it
// recorded, is left behind.
func TestRouteInTransaction(t *testing.T) {
	ctx := t.Context()
	h := openHub(t)
	cfg := Config{Defaults: map[string]string{"dev": "workstation"}}
	boom := errors.New("boom")
	fresh, err := h.ReceiveSlack(ctx, store.SlackMessage{Channel: "dev", TS: "1.1"}, func(tx *store.Hub) error {
		if _, err := New(tx, cfg).RouteNew(ctx, "dev", "", "dev/1.1", []byte("first")); err != nil {
			return err
		}
		return boom
	})
	if fresh || !errors.Is(err, boom) {
		t.Fatalf("ReceiveSlack = %v, %v; want false, boom", fresh, err)
	}
	if got := queued(t, h, "workstation"); len(got) != 0 {
		t.Fatalf("queued for workstation after a failed transaction = %v, want nothing", got)
	}
	if _, err := h.Owner(ctx, "dev/1.1"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Owner after a failed transaction: err = %v, want ErrNotFound", err)
	}
	fresh, err = h.ReceiveSlack(ctx, store.SlackMessage{Channel: "dev", TS: "1.1"}, func(tx *store.Hub) error {
		_, err := New(tx, cfg).RouteNew(ctx, "dev", "", "dev/1.1", []byte("first"))
		return err
	})
	if !fresh || err != nil {
		t.Fatalf("ReceiveSlack again = %v, %v; want true, nil", fresh, err)
	}
	if got := queued(t, h, "workstation"); !slices.Equal(got, []string{"first"}) {
		t.Fatalf("queued for workstation = %v, want [first]", got)
	}
}
