package route

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/Luolc/fednet/internal/payload"
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

// queued returns the texts of the messages queued for client, in order.
func queued(t *testing.T, h *store.Hub, client string) []string {
	t.Helper()
	var ps []string
	for _, m := range queuedMessages(t, h, client) {
		ps = append(ps, m.Text)
	}
	return ps
}

func queuedMessages(t *testing.T, h *store.Hub, client string) []payload.Message {
	t.Helper()
	ds, err := h.Outbox.After(t.Context(), client, 0)
	if err != nil {
		t.Fatal(err)
	}
	var ms []payload.Message
	for _, d := range ds {
		var m payload.Message
		if err := json.Unmarshal(d.Payload, &m); err != nil {
			t.Fatal(err)
		}
		ms = append(ms, m)
	}
	return ms
}

// msg is an inbound message with text.
func msg(text string) payload.Message { return payload.Message{Type: payload.Inbound, Text: text} }

// setRoute makes machine, with scope, take the new threads of channel;
// an empty machine removes the channel's route.
func setRoute(t *testing.T, h *store.Hub, channel, machine, scope string) {
	t.Helper()
	var after *store.Route
	if machine != "" {
		after = &store.Route{Channel: channel, Machine: machine, Scope: scope}
	}
	if err := h.SetRoute(t.Context(), channel, after, store.RouteChange{}); err != nil {
		t.Fatal(err)
	}
}

func routeNew(t *testing.T, r *Router, channel, thread, text string) string {
	t.Helper()
	client, err := r.RouteNew(t.Context(), channel, "", thread, msg(text))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func routeReply(t *testing.T, r *Router, thread, text string) string {
	t.Helper()
	client, err := r.RouteReply(t.Context(), thread, msg(text))
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
	setRoute(t, h, "dev", "workstation", "")
	setRoute(t, h, "data", "datamachine", "")
	r := New(h)

	// A new thread goes to its channel's default machine, which becomes the owner.
	if got := routeNew(t, r, "data", "t1", "first"); got != "datamachine" {
		t.Fatalf("new thread in data went to %q, want datamachine", got)
	}
	if owner, err := h.Owner(ctx, "t1"); err != nil || owner != "datamachine" {
		t.Fatalf("Owner(t1) = %q, %v; want datamachine", owner, err)
	}

	// A reply goes to the owner even after the channel's default changes.
	setRoute(t, h, "data", "workstation", "")
	setRoute(t, h, "dev", "", "")
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
	if _, err := r.RouteNew(ctx, "random", "", "t2", msg("x")); !errors.Is(err, ErrNoMachine) {
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
	if _, err := r.RouteReply(ctx, "t3", msg("x")); !errors.Is(err, ErrNoOwner) {
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
	setRoute(t, h, "dev", "a", "")
	// Two routers race on one new thread while its channel's route moves
	// to another machine; both messages must end up with the single owner.
	routers := []*Router{New(h), New(h)}
	got := make([]string, len(routers))
	var wg sync.WaitGroup
	for i, r := range routers {
		wg.Go(func() {
			client, err := r.RouteNew(t.Context(), "dev", "", "t1", msg("m"))
			if err != nil {
				t.Error(err)
			}
			got[i] = client
		})
	}
	setRoute(t, h, "dev", "b", "")
	wg.Wait()
	if got[0] != got[1] {
		t.Fatalf("concurrent first messages went to %v, want one client", got)
	}
	if n := len(queued(t, h, got[0])); n != 2 {
		t.Fatalf("owner %s has %d queued, want 2", got[0], n)
	}
}

// A thread takes the scope of its channel's route when it gets its owner;
// its messages carry that scope, also after the route's scope changes,
// which only the threads started after it take.
func TestRouteScope(t *testing.T) {
	h := openHub(t)
	r := New(h)
	setRoute(t, h, "dev", "workstation", "control-plane")
	routeNew(t, r, "dev", "t1", "first")
	setRoute(t, h, "dev", "workstation", "data")
	routeReply(t, r, "t1", "reply")
	routeNew(t, r, "dev", "t2", "second")
	setRoute(t, h, "dev", "workstation", "")
	routeNew(t, r, "dev", "t3", "third")
	routeReply(t, r, "t2", "reply2")
	var got [][2]string
	for _, m := range queuedMessages(t, h, "workstation") {
		got = append(got, [2]string{m.Text, m.Scope})
	}
	want := [][2]string{{"first", "control-plane"}, {"reply", "control-plane"}, {"second", "data"}, {"third", ""}, {"reply2", "data"}}
	if !slices.Equal(got, want) {
		t.Fatalf("queued (text, scope) = %v, want %v", got, want)
	}
}

func TestRouteDM(t *testing.T) {
	ctx := t.Context()
	h := openHub(t)
	setRoute(t, h, store.DMRoute, "workstation", "")
	r := New(h)

	// Each message that starts a direct message thread goes to the DM
	// machine, which owns the thread; replies in it follow the owner.
	if got, err := r.RouteNewDM(ctx, "D1/t1", msg("first")); err != nil || got != "workstation" {
		t.Fatalf("RouteNewDM = %q, %v; want workstation", got, err)
	}
	if got := routeReply(t, r, "D1/t1", "reply"); got != "workstation" {
		t.Fatalf("reply in a DM thread went to %q, want workstation", got)
	}
	if err := h.Reassign(ctx, "D1/t1", "datamachine"); err != nil {
		t.Fatal(err)
	}
	setRoute(t, h, store.DMRoute, "", "")
	if got := routeReply(t, r, "D1/t1", "after"); got != "datamachine" {
		t.Fatalf("reply after Reassign went to %q, want datamachine", got)
	}
	// With no DM machine, a new DM thread goes nowhere.
	if _, err := r.RouteNewDM(ctx, "D1/t2", msg("x")); !errors.Is(err, ErrNoMachine) {
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
	setRoute(t, h, "dev", "workstation", "")
	boom := errors.New("boom")
	fresh, err := h.ReceiveSlack(ctx, store.SlackMessage{Channel: "dev", TS: "1.1"}, func(tx *store.Hub) error {
		if _, err := New(tx).RouteNew(ctx, "dev", "", "dev/1.1", msg("first")); err != nil {
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
		_, err := New(tx).RouteNew(ctx, "dev", "", "dev/1.1", msg("first"))
		return err
	})
	if !fresh || err != nil {
		t.Fatalf("ReceiveSlack again = %v, %v; want true, nil", fresh, err)
	}
	if got := queued(t, h, "workstation"); !slices.Equal(got, []string{"first"}) {
		t.Fatalf("queued for workstation = %v, want [first]", got)
	}
}
