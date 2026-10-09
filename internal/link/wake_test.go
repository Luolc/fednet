package link

import (
	"slices"
	"sync/atomic"
	"testing"
)

// A message queued in the store directly, not by Send, reaches a client
// that is already connected once WakeAll is called, without a reconnect.
func TestWakeAllPushesStoreQueued(t *testing.T) {
	h, srv := testHub(t, nil)
	cs := openClientStore(t)
	testClient(t, cs, "a", srv.URL, &cutter{})
	// Once the first message is in, the hub's writer has done its first
	// pass and waits for a wake.
	first, err := h.Send(t.Context(), "a", []byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the first message", func() bool { return len(inboxIDs(t, cs.Inbox)) == 1 })
	d, err := h.Store.Outbox.Enqueue(t.Context(), "a", []byte("queued in the store"))
	if err != nil {
		t.Fatal(err)
	}
	h.WakeAll()
	waitFor(t, "the message queued in the store", func() bool { return len(inboxIDs(t, cs.Inbox)) == 2 })
	if got, want := inboxIDs(t, cs.Inbox), []string{first.MsgID, d.MsgID}; !slices.Equal(got, want) {
		t.Fatalf("inbox = %v, want %v", got, want)
	}
}

// Uplinked is called once for each uplink message, after it is in the
// hub's inbox.
func TestUplinkedAfterStored(t *testing.T) {
	h, srv := testHub(t, nil)
	var calls, stored atomic.Int32
	h.Uplinked = func() {
		if ms, err := h.Store.Inbox.Undelivered(t.Context()); err == nil {
			stored.Store(int32(len(ms)))
		}
		calls.Add(1)
	}
	c, _ := testClient(t, openClientStore(t), "a", srv.URL, &cutter{})
	if _, err := c.Post(t.Context(), []byte("up")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "Uplinked", func() bool { return calls.Load() == 1 })
	if n := stored.Load(); n != 1 {
		t.Fatalf("hub inbox held %d messages when Uplinked was called, want 1", n)
	}
}
