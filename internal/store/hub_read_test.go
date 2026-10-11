package store

import (
	"errors"
	"path/filepath"
	"slices"
	"testing"
)

func TestUndeliveredFrom(t *testing.T) {
	h := openHub(t, filepath.Join(t.TempDir(), "hub.db"))
	ctx := t.Context()
	for _, u := range []Uplink{
		{Message: Message{"m2", []byte("b")}, Client: "workstation"},
		{Message: Message{"m1", []byte("a")}, Client: "datamachine"},
		{Message: Message{"m3", []byte("c")}, Client: "workstation"},
	} {
		if _, err := h.Inbox.PutFrom(ctx, u.Client, u.Message); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.Inbox.MarkDelivered(ctx, "m1"); err != nil {
		t.Fatal(err)
	}
	for i, ts := range []string{"1.1", "1.2"} {
		if err := h.PartSent(ctx, "m3", i, ts); err != nil {
			t.Fatal(err)
		}
	}
	got, err := h.Inbox.UndeliveredFrom(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var ids, clients []string
	var parts []int
	for _, u := range got {
		ids = append(ids, u.MsgID)
		clients = append(clients, u.Client)
		parts = append(parts, u.PartsSent)
	}
	if !slices.Equal(ids, []string{"m2", "m3"}) || !slices.Equal(clients, []string{"workstation", "workstation"}) || !slices.Equal(parts, []int{0, 2}) {
		t.Fatalf("UndeliveredFrom = %v from %v with %v parts sent, want m2 and m3, in arrival order, from workstation, 0 and 2 parts", ids, clients, parts)
	}
}

func TestPosted(t *testing.T) {
	h := openHub(t, filepath.Join(t.TempDir(), "hub.db"))
	ctx := t.Context()
	if _, err := h.Inbox.PutFrom(ctx, "workstation", Message{"m1", []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Posted(ctx, "m2"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Posted of a msg_id never received = %v, want ErrNotFound", err)
	}
	p, err := h.Posted(ctx, "m1")
	if err != nil || p.Client != "workstation" || string(p.Payload) != "a" || p.Delivered || p.TS != nil {
		t.Fatalf("Posted before any part went out = %+v, %v", p, err)
	}
	// A part recorded twice, as a retried write records it, keeps the
	// last ts.
	for i, ts := range []string{"1.1", "1.2", "1.3"} {
		if err := h.PartSent(ctx, "m1", min(i, 1), ts); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.Inbox.MarkDelivered(ctx, "m1"); err != nil {
		t.Fatal(err)
	}
	p, err = h.Posted(ctx, "m1")
	if err != nil || !p.Delivered || p.PartsSent != 2 || !slices.Equal(p.TS, []string{"1.1", "1.3"}) {
		t.Fatalf("Posted = %+v, %v; want delivered, 2 parts at 1.1 and 1.3", p, err)
	}
}

func TestClients(t *testing.T) {
	h := openHub(t, filepath.Join(t.TempDir(), "hub.db"))
	ctx := t.Context()
	for _, c := range []string{"workstation", "old", "datamachine"} {
		if err := h.Register(ctx, c, []byte("hash")); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.Revoke(ctx, "old"); err != nil {
		t.Fatal(err)
	}
	got, err := h.Clients(ctx)
	if err != nil || !slices.Equal(got, []string{"datamachine", "workstation"}) {
		t.Fatalf("Clients = %v, %v; want datamachine and workstation", got, err)
	}
}
