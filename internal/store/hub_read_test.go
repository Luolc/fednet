package store

import (
	"path/filepath"
	"slices"
	"testing"
)

func TestUndeliveredFrom(t *testing.T) {
	h := openHub(t, filepath.Join(t.TempDir(), "hub.db"))
	ctx := t.Context()
	for _, u := range []Uplink{
		{Message{"m2", []byte("b")}, "workstation"},
		{Message{"m1", []byte("a")}, "datamachine"},
		{Message{"m3", []byte("c")}, "workstation"},
	} {
		if _, err := h.Inbox.PutFrom(ctx, u.Client, u.Message); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.Inbox.MarkDelivered(ctx, "m1"); err != nil {
		t.Fatal(err)
	}
	got, err := h.Inbox.UndeliveredFrom(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var ids, clients []string
	for _, u := range got {
		ids = append(ids, u.MsgID)
		clients = append(clients, u.Client)
	}
	if !slices.Equal(ids, []string{"m2", "m3"}) || !slices.Equal(clients, []string{"workstation", "workstation"}) {
		t.Fatalf("UndeliveredFrom = %v from %v, want m2 and m3, in arrival order, from workstation", ids, clients)
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
