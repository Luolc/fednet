package store

import (
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// setEnqueued sets when the queued message msgID was queued.
func setEnqueued(t *testing.T, h *Hub, msgID string, at time.Time) {
	t.Helper()
	if _, err := h.db.ExecContext(t.Context(), "UPDATE outbox SET enqueued_at = ? WHERE msg_id = ?", at.UnixMilli(), msgID); err != nil {
		t.Fatal(err)
	}
}

// DropQueued moves exactly the messages queued at or before the cutoff,
// of every client, into the dropped messages, with when they were queued
// and dropped, until both their notices are recorded.
func TestDropQueued(t *testing.T) {
	ctx := t.Context()
	h := openHub(t, filepath.Join(t.TempDir(), "hub.db"))
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	var ds []Downlink
	for i, client := range []string{"a", "b", "a"} {
		d, err := h.Outbox.Enqueue(ctx, client, []byte{byte('x' + i)})
		if err != nil {
			t.Fatal(err)
		}
		setEnqueued(t, h, d.MsgID, at.Add(time.Duration(i)*time.Millisecond))
		ds = append(ds, d)
	}
	now := at.Add(7 * 24 * time.Hour)
	if _, err := h.DropQueued(ctx, at.Add(-time.Millisecond), now); err != nil {
		t.Fatal(err)
	}
	if dropped, err := h.Dropped(ctx); err != nil || len(dropped) != 0 {
		t.Fatalf("Dropped before the cutoff = %+v, %v; want none", dropped, err)
	}
	if _, err := h.DropQueued(ctx, at.Add(time.Millisecond), now); err != nil {
		t.Fatal(err)
	}
	dropped, err := h.Dropped(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(dropped) != 2 || dropped[0].Seq != ds[0].Seq || dropped[0].Client != "a" || dropped[1].Seq != ds[1].Seq || dropped[1].Client != "b" ||
		!dropped[0].Enqueued.Equal(at) || !dropped[0].At.Equal(now) || string(dropped[0].Payload) != "x" || dropped[0].MsgID != ds[0].MsgID {
		t.Fatalf("Dropped = %+v, want the first two messages, queued at %v and dropped at %v", dropped, at, now)
	}
	left, err := h.Outbox.After(ctx, "a", 0)
	if err != nil || len(left) != 1 || left[0].Seq != ds[2].Seq {
		t.Fatalf("a's queue after the drop = %+v, %v; want only the last message", left, err)
	}

	if err := h.Notified(ctx, ds[0].Seq, true, false); err != nil {
		t.Fatal(err)
	}
	if err := h.Notified(ctx, ds[1].Seq, true, true); err != nil {
		t.Fatal(err)
	}
	dropped, err = h.Dropped(ctx)
	if err != nil || len(dropped) != 1 || dropped[0].Seq != ds[0].Seq || !dropped[0].Told || dropped[0].Alerted {
		t.Fatalf("Dropped after the notices = %+v, %v; want only the first, told and not alerted", dropped, err)
	}
}

// A mark is recorded only with a message still queued, and given back
// once, when the message leaves the outbox by an ack or a drop.
func TestMarks(t *testing.T) {
	ctx := t.Context()
	h := openHub(t, filepath.Join(t.TempDir(), "hub.db"))
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	var ds []Downlink
	for range 3 {
		d, err := h.Outbox.Enqueue(ctx, "a", []byte("x"))
		if err != nil {
			t.Fatal(err)
		}
		ds = append(ds, d)
	}
	for i, d := range ds[:2] {
		if ok, err := h.Outbox.Mark(ctx, d.MsgID, "C1/"+string(rune('1'+i))); err != nil || !ok {
			t.Fatalf("Mark of a queued message = %v, %v; want it recorded", ok, err)
		}
	}
	marks, err := h.Outbox.Ack(ctx, "a", ds[0].Seq)
	if err != nil || !slices.Equal(marks, []string{"C1/1"}) {
		t.Fatalf("Ack = %q, %v; want the first mark", marks, err)
	}
	if ok, err := h.Outbox.Mark(ctx, ds[0].MsgID, "C1/1"); err != nil || ok {
		t.Fatalf("Mark of an acked message = %v, %v; want nothing recorded", ok, err)
	}
	setEnqueued(t, h, ds[1].MsgID, at)
	marks, err = h.DropQueued(ctx, at, at.Add(time.Hour))
	if err != nil || !slices.Equal(marks, []string{"C1/2"}) {
		t.Fatalf("DropQueued = %q, %v; want the second mark", marks, err)
	}
	marks, err = h.Outbox.Ack(ctx, "a", ds[2].Seq)
	if err != nil || len(marks) != 0 {
		t.Fatalf("Ack of an unmarked message = %q, %v; want no marks", marks, err)
	}
}
