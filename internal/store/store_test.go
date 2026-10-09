package store

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"
)

func openHub(t *testing.T, path string) *Hub {
	t.Helper()
	h, err := OpenHub(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

func openClient(t *testing.T, path string) *Client {
	t.Helper()
	c, err := OpenClient(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func seqs(ds []Downlink) []int64 {
	var s []int64
	for _, d := range ds {
		s = append(s, d.Seq)
	}
	return s
}

func msgIDs(ms []Message) []string {
	var s []string
	for _, m := range ms {
		s = append(s, m.MsgID)
	}
	return s
}

func TestHubOutbox(t *testing.T) {
	ctx := t.Context()
	h := openHub(t, filepath.Join(t.TempDir(), "hub.db"))
	o := h.Outbox

	var a []int64
	for _, p := range []string{"a1", "a2", "a3"} {
		d, err := o.Enqueue(ctx, "a", []byte(p))
		if err != nil {
			t.Fatal(err)
		}
		a = append(a, d.Seq)
	}
	if _, err := o.Enqueue(ctx, "b", []byte("b1")); err != nil {
		t.Fatal(err)
	}
	if !slices.IsSorted(a) || a[0] == a[1] || a[1] == a[2] {
		t.Fatalf("seqs %v are not strictly increasing", a)
	}

	got, err := o.After(ctx, "a", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(seqs(got), a) || string(got[2].Payload) != "a3" {
		t.Fatalf("After(a, 0) = %v, want seqs %v", got, a)
	}

	// Resume after the first message.
	got, err = o.After(ctx, "a", a[0])
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(seqs(got), a[1:]) {
		t.Fatalf("After(a, %d) seqs = %v, want %v", a[0], seqs(got), a[1:])
	}

	// An ack removes everything up to and including its seq, for that client only.
	if err := o.Ack(ctx, "a", a[1]); err != nil {
		t.Fatal(err)
	}
	got, err = o.After(ctx, "a", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(seqs(got), a[2:]) {
		t.Fatalf("after ack, seqs = %v, want %v", seqs(got), a[2:])
	}
	got, err = o.After(ctx, "b", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("client b has %d queued, want 1", len(got))
	}
}

func TestHubOutboxSeqNeverGoesBack(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "hub.db")
	h := openHub(t, path)
	d, err := h.Outbox.Enqueue(ctx, "a", []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	// Empty the outbox, then reopen: the next seq must still be larger.
	if err := h.Outbox.Ack(ctx, "a", d.Seq); err != nil {
		t.Fatal(err)
	}
	h.Close()
	h = openHub(t, path)
	next, err := h.Outbox.Enqueue(ctx, "a", []byte("y"))
	if err != nil {
		t.Fatal(err)
	}
	if next.Seq <= d.Seq {
		t.Fatalf("seq after emptying and reopening = %d, want > %d", next.Seq, d.Seq)
	}
}

func TestInboxDedup(t *testing.T) {
	ctx := t.Context()
	in := openClient(t, filepath.Join(t.TempDir(), "client.db")).Inbox

	for i, tt := range []struct {
		m       Message
		wantNew bool
	}{
		{Message{"m1", []byte("first")}, true},
		{Message{"m2", []byte("other")}, true},
		{Message{"m1", []byte("redelivered")}, false},
	} {
		isNew, err := in.Put(ctx, tt.m)
		if err != nil {
			t.Fatal(err)
		}
		if isNew != tt.wantNew {
			t.Errorf("put %d (%s): new = %v, want %v", i, tt.m.MsgID, isNew, tt.wantNew)
		}
	}
	got, err := in.Undelivered(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(msgIDs(got), []string{"m1", "m2"}) || string(got[0].Payload) != "first" {
		t.Fatalf("Undelivered = %v, want m1 (first), m2", got)
	}

	// A delivered message stays out of Undelivered, and is still a duplicate.
	if err := in.MarkDelivered(ctx, "m1"); err != nil {
		t.Fatal(err)
	}
	if isNew, err := in.Put(ctx, Message{"m1", []byte("again")}); err != nil || isNew {
		t.Fatalf("put after delivery: new = %v, err = %v; want false, nil", isNew, err)
	}
	got, err = in.Undelivered(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(msgIDs(got), []string{"m2"}) {
		t.Fatalf("Undelivered after delivering m1 = %v, want [m2]", msgIDs(got))
	}
}

func TestHubInboxPutFrom(t *testing.T) {
	ctx := t.Context()
	h := openHub(t, filepath.Join(t.TempDir(), "hub.db"))

	for _, put := range []struct{ client, msgID string }{{"a", "m1"}, {"b", "m2"}, {"b", "m1"}} {
		if _, err := h.Inbox.PutFrom(ctx, put.client, Message{put.msgID, []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	// The redelivered m1 keeps the source it was first stored with.
	for msgID, want := range map[string]string{"m1": "a", "m2": "b"} {
		var got string
		if err := h.db.QueryRowContext(ctx, "SELECT client_id FROM inbox WHERE msg_id = ?", msgID).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("source of %s = %q, want %q", msgID, got, want)
		}
	}
}

func TestClientOutbox(t *testing.T) {
	ctx := t.Context()
	o := openClient(t, filepath.Join(t.TempDir(), "client.db")).Outbox

	id1, err := o.Enqueue(ctx, []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	id2, err := o.Enqueue(ctx, []byte("two"))
	if err != nil {
		t.Fatal(err)
	}
	if id1 == id2 {
		t.Fatalf("two enqueues got the same msg_id %q", id1)
	}
	got, err := o.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(msgIDs(got), []string{id1, id2}) {
		t.Fatalf("Pending = %v, want [%s %s]", msgIDs(got), id1, id2)
	}
	if err := o.Ack(ctx, id1); err != nil {
		t.Fatal(err)
	}
	got, err = o.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(msgIDs(got), []string{id2}) {
		t.Fatalf("Pending after ack = %v, want [%s]", msgIDs(got), id2)
	}
}

func TestOwner(t *testing.T) {
	ctx := t.Context()
	h := openHub(t, filepath.Join(t.TempDir(), "hub.db"))

	if _, err := h.Owner(ctx, "t1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Owner of a new thread: err = %v, want ErrNotFound", err)
	}
	if err := h.Reassign(ctx, "t1", "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Reassign of a thread with no owner: err = %v, want ErrNotFound", err)
	}
	// The first claim wins; a later claim gets the existing owner back.
	for _, claim := range []string{"a", "b"} {
		got, err := h.ClaimOwner(ctx, "t1", claim)
		if err != nil {
			t.Fatal(err)
		}
		if got != "a" {
			t.Fatalf("ClaimOwner(t1, %s) = %q, want a", claim, got)
		}
	}
	if err := h.Reassign(ctx, "t1", "b"); err != nil {
		t.Fatal(err)
	}
	if got, err := h.Owner(ctx, "t1"); err != nil || got != "b" {
		t.Fatalf("Owner(t1) after Reassign = %q, %v; want b", got, err)
	}

	// ReassignClient moves only the threads of the given client.
	for thread, client := range map[string]string{"t2": "b", "t3": "c"} {
		if _, err := h.ClaimOwner(ctx, thread, client); err != nil {
			t.Fatal(err)
		}
	}
	n, err := h.ReassignClient(ctx, "b", "d")
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("ReassignClient(b, d) moved %d threads, want 2", n)
	}
	for thread, want := range map[string]string{"t1": "d", "t2": "d", "t3": "c"} {
		if got, err := h.Owner(ctx, thread); err != nil || got != want {
			t.Fatalf("Owner(%s) = %q, %v; want %s", thread, got, err, want)
		}
	}
}

func TestClaimOwnerConcurrent(t *testing.T) {
	ctx := t.Context()
	h := openHub(t, filepath.Join(t.TempDir(), "hub.db"))

	clients := []string{"a", "b", "c", "d"}
	got := make([]string, len(clients))
	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Go(func() {
			owner, err := h.ClaimOwner(ctx, "t1", c)
			if err != nil {
				t.Error(err)
			}
			got[i] = owner
		})
	}
	wg.Wait()
	for _, owner := range got {
		if owner != got[0] {
			t.Fatalf("concurrent claims returned owners %v, want one owner", got)
		}
	}
	if owner, err := h.Owner(ctx, "t1"); err != nil || owner != got[0] {
		t.Fatalf("Owner(t1) = %q, %v; want %q", owner, err, got[0])
	}
}

// age moves every message queued for client back by d.
func age(t *testing.T, h *Hub, client string, d time.Duration) {
	t.Helper()
	if _, err := h.db.ExecContext(t.Context(),
		"UPDATE outbox SET enqueued_at = enqueued_at - ? WHERE client_id = ?", d.Milliseconds(), client); err != nil {
		t.Fatal(err)
	}
}

func TestQueuedFor(t *testing.T) {
	ctx := t.Context()
	h := openHub(t, filepath.Join(t.TempDir(), "hub.db"))
	o := h.Outbox

	queuedFor := func(client string, d time.Duration) bool {
		t.Helper()
		ok, err := o.QueuedFor(ctx, client, d)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}

	d, err := o.Enqueue(ctx, "a", []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o.Enqueue(ctx, "b", []byte("y")); err != nil {
		t.Fatal(err)
	}
	if queuedFor("a", time.Minute) {
		t.Fatal("a fresh message counts as queued for a minute")
	}
	age(t, h, "a", time.Hour)
	if !queuedFor("a", 30*time.Minute) {
		t.Fatal("a message queued an hour ago does not count as queued for 30m")
	}
	if queuedFor("a", 2*time.Hour) {
		t.Fatal("a message queued an hour ago counts as queued for 2h")
	}
	if queuedFor("b", 30*time.Minute) {
		t.Fatal("aging client a's queue changed client b's")
	}
	// Once acked, the message no longer counts.
	if err := o.Ack(ctx, "a", d.Seq); err != nil {
		t.Fatal(err)
	}
	if queuedFor("a", 30*time.Minute) {
		t.Fatal("an acked message still counts as queued")
	}
}

func TestHubUpgradeKeepsData(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "hub.db")

	// A database written by the first schema version.
	db, err := open(ctx, path, hubMigrations[:1])
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"INSERT INTO outbox (client_id, msg_id, payload) VALUES ('a', 'm1', 'x')",
		"INSERT INTO inbox (msg_id, payload) VALUES ('u1', 'y')",
		"INSERT INTO owner (thread, client_id) VALUES ('t1', 'a')",
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()

	before := time.Now().UnixMilli()
	h := openHub(t, path)
	got, err := h.Outbox.After(ctx, "a", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].MsgID != "m1" || string(got[0].Payload) != "x" {
		t.Fatalf("outbox after upgrade = %v, want m1 (x)", got)
	}
	in, err := h.Inbox.Undelivered(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(msgIDs(in), []string{"u1"}) {
		t.Fatalf("inbox after upgrade = %v, want [u1]", msgIDs(in))
	}
	if owner, err := h.Owner(ctx, "t1"); err != nil || owner != "a" {
		t.Fatalf("Owner(t1) after upgrade = %q, %v; want a", owner, err)
	}
	// The old row is dated at the upgrade, not at the epoch.
	var at int64
	if err := h.db.QueryRowContext(ctx, "SELECT enqueued_at FROM outbox WHERE msg_id = 'm1'").Scan(&at); err != nil {
		t.Fatal(err)
	}
	if at < before {
		t.Fatalf("enqueued_at of an old row = %d, want >= %d", at, before)
	}
}

func TestReopenKeepsData(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "client.db")
	c := openClient(t, path)
	id, err := c.Outbox.Enqueue(ctx, []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	c.Close()

	c = openClient(t, path)
	got, err := c.Outbox.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(msgIDs(got), []string{id}) {
		t.Fatalf("Pending after reopen = %v, want [%s]", msgIDs(got), id)
	}
}

func TestOpenPathWithURISyntax(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a?b#c.db")
	openClient(t, path)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("database not created at %q: %v", path, err)
	}
}

func TestMigrate(t *testing.T) {
	ctx := t.Context()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "m.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	// The first migration counts its own runs; a rerun would make it 2.
	m1 := "CREATE TABLE runs (n INTEGER); INSERT INTO runs VALUES (1)"
	m2 := "UPDATE runs SET n = n + 1; CREATE TABLE second (x)"
	if err := migrate(ctx, db, []string{m1}); err != nil {
		t.Fatal(err)
	}
	if err := migrate(ctx, db, []string{m1, m2}); err != nil {
		t.Fatal(err)
	}
	if err := migrate(ctx, db, []string{m1, m2}); err != nil {
		t.Fatal(err)
	}
	var n, v int
	if err := db.QueryRowContext(ctx, "SELECT n FROM runs").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&v); err != nil {
		t.Fatal(err)
	}
	// m1 inserted 1 and m2 added 1, each exactly once.
	if n != 2 || v != 2 {
		t.Fatalf("n = %d, user_version = %d; want 2, 2", n, v)
	}

	// A binary that knows fewer migrations than the file has refuses it.
	if err := migrate(ctx, db, []string{m1}); err == nil {
		t.Fatal("migrate with an older migration list: err = nil, want an error")
	}
}
