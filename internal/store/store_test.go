package store

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
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
	// The first claim wins; a later claim gets the existing owner back, and
	// its message is queued for that owner.
	for _, claim := range []string{"a", "b"} {
		got, d, err := h.ClaimAndEnqueue(ctx, "t1", claim, []byte(claim))
		if err != nil {
			t.Fatal(err)
		}
		if got != "a" {
			t.Fatalf("ClaimAndEnqueue(t1, %s) = %q, want a", claim, got)
		}
		queued, err := h.Outbox.After(ctx, "a", d.Seq-1)
		if err != nil {
			t.Fatal(err)
		}
		if len(queued) != 1 || string(queued[0].Payload) != claim {
			t.Fatalf("message of claim %s not queued for a: %v", claim, queued)
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
		if _, _, err := h.ClaimAndEnqueue(ctx, thread, client, []byte("x")); err != nil {
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

func TestClaimAndEnqueueConcurrent(t *testing.T) {
	ctx := t.Context()
	h := openHub(t, filepath.Join(t.TempDir(), "hub.db"))

	clients := []string{"a", "b", "c", "d"}
	got := make([]string, len(clients))
	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Go(func() {
			owner, _, err := h.ClaimAndEnqueue(ctx, "t1", c, []byte(c))
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
	queued, err := h.Outbox.After(ctx, got[0], 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(queued) != len(clients) {
		t.Fatalf("owner %s has %d queued, want %d", got[0], len(queued), len(clients))
	}
}

func TestClaimAndEnqueueAtomic(t *testing.T) {
	ctx := t.Context()
	h := openHub(t, filepath.Join(t.TempDir(), "hub.db"))
	if _, err := h.db.ExecContext(ctx,
		"CREATE TRIGGER fail BEFORE INSERT ON outbox BEGIN SELECT RAISE(ABORT, 'outbox write fails'); END"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.ClaimAndEnqueue(ctx, "t1", "a", []byte("x")); err == nil {
		t.Fatal("ClaimAndEnqueue with a failing outbox: err = nil")
	}
	// The failed enqueue leaves the thread unowned.
	if _, err := h.Owner(ctx, "t1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Owner(t1) after a failed enqueue: err = %v, want ErrNotFound", err)
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
	// The registry table arrived with a later migration.
	if err := h.Register(ctx, "a", []byte("h")); err != nil {
		t.Fatalf("Register after upgrade: %v", err)
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

func TestRegistry(t *testing.T) {
	ctx := t.Context()
	h := openHub(t, filepath.Join(t.TempDir(), "hub.db"))
	hash1, hash2 := []byte("hash-one"), []byte("hash-two")

	if _, err := h.Registration(ctx, "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Registration of an unknown client: err = %v, want ErrNotFound", err)
	}
	if err := h.Revoke(ctx, "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Revoke of an unknown client: err = %v, want ErrNotFound", err)
	}
	if err := h.Register(ctx, "a", hash1); err != nil {
		t.Fatal(err)
	}
	if err := h.SetVersion(ctx, "a", "1.0"); err != nil {
		t.Fatal(err)
	}
	want := Registration{SecretHash: hash1, Version: "1.0"}
	if got, err := h.Registration(ctx, "a"); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("Registration(a) = %+v, %v; want %+v", got, err, want)
	}

	// Revoking keeps the row but marks it; registering again replaces the
	// hash, lifts the revocation and keeps the version.
	if err := h.Revoke(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if got, err := h.Registration(ctx, "a"); err != nil || !got.Revoked {
		t.Fatalf("Registration(a) after Revoke = %+v, %v; want revoked", got, err)
	}
	if err := h.Register(ctx, "a", hash2); err != nil {
		t.Fatal(err)
	}
	want = Registration{SecretHash: hash2, Version: "1.0"}
	if got, err := h.Registration(ctx, "a"); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("Registration(a) after re-registering = %+v, %v; want %+v", got, err, want)
	}
}

func TestClientInboxRetryAndDeadLetter(t *testing.T) {
	ctx := t.Context()
	in := openClient(t, filepath.Join(t.TempDir(), "client.db")).Inbox
	for _, id := range []string{"m1", "m2"} {
		if _, err := in.Put(ctx, Message{id, []byte(id)}); err != nil {
			t.Fatal(err)
		}
	}
	qs, err := in.Queued(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(qs) != 2 || qs[0].MsgID != "m1" || qs[0].Attempts != 0 || !qs[0].NextAttempt.Equal(time.UnixMilli(0)) {
		t.Fatalf("Queued = %+v, want m1 and m2 with no attempts, due at the epoch", qs)
	}

	// A failed attempt is counted and scheduled.
	next := time.Now().Add(time.Hour).Truncate(time.Millisecond)
	if err := in.Retry(ctx, "m1", next); err != nil {
		t.Fatal(err)
	}
	qs, err = in.Queued(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if qs[0].Attempts != 1 || !qs[0].NextAttempt.Equal(next) || qs[1].Attempts != 0 {
		t.Fatalf("Queued after Retry(m1) = %+v, want m1 at attempt 1 due %v, m2 untouched", qs, next)
	}

	// The last attempt moves the message to the dead letters, with the
	// attempt counted and the reason kept; it leaves the queue, and a
	// redelivery is still a duplicate.
	before := time.Now().Truncate(time.Millisecond)
	if err := in.Bury(ctx, "m1", "exit status 1"); err != nil {
		t.Fatal(err)
	}
	if err := in.Bury(ctx, "m1", "again"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Bury of a buried message: err = %v, want ErrNotFound", err)
	}
	qs, err = in.Queued(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(qs) != 1 || qs[0].MsgID != "m2" {
		t.Fatalf("Queued after Bury(m1) = %+v, want just m2", qs)
	}
	ds, err := in.DeadLetters(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 1 || ds[0].MsgID != "m1" || string(ds[0].Payload) != "m1" || ds[0].Attempts != 2 || ds[0].Reason != "exit status 1" || ds[0].At.Before(before) {
		t.Fatalf("DeadLetters = %+v, want m1 after 2 attempts, reason \"exit status 1\", at or after %v", ds, before)
	}
	if isNew, err := in.Put(ctx, Message{"m1", []byte("redelivered")}); err != nil || isNew {
		t.Fatalf("Put of a dead letter: new = %v, err = %v; want false, nil", isNew, err)
	}
	if qs, err := in.Queued(ctx); err != nil || len(qs) != 1 {
		t.Fatalf("Queued after redelivering a dead letter = %+v, %v; want just m2", qs, err)
	}
}

func TestClientInboxPrune(t *testing.T) {
	ctx := t.Context()
	c := openClient(t, filepath.Join(t.TempDir(), "client.db"))
	in := c.Inbox
	for _, id := range []string{"old", "new", "queued"} {
		if _, err := in.Put(ctx, Message{id, []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"old", "new"} {
		if err := in.MarkDelivered(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.db.ExecContext(ctx, "UPDATE inbox SET delivered_at = delivered_at - ? WHERE msg_id = 'old'", time.Hour.Milliseconds()); err != nil {
		t.Fatal(err)
	}
	n, err := in.Prune(ctx, time.Now().Add(-30*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("Prune deleted %d rows, want 1", n)
	}
	// The pruned message is no longer a duplicate; the kept ones still are.
	for id, wantNew := range map[string]bool{"old": true, "new": false, "queued": false} {
		if isNew, err := in.Put(ctx, Message{id, []byte("again")}); err != nil || isNew != wantNew {
			t.Fatalf("Put(%s) after Prune: new = %v, err = %v; want %v", id, isNew, err, wantNew)
		}
	}
}

func TestClientUpgradeKeepsData(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "client.db")

	// A database written by the first schema version.
	db, err := open(ctx, path, clientMigrations[:1])
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"INSERT INTO inbox (msg_id, payload) VALUES ('m1', 'x')",
		"INSERT INTO inbox (msg_id, payload, delivered) VALUES ('m0', 'y', 1)",
		"INSERT INTO outbox (msg_id, payload) VALUES ('u1', 'z')",
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()

	before := time.Now().Add(-time.Second)
	c := openClient(t, path)
	qs, err := c.Inbox.Queued(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(qs) != 1 || qs[0].MsgID != "m1" || string(qs[0].Payload) != "x" || qs[0].Attempts != 0 {
		t.Fatalf("Queued after upgrade = %+v, want m1 (x) with no attempts", qs)
	}
	out, err := c.Outbox.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(msgIDs(out), []string{"u1"}) {
		t.Fatalf("outbox after upgrade = %v, want [u1]", msgIDs(out))
	}
	// The row delivered before the upgrade is dated at the upgrade, so a
	// prune of what was delivered before it keeps the row.
	if n, err := c.Inbox.Prune(ctx, before); err != nil || n != 0 {
		t.Fatalf("Prune(before the upgrade) = %d, %v; want 0, nil", n, err)
	}
	if isNew, err := c.Inbox.Put(ctx, Message{"m0", []byte("again")}); err != nil || isNew {
		t.Fatalf("Put(m0) after upgrade: new = %v, err = %v; want false, nil", isNew, err)
	}
	if ds, err := c.Inbox.DeadLetters(ctx); err != nil || len(ds) != 0 {
		t.Fatalf("DeadLetters after upgrade = %v, %v; want none", ds, err)
	}
}

func TestClaimAndThreads(t *testing.T) {
	ctx := t.Context()
	h := openHub(t, filepath.Join(t.TempDir(), "hub.db"))
	if got, err := h.Threads(ctx, "a"); err != nil || len(got) != 0 {
		t.Fatalf("Threads(a) with no threads = %v, %v; want none", got, err)
	}
	for thread, client := range map[string]string{"C1/2": "a", "C1/1": "a", "C2/1": "b"} {
		if err := h.Claim(ctx, thread, client); err != nil {
			t.Fatal(err)
		}
	}
	// A claimed thread is owned, and nothing is queued for it.
	if got, err := h.Owner(ctx, "C1/1"); err != nil || got != "a" {
		t.Fatalf("Owner(C1/1) = %q, %v; want a", got, err)
	}
	if ds, err := h.Outbox.After(ctx, "a", 0); err != nil || len(ds) != 0 {
		t.Fatalf("outbox of a = %v, %v; want empty", ds, err)
	}
	// A thread that has an owner keeps it.
	if err := h.Claim(ctx, "C2/1", "a"); err == nil {
		t.Fatal("Claim of an owned thread succeeded")
	}
	if got, err := h.Threads(ctx, "a"); err != nil || !slices.Equal(got, []string{"C1/1", "C1/2"}) {
		t.Fatalf("Threads(a) = %v, %v; want [C1/1 C1/2]", got, err)
	}
	if got, err := h.Threads(ctx, "b"); err != nil || !slices.Equal(got, []string{"C2/1"}) {
		t.Fatalf("Threads(b) = %v, %v; want [C2/1]", got, err)
	}
}

func TestReceiveSlack(t *testing.T) {
	ctx := t.Context()
	h := openHub(t, filepath.Join(t.TempDir(), "hub.db"))
	var handled []string
	receive := func(m SlackMessage) bool {
		t.Helper()
		fresh, err := h.ReceiveSlack(ctx, m, func(tx *Hub) error {
			handled = append(handled, m.Channel+"/"+m.TS)
			_, err := tx.Outbox.Enqueue(ctx, "a", []byte(m.TS))
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		return fresh
	}
	for i, tt := range []struct {
		m    SlackMessage
		want bool
	}{
		{SlackMessage{"C1", "1.1", "Ev1"}, true},
		// The same event again, and the same message by another event.
		{SlackMessage{"C1", "1.1", "Ev1"}, false},
		{SlackMessage{"C1", "1.1", "Ev2"}, false},
		// The same message read from history, with no event id.
		{SlackMessage{"C1", "1.1", ""}, false},
		// Messages from history have no event id to collide on.
		{SlackMessage{"C1", "1.0", ""}, true},
		{SlackMessage{"C2", "1.0", ""}, true},
		// An event seen before, now carrying another message.
		{SlackMessage{"C2", "1.2", "Ev1"}, false},
		{SlackMessage{"C2", "1.2", "Ev3"}, true},
	} {
		if got := receive(tt.m); got != tt.want {
			t.Fatalf("#%d ReceiveSlack(%+v) = %v, want %v", i, tt.m, got, tt.want)
		}
	}
	if want := []string{"C1/1.1", "C1/1.0", "C2/1.0", "C2/1.2"}; !slices.Equal(handled, want) {
		t.Fatalf("handled %v, want %v", handled, want)
	}
	got, err := h.Outbox.After(ctx, "a", 0)
	if err != nil || len(got) != 4 {
		t.Fatalf("queued %d, %v; want the 4 fresh messages", len(got), err)
	}
	// LastSeen is the latest ts taken in, not the last.
	if v, err := h.SlackState(ctx, LastSeen); err != nil || v != "1.2" {
		t.Fatalf("LastSeen = %q, %v; want 1.2", v, err)
	}
	// A failing handler leaves no record, so the message can come again.
	boom := errors.New("boom")
	if fresh, err := h.ReceiveSlack(ctx, SlackMessage{"C3", "9.9", "Ev9"}, func(*Hub) error { return boom }); fresh || !errors.Is(err, boom) {
		t.Fatalf("ReceiveSlack with a failing handler = %v, %v; want false, boom", fresh, err)
	}
	if v, _ := h.SlackState(ctx, LastSeen); v != "1.2" {
		t.Fatalf("LastSeen after a failed receive = %q, want 1.2", v)
	}
	if fresh := receive(SlackMessage{"C3", "9.9", "Ev9"}); !fresh {
		t.Fatal("the message of a failed receive was not fresh when it came again")
	}
}

func TestSlackState(t *testing.T) {
	ctx := t.Context()
	h := openHub(t, filepath.Join(t.TempDir(), "hub.db"))
	if v, err := h.SlackState(ctx, BackfillFrom); err != nil || v != "" {
		t.Fatalf("SlackState of an unset key = %q, %v; want empty", v, err)
	}
	for _, v := range []string{"1.1", "2.2", ""} {
		if err := h.SetSlackState(ctx, BackfillFrom, v); err != nil {
			t.Fatal(err)
		}
		if got, err := h.SlackState(ctx, BackfillFrom); err != nil || got != v {
			t.Fatalf("SlackState after setting %q = %q, %v", v, got, err)
		}
	}
}

func TestThreadsIn(t *testing.T) {
	ctx := t.Context()
	h := openHub(t, filepath.Join(t.TempDir(), "hub.db"))
	for thread, client := range map[string]string{"C1/1.2": "a", "C1/1.1": "b", "C10/1.1": "a", "D1/1.1": "a"} {
		if err := h.Claim(ctx, thread, client); err != nil {
			t.Fatal(err)
		}
	}
	got, err := h.ThreadsIn(ctx, "C1")
	if err != nil || !slices.Equal(got, []string{"C1/1.1", "C1/1.2"}) {
		t.Fatalf("ThreadsIn(C1) = %v, %v; want [C1/1.1 C1/1.2]", got, err)
	}
	if got, err := h.ThreadsIn(ctx, "C2"); err != nil || len(got) != 0 {
		t.Fatalf("ThreadsIn(C2) = %v, %v; want nothing", got, err)
	}
}

// An approval is recorded pending, decided once, and listed while pending
// and while its card is not final.
func TestApprovals(t *testing.T) {
	ctx := t.Context()
	h := openHub(t, filepath.Join(t.TempDir(), "hub.db"))
	at := time.Date(2026, 10, 9, 12, 0, 0, 123456789, time.UTC)
	a := Approval{ID: "apr-1", Client: "workstation", Agent: "ops-exec", Requester: "U2", Summary: "delete b", Action: []byte(`{"b":1}`),
		Nonce: []byte{1, 2, 3}, RequestedAt: at, ExpiresAt: at.Add(time.Hour), Channel: "C9", TS: "1.1"}
	if err := h.PutApproval(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := h.PutApproval(ctx, a); err == nil {
		t.Fatal("PutApproval took the same id twice")
	}
	got, err := h.Approval(ctx, "apr-1")
	if err != nil {
		t.Fatal(err)
	}
	a.Status = Pending
	if !reflect.DeepEqual(got, a) {
		t.Fatalf("Approval = %+v, want %+v", got, a)
	}
	if _, err := h.Approval(ctx, "apr-9"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Approval(apr-9) = %v, want ErrNotFound", err)
	}
	if ps, err := h.PendingApprovals(ctx); err != nil || len(ps) != 1 || ps[0].ID != "apr-1" {
		t.Fatalf("PendingApprovals = %+v, %v; want apr-1", ps, err)
	}
	if us, err := h.UnfinishedCards(ctx); err != nil || len(us) != 0 {
		t.Fatalf("UnfinishedCards while pending = %+v, %v; want none", us, err)
	}

	// The first decision takes and runs its handler in the transaction; the
	// second does not run its handler.
	decidedAt := at.Add(10 * time.Minute)
	ran := 0
	decided, err := h.DecideApproval(ctx, "apr-1", "approved", "U1", decidedAt, func(tx *Hub) error {
		ran++
		_, err := tx.Outbox.Enqueue(ctx, "workstation", []byte("outcome"))
		return err
	})
	if err != nil || !decided {
		t.Fatalf("DecideApproval = %v, %v; want true", decided, err)
	}
	decided, err = h.DecideApproval(ctx, "apr-1", "rejected", "U2", decidedAt, func(tx *Hub) error { ran++; return nil })
	if err != nil || decided {
		t.Fatalf("second DecideApproval = %v, %v; want false", decided, err)
	}
	if ran != 1 {
		t.Fatalf("handlers ran %d times, want 1", ran)
	}
	got, err = h.Approval(ctx, "apr-1")
	if err != nil || got.Status != "approved" || got.DecidedBy != "U1" || !got.DecidedAt.Equal(decidedAt) || got.CardFinal {
		t.Fatalf("Approval after deciding = %+v, %v; want approved by U1 at %s, card not final", got, err, decidedAt)
	}
	if ds, err := h.Outbox.After(ctx, "workstation", 0); err != nil || len(ds) != 1 {
		t.Fatalf("outbox = %+v, %v; want the one outcome", ds, err)
	}
	// A handler that fails leaves the approval pending.
	b := a
	b.ID = "apr-2"
	if err := h.PutApproval(ctx, b); err != nil {
		t.Fatal(err)
	}
	if decided, err := h.DecideApproval(ctx, "apr-2", "approved", "U1", decidedAt, func(*Hub) error { return errors.New("no") }); decided || err == nil {
		t.Fatalf("DecideApproval with a failing handler = %v, %v; want false and the error", decided, err)
	}
	if got, err := h.Approval(ctx, "apr-2"); err != nil || got.Status != Pending {
		t.Fatalf("Approval after a failed handler = %+v, %v; want still pending", got, err)
	}

	if ps, err := h.PendingApprovals(ctx); err != nil || len(ps) != 1 || ps[0].ID != "apr-2" {
		t.Fatalf("PendingApprovals after deciding = %+v, %v; want apr-2", ps, err)
	}
	if us, err := h.UnfinishedCards(ctx); err != nil || len(us) != 1 || us[0].ID != "apr-1" {
		t.Fatalf("UnfinishedCards after deciding = %+v, %v; want apr-1", us, err)
	}
	if err := h.MarkCardFinal(ctx, "apr-1"); err != nil {
		t.Fatal(err)
	}
	if us, err := h.UnfinishedCards(ctx); err != nil || len(us) != 0 {
		t.Fatalf("UnfinishedCards after marking final = %+v, %v; want none", us, err)
	}
}
