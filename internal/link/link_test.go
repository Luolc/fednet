package link

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Luolc/fednet/internal/store"
)

const (
	testLease     = 200 * time.Millisecond
	testHeartbeat = 20 * time.Millisecond
)

var testBackoff = Backoff{Min: time.Millisecond, Max: 10 * time.Millisecond}

// testHub is a Hub served by an httptest server; wrap, if set, wraps the
// Hub's handler.
func testHub(t *testing.T, wrap func(http.Handler) http.Handler) (*Hub, *httptest.Server) {
	t.Helper()
	st, err := store.OpenHub(t.Context(), filepath.Join(t.TempDir(), "hub.db"))
	if err != nil {
		t.Fatal(err)
	}
	h := &Hub{Store: st, Lease: testLease}
	var handler http.Handler = h.Handler()
	if wrap != nil {
		handler = wrap(handler)
	}
	srv := httptest.NewServer(handler)
	t.Cleanup(func() {
		h.Close()
		srv.Close()
		st.Close()
	})
	return h, srv
}

// cutter dials TCP connections and can close all of them at once.
type cutter struct {
	mu    sync.Mutex
	conns []net.Conn
}

func (c *cutter) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.conns = append(c.conns, conn)
	c.mu.Unlock()
	return conn, nil
}

func (c *cutter) cut() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.conns {
		conn.Close()
	}
	c.conns = nil
}

// testClient is a Client with small timings whose Run has been started;
// the returned func stops it and waits for it to return.
func testClient(t *testing.T, st *store.Client, id, hub string, cut *cutter) (*Client, func()) {
	t.Helper()
	c := &Client{
		Store:      st,
		ID:         id,
		Hub:        hub,
		Heartbeat:  testHeartbeat,
		Backoff:    testBackoff,
		HTTPClient: &http.Client{Transport: &http.Transport{DialContext: cut.dial}},
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx)
	}()
	stop := func() {
		cancel()
		<-done
		c.HTTPClient.CloseIdleConnections()
	}
	t.Cleanup(stop)
	return c, stop
}

func openClientStore(t *testing.T) *store.Client {
	t.Helper()
	st, err := store.OpenClient(t.Context(), filepath.Join(t.TempDir(), "client.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// waitFor polls cond until it holds or five seconds pass.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func inboxIDs(t *testing.T, in store.Inbox) []string {
	t.Helper()
	ms, err := in.Undelivered(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(ms))
	for _, m := range ms {
		ids = append(ids, m.MsgID)
	}
	return ids
}

func outboxEmpty(t *testing.T, h *Hub, client string) bool {
	t.Helper()
	ds, err := h.Store.Outbox.After(t.Context(), client, 0)
	if err != nil {
		t.Fatal(err)
	}
	return len(ds) == 0
}

func TestDownlinkStoresOnceAndAcks(t *testing.T) {
	h, srv := testHub(t, nil)
	cs := openClientStore(t)
	before, err := h.Send(t.Context(), "a", []byte("queued before connect"))
	if err != nil {
		t.Fatal(err)
	}
	testClient(t, cs, "a", srv.URL, &cutter{})
	after, err := h.Send(t.Context(), "a", []byte("pushed while connected"))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{before.MsgID, after.MsgID}
	waitFor(t, "both messages in the inbox", func() bool { return len(inboxIDs(t, cs.Inbox)) == 2 })
	if got := inboxIDs(t, cs.Inbox); !slices.Equal(got, want) {
		t.Fatalf("inbox = %v, want %v", got, want)
	}
	waitFor(t, "the hub outbox to be acked", func() bool { return outboxEmpty(t, h, "a") })
	if got := inboxIDs(t, cs.Inbox); !slices.Equal(got, want) {
		t.Fatalf("inbox after ack = %v, want %v", got, want)
	}
}

func TestDownlinkResumesAfterDisconnect(t *testing.T) {
	h, srv := testHub(t, nil)
	cs := openClientStore(t)
	cut := &cutter{}
	testClient(t, cs, "a", srv.URL, cut)
	var want []string
	// Each round queues a batch, lets some of it arrive, then cuts the
	// connection under the client, with the rest of the batch still queued.
	for round := range 5 {
		for i := range 10 {
			d, err := h.Send(t.Context(), "a", []byte{byte(round), byte(i)})
			if err != nil {
				t.Fatal(err)
			}
			want = append(want, d.MsgID)
		}
		waitFor(t, "part of the batch", func() bool { return len(inboxIDs(t, cs.Inbox)) > round*10 })
		cut.cut()
	}
	waitFor(t, "every message", func() bool { return len(inboxIDs(t, cs.Inbox)) == len(want) })
	if got := inboxIDs(t, cs.Inbox); !slices.Equal(got, want) {
		t.Fatalf("inbox = %v, want %v", got, want)
	}
	waitFor(t, "the hub outbox to be acked", func() bool { return outboxEmpty(t, h, "a") })
}

func TestUplinkRetriesUntilStored(t *testing.T) {
	var posts atomic.Int32
	h, srv := testHub(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == UplinkPath && posts.Add(1) <= 3 {
				http.Error(w, "not now", http.StatusServiceUnavailable)
				return
			}
			next.ServeHTTP(w, r)
		})
	})
	cs := openClientStore(t)
	c, _ := testClient(t, cs, "a", srv.URL, &cutter{})
	id, err := c.Post(t.Context(), []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the hub inbox", func() bool { return len(inboxIDs(t, h.Store.Inbox)) == 1 })
	if got := inboxIDs(t, h.Store.Inbox); !slices.Equal(got, []string{id}) {
		t.Fatalf("hub inbox = %v, want [%s]", got, id)
	}
	waitFor(t, "the client outbox to be acked", func() bool {
		ms, err := cs.Outbox.Pending(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		return len(ms) == 0
	})
	if n := posts.Load(); n != 4 {
		t.Fatalf("hub saw %d posts, want 4", n)
	}
}

func TestOnlineFollowsHeartbeat(t *testing.T) {
	h, srv := testHub(t, nil)
	cs := openClientStore(t)
	if h.Online("a") {
		t.Fatal("online before any connection")
	}
	_, stop := testClient(t, cs, "a", srv.URL, &cutter{})
	waitFor(t, "online", func() bool { return h.Online("a") })
	stop()
	waitFor(t, "offline after the lease", func() bool { return !h.Online("a") })
	testClient(t, cs, "a", srv.URL, &cutter{})
	waitFor(t, "online again", func() bool { return h.Online("a") })
}
