package link

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/Luolc/fednet/internal/store"
)

const (
	testLease     = 200 * time.Millisecond
	testHeartbeat = 20 * time.Millisecond
	testTimeout   = 100 * time.Millisecond
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
		Timeout:    testTimeout,
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
	waitFor(t, "the hub inbox", func() bool { return len(inboxIDs(t, h.Store.Inbox.Inbox)) == 1 })
	if got := inboxIDs(t, h.Store.Inbox.Inbox); !slices.Equal(got, []string{id}) {
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

// dialHub opens a raw downlink connection to srv as client id. The
// connection is closed when the test ends.
func dialHub(t *testing.T, srv *httptest.Server, id string) *websocket.Conn {
	t.Helper()
	conn, _, err := websocket.Dial(t.Context(), "ws"+strings.TrimPrefix(srv.URL, "http")+DownlinkPath,
		&websocket.DialOptions{HTTPHeader: http.Header{ClientHeader: {id}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.CloseNow() })
	return conn
}

func readDownlink(t *testing.T, conn *websocket.Conn) downlink {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var d downlink
	if err := wsjson.Read(ctx, conn, &d); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestOnlineFollowsHeartbeat(t *testing.T) {
	h, srv := testHub(t, nil)
	if h.Online("a") {
		t.Fatal("online before any connection")
	}
	// A connection that never pings: online on connect, offline once the
	// lease runs out with the connection still open, online again on a ping.
	conn := dialHub(t, srv, "a")
	conn.CloseRead(t.Context())
	if !h.Online("a") {
		t.Fatal("not online right after connecting")
	}
	waitFor(t, "offline after the lease", func() bool { return !h.Online("a") })
	if err := conn.Ping(t.Context()); err != nil {
		t.Fatalf("ping on the idle connection: %v", err)
	}
	if !h.Online("a") {
		t.Fatal("not online right after a ping")
	}
	conn.CloseNow()

	// A Client pings on its heartbeat, so it stays online past the lease.
	cs := openClientStore(t)
	_, stop := testClient(t, cs, "b", srv.URL, &cutter{})
	waitFor(t, "online", func() bool { return h.Online("b") })
	for end := time.Now().Add(3 * testLease); time.Now().Before(end); time.Sleep(time.Millisecond) {
		if !h.Online("b") {
			t.Fatal("went offline while the client was connected and pinging")
		}
	}
	stop()
	waitFor(t, "offline after the client stopped", func() bool { return !h.Online("b") })
}

// fakeHub accepts downlink connections from a Client and hands each one to
// the test.
func fakeHub(t *testing.T) (*httptest.Server, <-chan *websocket.Conn) {
	t.Helper()
	conns := make(chan *websocket.Conn, 8)
	done := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		select {
		case conns <- conn:
		default:
			conn.CloseNow()
		}
		<-done
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(done) })
	return srv, conns
}

func TestClientStoresBeforeAck(t *testing.T) {
	srv, conns := fakeHub(t)
	cs := openClientStore(t)
	testClient(t, cs, "a", srv.URL, &cutter{})
	conn := <-conns
	defer conn.CloseNow()
	// With the store closed the client cannot keep the message, so it must
	// not ack it: the only thing the hub may see is the connection going.
	cs.Close()
	if err := wsjson.Write(t.Context(), conn, downlink{Seq: 1, MsgID: "m1", Payload: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var a ack
	if err := wsjson.Read(ctx, conn, &a); err == nil {
		t.Fatalf("got ack %d for a message the client could not store", a.Seq)
	} else if ctx.Err() != nil {
		t.Fatal("client neither acked nor closed the connection")
	}
}

func TestClientStoresReplayOnce(t *testing.T) {
	srv, conns := fakeHub(t)
	cs := openClientStore(t)
	testClient(t, cs, "a", srv.URL, &cutter{})
	conn := <-conns
	defer conn.CloseNow()
	// The same message twice, as a hub does when the first ack was lost.
	for _, seq := range []int64{1, 2} {
		if err := wsjson.Write(t.Context(), conn, downlink{Seq: seq, MsgID: "m1", Payload: []byte("x")}); err != nil {
			t.Fatal(err)
		}
		var a ack
		if err := wsjson.Read(t.Context(), conn, &a); err != nil {
			t.Fatal(err)
		}
		if a.Seq != seq {
			t.Fatalf("ack %d, want %d", a.Seq, seq)
		}
	}
	if got := inboxIDs(t, cs.Inbox); !slices.Equal(got, []string{"m1"}) {
		t.Fatalf("inbox = %v, want [m1]", got)
	}
}

func TestHubResendsUntilAcked(t *testing.T) {
	h, srv := testHub(t, nil)
	m1, err := h.Send(t.Context(), "a", []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	// Receive without acking, drop the connection: the message comes again.
	conn := dialHub(t, srv, "a")
	if d := readDownlink(t, conn); d.MsgID != m1.MsgID {
		t.Fatalf("first connection got %q, want %q", d.MsgID, m1.MsgID)
	}
	conn.CloseNow()
	conn = dialHub(t, srv, "a")
	d := readDownlink(t, conn)
	if d.MsgID != m1.MsgID || d.Seq != m1.Seq {
		t.Fatalf("second connection got %+v, want seq %d msg %q", d, m1.Seq, m1.MsgID)
	}
	// Ack it: after a reconnect the next message is the one queued later.
	if err := wsjson.Write(t.Context(), conn, ack{Seq: d.Seq}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the hub outbox to be acked", func() bool { return outboxEmpty(t, h, "a") })
	conn.CloseNow()
	m2, err := h.Send(t.Context(), "a", []byte("two"))
	if err != nil {
		t.Fatal(err)
	}
	conn = dialHub(t, srv, "a")
	if d := readDownlink(t, conn); d.MsgID != m2.MsgID {
		t.Fatalf("third connection got %q, want %q", d.MsgID, m2.MsgID)
	}
}

func TestPayloadLimit(t *testing.T) {
	h, srv := testHub(t, nil)
	cs := openClientStore(t)
	c, _ := testClient(t, cs, "a", srv.URL, &cutter{})
	full := make([]byte, MaxPayload)
	over := make([]byte, MaxPayload+1)

	d1, err := h.Send(t.Context(), "a", full)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Send(t.Context(), "a", over); !errors.Is(err, ErrPayloadTooBig) {
		t.Fatalf("Send over the limit: %v, want ErrPayloadTooBig", err)
	}
	d2, err := h.Send(t.Context(), "a", []byte("after"))
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "both downlink messages", func() bool { return len(inboxIDs(t, cs.Inbox)) == 2 })
	if got := inboxIDs(t, cs.Inbox); !slices.Equal(got, []string{d1.MsgID, d2.MsgID}) {
		t.Fatalf("client inbox = %v, want [%s %s]", got, d1.MsgID, d2.MsgID)
	}

	u1, err := c.Post(t.Context(), full)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Post(t.Context(), over); !errors.Is(err, ErrPayloadTooBig) {
		t.Fatalf("Post over the limit: %v, want ErrPayloadTooBig", err)
	}
	u2, err := c.Post(t.Context(), []byte("after"))
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "both uplink messages", func() bool { return len(inboxIDs(t, h.Store.Inbox.Inbox)) == 2 })
	if got := inboxIDs(t, h.Store.Inbox.Inbox); !slices.Equal(got, []string{u1, u2}) {
		t.Fatalf("hub inbox = %v, want [%s %s]", got, u1, u2)
	}
}

func TestRequestsTimeOutAndRetry(t *testing.T) {
	// The first request on each path never gets a reply, so the client has
	// to give up on it; the server lets it go when the test ends.
	var held sync.Map
	var posts atomic.Int32
	done := make(chan struct{})
	h, srv := testHub(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == UplinkPath {
				posts.Add(1)
			}
			if _, seen := held.LoadOrStore(r.URL.Path, true); !seen {
				<-done
				return
			}
			next.ServeHTTP(w, r)
		})
	})
	t.Cleanup(func() { close(done) })
	cs := openClientStore(t)
	c, _ := testClient(t, cs, "a", srv.URL, &cutter{})
	d, err := h.Send(t.Context(), "a", []byte("down"))
	if err != nil {
		t.Fatal(err)
	}
	u, err := c.Post(t.Context(), []byte("up"))
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the downlink message", func() bool { return len(inboxIDs(t, cs.Inbox)) == 1 })
	if got := inboxIDs(t, cs.Inbox); !slices.Equal(got, []string{d.MsgID}) {
		t.Fatalf("client inbox = %v, want [%s]", got, d.MsgID)
	}
	waitFor(t, "the uplink message", func() bool { return len(inboxIDs(t, h.Store.Inbox.Inbox)) == 1 })
	if got := inboxIDs(t, h.Store.Inbox.Inbox); !slices.Equal(got, []string{u}) {
		t.Fatalf("hub inbox = %v, want [%s]", got, u)
	}
	if n := posts.Load(); n < 2 {
		t.Fatalf("hub saw %d posts, want at least 2", n)
	}
}

func TestCloseRefusesHandshakeInFlight(t *testing.T) {
	arrived := make(chan struct{})
	release := make(chan struct{})
	h, srv := testHub(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(arrived)
			<-release
			next.ServeHTTP(w, r)
		})
	})
	type dialed struct {
		conn *websocket.Conn
		err  error
	}
	dc := make(chan dialed, 1)
	go func() {
		conn, _, err := websocket.Dial(t.Context(), "ws"+strings.TrimPrefix(srv.URL, "http")+DownlinkPath,
			&websocket.DialOptions{HTTPHeader: http.Header{ClientHeader: {"a"}}})
		dc <- dialed{conn, err}
	}()
	<-arrived
	h.Close()
	close(release)
	got := <-dc
	if got.err != nil {
		t.Fatalf("dial: %v", got.err)
	}
	defer got.conn.CloseNow()
	// The hub accepted the handshake after Close had begun, so it must
	// close the connection instead of serving it.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var d downlink
	err := wsjson.Read(ctx, got.conn, &d)
	if websocket.CloseStatus(err) != websocket.StatusGoingAway {
		t.Fatalf("read after Close: %v, want close status going away", err)
	}
	h.mu.Lock()
	n := len(h.sessions)
	h.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d sessions registered after Close", n)
	}
}

// waitForGoroutine polls the goroutine stacks until one contains every
// substring in want, as a barrier on where another goroutine has got to.
func waitForGoroutine(t *testing.T, want ...string) {
	t.Helper()
	buf := make([]byte, 1<<20)
	waitFor(t, "a goroutine at "+strings.Join(want, " and "), func() bool {
		for _, g := range strings.Split(string(buf[:runtime.Stack(buf, true)]), "\n\n") {
			ok := true
			for _, w := range want {
				ok = ok && strings.Contains(g, w)
			}
			if ok {
				return true
			}
		}
		return false
	})
}

// TestClosingAConnectionDoesNotWaitOnPing pins the interleaving in which a
// ping callback is blocked on the hub's lock while the lock holder goes on
// to close that same connection: closing waits for the reader, which is
// inside the callback. Both ways of closing a connection are covered.
func TestClosingAConnectionDoesNotWaitOnPing(t *testing.T) {
	for _, tc := range []struct {
		name  string
		waits string
		close func(h *Hub, srv *httptest.Server)
	}{
		{"Close", "link.(*Hub).Close", func(h *Hub, _ *httptest.Server) { h.Close() }},
		{"replacement", "link.(*Hub).register", func(_ *Hub, srv *httptest.Server) { dialHub(t, srv, "a").CloseRead(t.Context()) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, srv := testHub(t, nil)
			conn := dialHub(t, srv, "a")
			conn.CloseRead(t.Context())
			waitForGoroutine(t, "link.(*Hub).readAcks", "websocket.(*Conn).Read")

			// Hold the lock so that the closer queues on it first and the
			// ping callback, holding the connection's reader, queues behind.
			h.mu.Lock()
			done := make(chan struct{})
			go func() {
				defer close(done)
				tc.close(h, srv)
			}()
			waitForGoroutine(t, tc.waits, "sync.(*Mutex).Lock")
			go conn.Ping(t.Context())
			waitForGoroutine(t, "link.(*Hub).heartbeat", "sync.(*Mutex).Lock")
			h.mu.Unlock()

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("closing the connection did not return: it waits for the reader, which waits for the lock")
			}
		})
	}
}
