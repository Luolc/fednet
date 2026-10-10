package link

import (
	"bytes"
	"context"
	"errors"
	"io"
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
	return testHubWith(t, nil, wrap)
}

// testHubWith is testHub with configure, if set, run on the Hub before it
// serves.
func testHubWith(t *testing.T, configure func(*Hub), wrap func(http.Handler) http.Handler) (*Hub, *httptest.Server) {
	t.Helper()
	st, err := store.OpenHub(t.Context(), filepath.Join(t.TempDir(), "hub.db"))
	if err != nil {
		t.Fatal(err)
	}
	h := &Hub{Store: st, Lease: testLease}
	if configure != nil {
		configure(h)
	}
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
	return testClientWith(t, st, id, hub, cut, nil)
}

// testClientWith is testClient with configure, if set, run on the Client
// before it runs.
func testClientWith(t *testing.T, st *store.Client, id, hub string, cut *cutter, configure func(*Client)) (*Client, func()) {
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
	if configure != nil {
		configure(c)
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

// withVersion sets the version a test client sends.
func withVersion(v string) func(*Client) {
	return func(c *Client) { c.Header = http.Header{VersionHeader: {v}} }
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

func inboxIDs(t *testing.T, in interface {
	Undelivered(context.Context) ([]store.Message, error)
}) []string {
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
	// The hub refuses the first three posts, and stores the fourth only
	// once the client has given up on it, so the client posts it again and
	// the message reaches the store twice whatever the client's timeout.
	var posts atomic.Int32
	h, srv := testHub(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != UplinkPath {
				next.ServeHTTP(w, r)
				return
			}
			switch n := posts.Add(1); {
			case n <= 3:
				http.Error(w, "not now", http.StatusServiceUnavailable)
			case n == 4:
				// The server notices the client hang up only once the
				// body has been read to the end.
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("held post: %v", err)
					return
				}
				select {
				case <-r.Context().Done():
				case <-t.Context().Done():
					return
				}
				// The store must not see the client's cancellation, or it
				// fails instead of storing the message a first time.
				held := r.WithContext(context.WithoutCancel(r.Context()))
				held.Body = io.NopCloser(bytes.NewReader(body))
				rec := httptest.NewRecorder()
				next.ServeHTTP(rec, held)
				if rec.Code != http.StatusNoContent {
					t.Errorf("held post: hub replied %d, want %d", rec.Code, http.StatusNoContent)
				}
			default:
				next.ServeHTTP(w, r)
			}
		})
	})
	cs := openClientStore(t)
	c, _ := testClient(t, cs, "a", srv.URL, &cutter{})
	id, err := c.Post(t.Context(), []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the client outbox to be acked", func() bool {
		ms, err := cs.Outbox.Pending(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		return len(ms) == 0
	})
	if got := inboxIDs(t, h.Store.Inbox.Inbox); !slices.Equal(got, []string{id}) {
		t.Fatalf("hub inbox = %v, want [%s]", got, id)
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
	// The hub records the first heartbeat only after the handshake reply has
	// gone out, so Dial can return before it. Holding h.mu keeps the handler
	// from getting that far, which pins the window.
	h.mu.Lock()
	conn := dialHub(t, srv, "a")
	conn.CloseRead(t.Context())
	_, beat := h.seen["a"]
	h.mu.Unlock()
	if beat {
		t.Fatal("heartbeat recorded before the handler could take the lock")
	}
	waitFor(t, "online after connecting", func() bool { return h.Online("a") })
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
	// A full payload takes the hub a good part of testTimeout to store
	// under -race, so a slow machine would time out every attempt. The
	// limits do not depend on the timeout: leave it at the default.
	c, _ := testClientWith(t, cs, "a", srv.URL, &cutter{}, func(c *Client) { c.Timeout = 0 })
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

func TestRequest(t *testing.T) {
	h, srv := testHub(t, nil)
	h.Answer = func(_ context.Context, client string, req []byte) ([]byte, error) {
		switch string(req) {
		case "bad":
			return nil, Refuse(ErrBadRequest, "no such thing as %s", req)
		case "denied":
			return nil, Refuse(ErrDenied, "not for %s", client)
		case "missing":
			return nil, Refuse(ErrNotFound, "nothing here")
		case "busy":
			return nil, Refuse(ErrBusy, "try again later")
		case "broken":
			return nil, errors.New("disk on fire")
		}
		return []byte(client + " asked " + string(req)), nil
	}
	c := &Client{ID: "a", Hub: srv.URL, Timeout: testTimeout}

	answer, err := c.Request(t.Context(), []byte("hello"))
	if err != nil || string(answer) != "a asked hello" {
		t.Fatalf("Request(hello) = %q, %v; want %q", answer, err, "a asked hello")
	}
	tests := []struct {
		req     string
		wantErr error
		wantMsg string
	}{
		{"bad", ErrBadRequest, "no such thing as bad"},
		{"denied", ErrDenied, "not for a"},
		{"missing", ErrNotFound, "nothing here"},
		{"busy", ErrBusy, "try again later"},
		// The cause of a failure stays on the hub.
		{"broken", nil, "the hub failed to answer"},
	}
	for _, tt := range tests {
		_, err := c.Request(t.Context(), []byte(tt.req))
		if err == nil || !strings.Contains(err.Error(), tt.wantMsg) || strings.Contains(err.Error(), "fire") {
			t.Errorf("Request(%s) = %v, want an error saying %q", tt.req, err, tt.wantMsg)
		}
		if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
			t.Errorf("Request(%s) = %v, want it to wrap %v", tt.req, err, tt.wantErr)
		}
	}
}

// A request the hub refuses to identify is denied.
func TestRequestUnauthorized(t *testing.T) {
	h, srv := testHub(t, nil)
	h.Identify = func(*http.Request) (string, error) { return "", errors.New("who are you") }
	h.Answer = func(context.Context, string, []byte) ([]byte, error) { return []byte("{}"), nil }
	c := &Client{ID: "a", Hub: srv.URL, Timeout: testTimeout}
	if _, err := c.Request(t.Context(), []byte("hello")); !errors.Is(err, ErrDenied) {
		t.Fatalf("Request = %v, want ErrDenied", err)
	}
}

// A hub that is down, or that does not answer in time, fails a request at
// once instead of queueing it.
func TestRequestUnreachable(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	down := "http://" + ln.Addr().String()
	ln.Close()

	done := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-done }))
	t.Cleanup(func() {
		close(done)
		srv.Close()
	})

	for name, hub := range map[string]string{"down": down, "silent": srv.URL} {
		c := &Client{ID: "a", Hub: hub, Timeout: testTimeout}
		start := time.Now()
		_, err := c.Request(t.Context(), []byte("hello"))
		if !errors.Is(err, ErrUnreachable) {
			t.Errorf("%s: Request = %v, want ErrUnreachable", name, err)
		}
		if d := time.Since(start); d > 5*testTimeout {
			t.Errorf("%s: Request took %v, want it bounded by the timeout %v", name, d, testTimeout)
		}
	}
}

// A client whose version the hub does not serve gets no downlink: its
// messages wait in the outbox, its uplink still works, and a client of a
// served version gets its messages. The waiting messages reach the client
// once it runs a served version.
func TestOutdatedClientGetsNoDownlink(t *testing.T) {
	ctx := t.Context()
	h, srv := testHubWith(t, func(h *Hub) {
		h.AcceptVersion = func(v string) error {
			if v != "v2" {
				return errors.New("version " + v + " is older than v2")
			}
			return nil
		}
	}, nil)
	old := openClientStore(t)
	oldClient, stopOld := testClientWith(t, old, "a", srv.URL, &cutter{}, withVersion("v1"))
	served := openClientStore(t)
	testClientWith(t, served, "b", srv.URL, &cutter{}, withVersion("v2"))
	forA, err := h.Send(ctx, "a", []byte("for a"))
	if err != nil {
		t.Fatal(err)
	}
	forB, err := h.Send(ctx, "b", []byte("for b"))
	if err != nil {
		t.Fatal(err)
	}
	posted, err := oldClient.Post(ctx, []byte("from a"))
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "b's message", func() bool { return len(inboxIDs(t, served.Inbox)) == 1 })
	waitFor(t, "a's post in the hub inbox", func() bool { return len(inboxIDs(t, h.Store.Inbox.Inbox)) == 1 })
	if got := inboxIDs(t, h.Store.Inbox.Inbox); !slices.Equal(got, []string{posted}) {
		t.Fatalf("hub inbox = %v, want [%s]", got, posted)
	}
	if got := inboxIDs(t, served.Inbox); !slices.Equal(got, []string{forB.MsgID}) {
		t.Fatalf("b's inbox = %v, want [%s]", got, forB.MsgID)
	}
	// Enough reconnect attempts have happened for a to have got its
	// message, had the hub served it.
	if got := inboxIDs(t, old.Inbox); len(got) != 0 {
		t.Fatalf("a's inbox = %v, want empty", got)
	}
	if outboxEmpty(t, h, "a") {
		t.Fatal("a's message is gone from the hub outbox")
	}
	// a upgraded: the same client id at a served version.
	stopOld()
	testClientWith(t, old, "a", srv.URL, &cutter{}, withVersion("v2"))
	waitFor(t, "a's message after the upgrade", func() bool { return len(inboxIDs(t, old.Inbox)) == 1 })
	if got := inboxIDs(t, old.Inbox); !slices.Equal(got, []string{forA.MsgID}) {
		t.Fatalf("a's inbox = %v, want [%s]", got, forA.MsgID)
	}
}

// A message Divert takes is acked without going in the inbox, and acted
// on before the ack; the others go in as usual. The release UpgradeTo
// names for the client's version goes to the client on the response to
// each dial, and the client's Upgrade gets it: on a dial the hub accepts
// and, so that a client the hub no longer serves is told too, on one it
// refuses, every time the client dials.
func TestUpgradeHeader(t *testing.T) {
	h, srv := testHubWith(t, func(hub *Hub) {
		hub.AcceptVersion = func(v string) error {
			if v == "v0.1.0" {
				return errors.New("too old")
			}
			return nil
		}
		hub.UpgradeTo = func(v string) string {
			if v == "v0.3.0" {
				return ""
			}
			return "v0.3.0"
		}
	}, nil)
	var mu sync.Mutex
	taken := make(map[string][]string)
	told := make(map[string][]string)
	start := func(id, version string) *Client {
		cut := &cutter{}
		c := &Client{
			Store: openClientStore(t), ID: id, Hub: srv.URL, Header: http.Header{VersionHeader: {version}},
			Heartbeat: testHeartbeat, Backoff: testBackoff, Timeout: testTimeout,
			HTTPClient: &http.Client{Transport: &http.Transport{DialContext: cut.dial}},
			Divert: func(_ context.Context, p []byte) bool {
				if string(p) != "upgrade" {
					return false
				}
				mu.Lock()
				taken[id] = append(taken[id], string(p))
				mu.Unlock()
				return true
			},
			Upgrade: func(_ context.Context, to string) {
				mu.Lock()
				told[id] = append(told[id], to)
				mu.Unlock()
			},
		}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() {
			defer close(done)
			c.Run(ctx)
		}()
		t.Cleanup(func() {
			cancel()
			<-done
			c.HTTPClient.CloseIdleConnections()
		})
		return c
	}
	old := start("old", "v0.1.0")
	behind := start("behind", "v0.2.0")
	current := start("current", "v0.3.0")
	count := func(id string) int {
		mu.Lock()
		defer mu.Unlock()
		return len(told[id])
	}
	// The refused client is told on every dial it makes.
	waitFor(t, "the refused client to be told twice", func() bool { return count("old") >= 2 })
	waitFor(t, "the accepted clients to connect", func() bool { return h.Online("behind") && h.Online("current") })
	if n := count("behind"); n != 1 {
		t.Fatalf("the client behind was told %d times, want once for its one dial", n)
	}
	if n := count("current"); n != 0 {
		t.Fatalf("the current client was told %d times, want never", n)
	}
	mu.Lock()
	for _, id := range []string{"old", "behind"} {
		for _, to := range told[id] {
			if to != "v0.3.0" {
				t.Fatalf("%s was told %q, want v0.3.0", id, told[id])
			}
		}
	}
	mu.Unlock()
	// Divert on the accepted client: the diverted message is acked and
	// not stored, the other stored.
	if _, err := h.Send(t.Context(), "behind", []byte("upgrade")); err != nil {
		t.Fatal(err)
	}
	kept, err := h.Send(t.Context(), "behind", []byte("for the hook"))
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the hub outbox to be acked", func() bool { return outboxEmpty(t, h, "behind") })
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(taken["behind"], []string{"upgrade"}) {
		t.Fatalf("Divert took %q, want the upgrade once", taken["behind"])
	}
	if got := inboxIDs(t, behind.Store.Inbox); !slices.Equal(got, []string{kept.MsgID}) {
		t.Fatalf("inbox = %v, want only the message Divert left", got)
	}
	_, _ = old, current
}

// A file GET streams what Fetch returns, with the file's name, type and
// size in the headers; a refusal comes back as a request's does; a
// download the hub breaks off ends short, which the reader sees.
func TestFetch(t *testing.T) {
	h, srv := testHub(t, nil)
	h.Fetch = func(_ context.Context, client, id string) (File, io.ReadCloser, error) {
		switch id {
		case "F1":
			return File{Name: "截图 1.png", Mimetype: "image/png", Size: 6}, io.NopCloser(strings.NewReader("PNG...")), nil
		case "short":
			return File{Name: "short.png", Mimetype: "image/png", Size: 100}, io.NopCloser(strings.NewReader("PNG")), nil
		case "big":
			return File{}, nil, Refuse(ErrDenied, "file big is too big for %s", client)
		case "gone":
			return File{}, nil, Refuse(ErrNotFound, "file gone no longer exists")
		}
		return File{}, nil, errors.New("disk on fire")
	}
	c := &Client{ID: "a", Hub: srv.URL, Timeout: testTimeout}
	f, body, err := c.Fetch(t.Context(), "F1")
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(body)
	body.Close()
	if err != nil || string(b) != "PNG..." || f != (File{Name: "截图 1.png", Mimetype: "image/png", Size: 6}) {
		t.Fatalf("Fetch(F1) = %+v, %q, %v; want the file and its content", f, b, err)
	}
	_, body, err = c.Fetch(t.Context(), "short")
	if err != nil {
		t.Fatal(err)
	}
	b, err = io.ReadAll(body)
	body.Close()
	if !errors.Is(err, io.ErrUnexpectedEOF) || len(b) >= 100 {
		t.Fatalf("Fetch(short) read %d bytes, %v; want fewer than the size and io.ErrUnexpectedEOF", len(b), err)
	}
	tests := []struct {
		id      string
		wantErr error
		wantMsg string
	}{
		{"big", ErrDenied, "file big is too big for a"},
		{"gone", ErrNotFound, "file gone no longer exists"},
		{"broken", nil, "the hub failed to answer"},
	}
	for _, tt := range tests {
		_, _, err := c.Fetch(t.Context(), tt.id)
		if err == nil || !strings.Contains(err.Error(), tt.wantMsg) || strings.Contains(err.Error(), "fire") {
			t.Errorf("Fetch(%s) = %v, want an error saying %q", tt.id, err, tt.wantMsg)
		}
		if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
			t.Errorf("Fetch(%s) = %v, want it to wrap %v", tt.id, err, tt.wantErr)
		}
	}
	// An unknown client is denied; a hub that is down is unreachable.
	h.Identify = func(r *http.Request) (string, error) { return "", errors.New("who?") }
	if _, _, err := c.Fetch(t.Context(), "F1"); !errors.Is(err, ErrDenied) {
		t.Fatalf("Fetch by an unknown client = %v, want ErrDenied", err)
	}
	srv.Close()
	if _, _, err := c.Fetch(t.Context(), "F1"); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("Fetch with the hub down = %v, want ErrUnreachable", err)
	}
}

// An upload: the header goes first, then each file's content; the hub's
// Upload gets them in order and the refusals come back as a request's do.
func TestUpload(t *testing.T) {
	h, srv := testHub(t, nil)
	var got []string
	h.Upload = func(_ context.Context, client string, u Upload, body io.Reader) error {
		if u.Thread == "C1/bad" {
			return Refuse(ErrBadRequest, "not for %s", client)
		}
		for _, f := range u.Files {
			b := make([]byte, f.Size)
			if _, err := io.ReadFull(body, b); err != nil {
				return err
			}
			got = append(got, f.Name+"="+string(b))
		}
		// Nothing follows the declared content.
		if n, _ := io.Copy(io.Discard, body); n != 0 {
			return errors.New("extra bytes")
		}
		got = append(got, "text="+u.Text)
		return nil
	}
	c := &Client{Store: openClientStore(t), ID: "a", Hub: srv.URL, Timeout: testTimeout}
	u := Upload{Thread: "C1/1.1", Text: "see", Files: []FileHeader{{Name: "a.png", Size: 3}, {Name: "b.log", Size: 2}}}
	// More than the declared sizes is not sent.
	if err := c.Upload(t.Context(), u, strings.NewReader("PNGerextra")); err != nil {
		t.Fatal(err)
	}
	if want := []string{"a.png=PNG", "b.log=er", "text=see"}; !slices.Equal(got, want) {
		t.Fatalf("the hub got %q, want %q", got, want)
	}
	err := c.Upload(t.Context(), Upload{Thread: "C1/bad", Files: []FileHeader{{Name: "a", Size: 1}}}, strings.NewReader("x"))
	if !errors.Is(err, ErrBadRequest) || !strings.Contains(err.Error(), "not for a") {
		t.Fatalf("Upload refused = %v, want the refusal", err)
	}
	srv.Close()
	if err := c.Upload(t.Context(), u, strings.NewReader("PNGer")); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("Upload with the hub down = %v, want ErrUnreachable", err)
	}
}

// An upload goes out only once what was queued before it has reached the
// hub; when that does not happen within UploadWait it fails as
// unreachable, and the hub gets nothing.
func TestUploadWaitsForTheOutbox(t *testing.T) {
	outboxPoll = time.Millisecond
	t.Cleanup(func() { outboxPoll = 100 * time.Millisecond })
	h, srv := testHub(t, nil)
	uploads := 0
	h.Upload = func(_ context.Context, _ string, u Upload, body io.Reader) error {
		uploads++
		_, err := io.Copy(io.Discard, body)
		return err
	}
	st := openClientStore(t)
	c := &Client{Store: st, ID: "a", Hub: srv.URL, Timeout: testTimeout, UploadWait: 20 * time.Millisecond}
	id, err := c.Post(t.Context(), []byte(`{"type":"post"}`))
	if err != nil {
		t.Fatal(err)
	}
	u := Upload{Thread: "C1/1.1", Files: []FileHeader{{Name: "a.png", Size: 3}}}
	if err := c.Upload(t.Context(), u, strings.NewReader("PNG")); !errors.Is(err, ErrUnreachable) || uploads != 0 {
		t.Fatalf("Upload behind a queued post = %v with %d uploads, want ErrUnreachable and none", err, uploads)
	}
	if err := st.Outbox.Ack(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	if err := c.Upload(t.Context(), u, strings.NewReader("PNG")); err != nil || uploads != 1 {
		t.Fatalf("Upload with the outbox empty = %v with %d uploads, want one", err, uploads)
	}
}
