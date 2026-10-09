package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/Luolc/fednet/internal/link"
	"github.com/Luolc/fednet/internal/store"
)

func TestCredentialFile(t *testing.T) {
	c1, err := New("ws")
	if err != nil {
		t.Fatal(err)
	}
	c2, err := New("ws")
	if err != nil {
		t.Fatal(err)
	}
	if len(c1.Secret) < 43 || c1.Secret == c2.Secret {
		t.Fatalf("secrets: len %d (want >= 43), equal %v", len(c1.Secret), c1.Secret == c2.Secret)
	}
	if bytes.Equal(c1.Hash(), c2.Hash()) || len(c1.Hash()) != 32 {
		t.Fatal("hashes of different secrets are equal, or not SHA-256 sized")
	}

	path := filepath.Join(t.TempDir(), "cred")
	if err := Write(path, c1); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("credential file mode = %04o, want 0600", fi.Mode().Perm())
	}
	got, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != c1 {
		t.Fatalf("Read = %+v, want %+v", got.ClientID, c1.ClientID)
	}
	// An existing file is not replaced: the hub has its hash.
	if err := Write(path, c2); err == nil {
		t.Fatal("Write over an existing credential file: err = nil")
	}
	if got, err := Read(path); err != nil || got != c1 {
		t.Fatal("the existing credential was replaced")
	}
	// A file others can read is refused.
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := Read(path); err == nil || strings.Contains(err.Error(), c1.Secret) {
		t.Fatalf("Read of a group-readable file: err nil %v, contains the secret %v; want an error without the secret",
			err == nil, err != nil && strings.Contains(err.Error(), c1.Secret))
	}
}

// identify is the type of link.Hub's Identify.
type identify = func(r *http.Request) (string, error)

// testRecheck is how often the test hub checks open downlinks again.
const testRecheck = 200 * time.Millisecond

// testHub is a link.Hub authenticating against a fresh registry, served by
// an httptest server. A non-nil wrap wraps the hub's Identify.
func testHub(t *testing.T, wrap func(identify) identify) (*store.Hub, *link.Hub, *httptest.Server) {
	t.Helper()
	st, err := store.OpenHub(t.Context(), filepath.Join(t.TempDir(), "hub.db"))
	if err != nil {
		t.Fatal(err)
	}
	id := identify((&Authenticator{Store: st}).Identify)
	if wrap != nil {
		id = wrap(id)
	}
	h := &link.Hub{Store: st, Identify: id, Recheck: testRecheck}
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(func() {
		h.Close()
		srv.Close()
		st.Close()
	})
	return st, h, srv
}

// dial tries the downlink and the uplink with hdr and returns the HTTP
// status each got, plus the uplink reply body.
func dial(t *testing.T, srv *httptest.Server, hdr http.Header) (down, up int, body string) {
	t.Helper()
	conn, res, err := websocket.Dial(t.Context(), "ws"+strings.TrimPrefix(srv.URL, "http")+link.DownlinkPath,
		&websocket.DialOptions{HTTPHeader: hdr})
	if err == nil {
		conn.CloseNow()
	}
	if res == nil {
		t.Fatalf("downlink dial got no response: %v", err)
	}
	down = res.StatusCode

	b, _ := json.Marshal(map[string]any{"msg_id": "m1", "payload": []byte("x")})
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+link.UplinkPath, bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	req.Header = hdr.Clone()
	ures, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer ures.Body.Close()
	var out bytes.Buffer
	out.ReadFrom(ures.Body)
	return down, ures.StatusCode, out.String()
}

func header(id string, c Credential, version string) http.Header {
	h := c.Header(version)
	h.Set(link.ClientHeader, id)
	return h
}

func TestIdentify(t *testing.T) {
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	ctx := t.Context()
	st, _, srv := testHub(t, nil)
	good, err := New("workstation")
	if err != nil {
		t.Fatal(err)
	}
	other, err := New("workstation")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Register(ctx, "workstation", good.Hash()); err != nil {
		t.Fatal(err)
	}

	for _, tt := range []struct {
		name string
		hdr  http.Header
		want int
	}{
		{"registered", header("workstation", good, "1.2.3"), http.StatusSwitchingProtocols},
		{"wrong credential", header("workstation", other, "1.2.3"), http.StatusUnauthorized},
		{"not registered", header("datamachine", good, "1.2.3"), http.StatusUnauthorized},
		{"no credential", http.Header{link.ClientHeader: {"workstation"}}, http.StatusUnauthorized},
		{"no client id", good.Header("1.2.3"), http.StatusUnauthorized},
	} {
		down, up, body := dial(t, srv, tt.hdr)
		wantUp := http.StatusNoContent
		if tt.want != http.StatusSwitchingProtocols {
			wantUp = tt.want
		}
		if down != tt.want || up != wantUp {
			t.Errorf("%s: downlink %d, uplink %d; want %d, %d", tt.name, down, up, tt.want, wantUp)
		}
		// A refusal says nothing about why; the reason is in the log.
		// The body is not printed: a regression could put the credential in it.
		if wantUp == http.StatusUnauthorized && body != "unauthorized\n" {
			t.Errorf("%s: 401 body is %d bytes and not just \"unauthorized\"", tt.name, len(body))
		}
	}

	// The version came along with the accepted connection.
	if reg, err := st.Registration(ctx, "workstation"); err != nil || reg.Version != "1.2.3" {
		t.Fatalf("Registration(workstation) = %+v, %v; want version 1.2.3", reg, err)
	}

	// Revoked: the same credential no longer gets in.
	if err := st.Revoke(ctx, "workstation"); err != nil {
		t.Fatal(err)
	}
	if down, up, _ := dial(t, srv, header("workstation", good, "1.2.3")); down != http.StatusUnauthorized || up != http.StatusUnauthorized {
		t.Fatalf("after Revoke: downlink %d, uplink %d; want 401, 401", down, up)
	}

	// The log names the refused clients, and never a credential.
	for _, want := range []string{"workstation", "datamachine", "revoked", "not registered", "wrong credential"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("log does not mention %q", want)
		}
	}
	for _, secret := range []string{good.Secret, other.Secret} {
		if strings.Contains(logs.String(), secret) {
			t.Fatal("log contains a credential")
		}
	}
}

// Identify's own error strings, for the cases the link only turns into 401.
func TestIdentifyErrors(t *testing.T) {
	ctx := t.Context()
	st, _, _ := testHub(t, nil)
	good, err := New("ws")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Register(ctx, "ws", good.Hash()); err != nil {
		t.Fatal(err)
	}
	a := &Authenticator{Store: st}
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
	req.Header = header("ws", Credential{ClientID: "ws", Secret: "not-the-secret"}, "")
	// The error is not printed either: it is what might carry a credential.
	_, err = a.Identify(req)
	if err == nil {
		t.Fatal("Identify with a wrong credential: err = nil")
	}
	if strings.Contains(err.Error(), "not-the-secret") || strings.Contains(err.Error(), good.Secret) {
		t.Fatal("Identify error contains a credential")
	}
	if !strings.Contains(err.Error(), `"ws"`) {
		t.Fatal("Identify error does not name the client")
	}
}

// connect opens a downlink with hdr, failing the test if it is refused,
// and returns the frames read from it; the channel is closed when the
// connection is. One reader for the connection's life: a Read whose context
// ends closes the connection.
func connect(t *testing.T, srv *httptest.Server, hdr http.Header) <-chan []byte {
	t.Helper()
	conn, _, err := websocket.Dial(t.Context(), "ws"+strings.TrimPrefix(srv.URL, "http")+link.DownlinkPath,
		&websocket.DialOptions{HTTPHeader: hdr})
	if err != nil {
		t.Fatalf("downlink dial: %v", err)
	}
	t.Cleanup(func() { conn.CloseNow() })
	frames := make(chan []byte, 8)
	go func() {
		defer close(frames)
		for {
			_, b, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			frames <- b
		}
	}()
	return frames
}

// rechecks wraps a hub's Identify to report each identification of a
// downlink and can make a registry change right after one.
type rechecks struct {
	// started gets the client id of each downlink request before it is
	// identified: first the handshake, then every recheck of the open
	// connection.
	started chan string
	// changed gets the error of each armed change once it is made.
	changed chan error

	mu    sync.Mutex
	armed map[string]func() error
}

func newRechecks() *rechecks {
	return &rechecks{started: make(chan string, 64), changed: make(chan error, 1), armed: map[string]func() error{}}
}

func (rc *rechecks) wrap(next identify) identify {
	return func(r *http.Request) (string, error) {
		if r.URL.Path != link.DownlinkPath {
			return next(r)
		}
		client := r.Header.Get(link.ClientHeader)
		rc.started <- client
		id, err := next(r)
		rc.mu.Lock()
		change := rc.armed[client]
		delete(rc.armed, client)
		rc.mu.Unlock()
		if change != nil {
			rc.changed <- change()
		}
		return id, err
	}
}

// changeAfterRecheck makes change from inside the next recheck of client's
// open downlink, once that recheck has read the registry: the change lands
// a full recheck interval before the following one. Then it forgets the
// identifications reported so far, so count sees only those that start
// after the change.
func (rc *rechecks) changeAfterRecheck(t *testing.T, client string, change func() error) {
	t.Helper()
	rc.mu.Lock()
	rc.armed[client] = change
	rc.mu.Unlock()
	select {
	case err := <-rc.changed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no recheck of %s's downlink", client)
	}
	rc.forget()
}

// forget drops the identifications reported so far.
func (rc *rechecks) forget() {
	for {
		select {
		case <-rc.started:
		default:
			return
		}
	}
}

// count counts the rechecks of client's downlink that start until the
// connection behind frames closes, up to limit. It reports the count and
// whether the connection closed. Rechecks of other clients are skipped.
func (rc *rechecks) count(client string, frames <-chan []byte, limit int) (int, bool) {
	n := 0
	timeout := time.After(5 * time.Second)
	for n < limit {
		select {
		case _, ok := <-frames:
			if !ok {
				return n, true
			}
		case id := <-rc.started:
			if id == client {
				n++
			}
		case <-timeout:
			return n, false
		}
	}
	return n, false
}

// receives reports whether frames gets the next message queued for client.
func receives(t *testing.T, h *link.Hub, client string, frames <-chan []byte) bool {
	t.Helper()
	if _, err := h.Send(t.Context(), client, []byte("ping")); err != nil {
		t.Fatal(err)
	}
	select {
	case b, ok := <-frames:
		return ok && strings.Contains(string(b), "msg_id")
	case <-time.After(5 * time.Second):
		return false
	}
}

// An open downlink is dropped by the first recheck that starts after its
// client is revoked or re-registered with another credential; other
// clients stay connected. Each change is made right after a recheck, so the
// next recheck is a full interval later. The test counts rechecks instead
// of timing them: how long one takes depends on how busy the machine is.
func TestRegistryChangeDropsConnection(t *testing.T) {
	ctx := t.Context()
	rc := newRechecks()
	st, h, srv := testHub(t, rc.wrap)
	creds := map[string]Credential{}
	for _, id := range []string{"workstation", "datamachine"} {
		c, err := New(id)
		if err != nil {
			t.Fatal(err)
		}
		if err := st.Register(ctx, id, c.Hash()); err != nil {
			t.Fatal(err)
		}
		creds[id] = c
	}

	// Unchanged registry: the connection outlives several rechecks. The
	// fourth starting means the first three passed.
	dm := connect(t, srv, header("datamachine", creds["datamachine"], "1"))
	rc.forget()
	if n, closed := rc.count("datamachine", dm, 4); closed || n != 4 {
		t.Fatalf("registered client: %d rechecks started, closed %v; want 4 started and still open", n, closed)
	}

	ws := connect(t, srv, header("workstation", creds["workstation"], "1"))
	rc.changeAfterRecheck(t, "workstation", func() error { return st.Revoke(ctx, "workstation") })
	if n, closed := rc.count("workstation", ws, 2); !closed || n > 1 {
		t.Fatalf("after Revoke: closed %v, %d rechecks started; want closed by the first", closed, n)
	}
	if down, up, _ := dial(t, srv, header("workstation", creds["workstation"], "1")); down != http.StatusUnauthorized || up != http.StatusUnauthorized {
		t.Fatalf("after Revoke: downlink %d, uplink %d; want 401, 401", down, up)
	}
	if !receives(t, h, "datamachine", dm) {
		t.Fatal("datamachine stopped receiving after another client was revoked")
	}

	// Re-registering with a new credential drops the old one's connection.
	// The workstation's revoked connection is gone, so every recheck of a
	// workstation downlink from here on is of the connection below.
	old, err := New("workstation")
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := New("workstation")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Register(ctx, "workstation", old.Hash()); err != nil {
		t.Fatal(err)
	}
	ws = connect(t, srv, header("workstation", old, "1"))
	rc.changeAfterRecheck(t, "workstation", func() error { return st.Register(ctx, "workstation", fresh.Hash()) })
	if n, closed := rc.count("workstation", ws, 2); !closed || n > 1 {
		t.Fatalf("after Register: closed %v, %d rechecks started; want closed by the first", closed, n)
	}
	if down, up, _ := dial(t, srv, header("workstation", old, "1")); down != http.StatusUnauthorized || up != http.StatusUnauthorized {
		t.Fatalf("replaced credential: downlink %d, uplink %d; want 401, 401", down, up)
	}
	ws = connect(t, srv, header("workstation", fresh, "1"))
	if !receives(t, h, "workstation", ws) {
		t.Fatal("the new credential's connection does not receive")
	}
}
