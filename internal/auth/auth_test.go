package auth

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

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

// testHub is a link.Hub authenticating against a fresh registry, served by
// an httptest server.
func testHub(t *testing.T) (*store.Hub, *httptest.Server) {
	t.Helper()
	st, err := store.OpenHub(t.Context(), filepath.Join(t.TempDir(), "hub.db"))
	if err != nil {
		t.Fatal(err)
	}
	h := &link.Hub{Store: st, Identify: (&Authenticator{Store: st}).Identify}
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(func() {
		h.Close()
		srv.Close()
		st.Close()
	})
	return st, srv
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
	st, srv := testHub(t)
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
	st, _ := testHub(t)
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
