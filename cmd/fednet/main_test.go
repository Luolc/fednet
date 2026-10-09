package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Luolc/fednet/internal/auth"
	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/store"
)

func TestRun(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantCode   int
		wantStdout string
		wantStderr string
	}{
		{"version", []string{"version"}, 0, "dev\n", ""},
		{"hub without flags", []string{"hub"}, 2, "", "-listen and -db are required"},
		{"client without flags", []string{"client"}, 2, "", "-hub, -db, -credential and -socket are required"},
		{"post without a thread", []string{"client", "post", "-socket", "x", "hello"}, 2, "", "-socket, -thread and a non-empty TEXT are required"},
		{"post without a text", []string{"client", "post", "-socket", "x", "-thread", "t"}, 2, "", "want 1 arguments"},
		{"post to a missing socket", []string{"client", "post", "-socket", "/nonexistent/fednet.sock", "-thread", "t", "hello"}, 4, "", "no such file"},
		{"client init without flags", []string{"client", "init"}, 2, "", "-id and -credential are required"},
		{"register with a bad hash", []string{"hub", "register", "-db", "x", "ws", "nothex"}, 2, "", "HASH must be"},
		{"revoke without a client", []string{"hub", "revoke", "-db", "x"}, 2, "", "want 1 arguments"},
		{"no command", nil, 2, "", "usage: fednet"},
		{"unknown command", []string{"serve"}, 2, "", "usage: fednet"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(t.Context(), tt.args, &stdout, &stderr)
			if code != tt.wantCode {
				t.Errorf("exit code = %d, want %d", code, tt.wantCode)
			}
			if stdout.String() != tt.wantStdout {
				t.Errorf("stdout = %q, want %q", stdout.String(), tt.wantStdout)
			}
			if !strings.Contains(stderr.String(), tt.wantStderr) {
				t.Errorf("stderr = %q, want it to contain %q", stderr.String(), tt.wantStderr)
			}
		})
	}
}

// syncBuffer is a bytes.Buffer that several goroutines may write and read.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
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

// start runs a fednet command in the background; the returned func stops it
// and returns its exit code.
func start(t *testing.T, args []string, stdout, stderr *syncBuffer) func() int {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan int, 1)
	go func() { done <- run(ctx, args, stdout, stderr) }()
	var once sync.Once
	code := -1
	stop := func() int {
		once.Do(func() {
			cancel()
			select {
			case code = <-done:
			case <-time.After(10 * time.Second):
				t.Fatalf("fednet %s did not stop", args[0])
			}
		})
		return code
	}
	t.Cleanup(func() { stop() })
	return stop
}

var listening = regexp.MustCompile(`listening on (\S+)`)

func TestHubAndClient(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	hubDB := filepath.Join(dir, "hub.db")
	clientDB := filepath.Join(dir, "client.db")
	credPath := filepath.Join(dir, "credential")
	socket := filepath.Join(dir, "fednet.sock")
	var stdout, stderr syncBuffer
	// Whatever a failure prints goes through redact, so that a regression
	// which leaks the credential does not leak it into the test log too.
	var secret string
	redact := func(s string) string {
		if secret != "" {
			s = strings.ReplaceAll(s, secret, "[REDACTED]")
		}
		return s
	}
	t.Cleanup(func() {
		switch {
		case !t.Failed():
		case secret == "":
			// init may have written the file and still failed, so the
			// output could hold the credential and redact cannot find it.
			t.Logf("output not shown: %d bytes of stdout, %d of stderr, credential unknown", len(stdout.String()), len(stderr.String()))
		default:
			t.Logf("stdout:\n%s\nstderr:\n%s", redact(stdout.String()), redact(stderr.String()))
		}
	})

	// The client makes its credential; the hub registers its hash.
	if code := run(ctx, []string{"client", "init", "-id", "workstation", "-credential", credPath}, &stdout, &stderr); code != 0 {
		t.Fatalf("client init: exit %d, stderr %d bytes", code, len(stderr.String()))
	}
	cred, err := auth.Read(credPath)
	if err != nil {
		t.Fatal(err)
	}
	secret = cred.Secret
	fields := strings.Fields(stdout.String())
	if len(fields) != 2 || fields[0] != "workstation" || len(fields[1]) != 64 {
		t.Fatalf("client init printed %q, want the client id and a hex SHA-256", redact(stdout.String()))
	}
	if code := run(ctx, []string{"hub", "register", "-db", hubDB, fields[0], fields[1]}, &stdout, &stderr); code != 0 {
		t.Fatalf("hub register: exit %d, stderr %q", code, redact(stderr.String()))
	}

	// A message queued on the hub before the client ever connects.
	hs, err := store.OpenHub(ctx, hubDB)
	if err != nil {
		t.Fatal(err)
	}
	queued, err := hs.Outbox.Enqueue(ctx, "workstation", []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	hs.Close()

	stopHub := start(t, []string{"hub", "-listen", "127.0.0.1:0", "-db", hubDB}, &stdout, &stderr)
	var addr string
	waitFor(t, "the hub to listen", func() bool {
		m := listening.FindStringSubmatch(stdout.String())
		if m != nil {
			addr = m[1]
		}
		return m != nil
	})
	// The test's own handle on the client database is opened first: two
	// connections creating the same new file at once can get SQLITE_BUSY.
	cs, err := store.OpenClient(ctx, clientDB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cs.Close() })
	stopClient := start(t, []string{"client", "-hub", "http://" + addr, "-db", clientDB, "-credential", credPath, "-socket", socket}, &stdout, &stderr)
	waitFor(t, "the message in the client inbox", func() bool {
		ms, err := cs.Inbox.Undelivered(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return len(ms) == 1 && ms[0].MsgID == queued.MsgID && string(ms[0].Payload) == "hello"
	})
	// The hub learned the client's version from the connection.
	hs, err = store.OpenHub(ctx, hubDB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hs.Close() })
	if reg, err := hs.Registration(ctx, "workstation"); err != nil || reg.Version != version {
		t.Fatalf("Registration(workstation) = %+v, %v; want version %q", reg, err, version)
	}

	// An agent posts through the client's socket, once as text and once as
	// JSON; the hub stores both posts with their thread.
	waitFor(t, "the client's socket", func() bool {
		_, err := os.Stat(socket)
		return err == nil
	})
	const thread = "C1/1700000000.000100"
	var out syncBuffer
	if code := run(ctx, []string{"client", "post", "-socket", socket, "-thread", thread, "build is green"}, &out, &stderr); code != 0 {
		t.Fatalf("client post: exit %d, stderr %q", code, redact(stderr.String()))
	}
	textID := strings.TrimSpace(out.String())
	out = syncBuffer{}
	if code := run(ctx, []string{"client", "post", "-socket", socket, "-thread", thread, "-json", "--", "- done"}, &out, &stderr); code != 0 {
		t.Fatalf("client post -json: exit %d, stderr %q", code, redact(stderr.String()))
	}
	var res struct {
		MsgID string `json:"msg_id"`
	}
	if err := json.Unmarshal([]byte(out.String()), &res); err != nil || res.MsgID == "" || textID == "" {
		t.Fatalf("client post printed %q, then %q; want a msg_id, then JSON with one", textID, out.String())
	}
	want := map[string]payload.Message{
		textID:    {Type: payload.Post, Thread: thread, Text: "build is green"},
		res.MsgID: {Type: payload.Post, Thread: thread, Text: "- done"},
	}
	waitFor(t, "the posts in the hub inbox", func() bool {
		ms, err := hs.Inbox.Undelivered(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return len(ms) == len(want)
	})
	ms, err := hs.Inbox.Undelivered(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range ms {
		var got payload.Message
		if err := json.Unmarshal(m.Payload, &got); err != nil || got != want[m.MsgID] {
			t.Errorf("hub inbox has %s: %+v, %v; want %+v", m.MsgID, got, err, want[m.MsgID])
		}
	}

	if code := stopClient(); code != 0 {
		t.Errorf("client exited %d", code)
	}
	if code := stopHub(); code != 0 {
		t.Errorf("hub exited %d", code)
	}

	// Retiring the client, and a client the hub never heard of.
	if code := run(ctx, []string{"hub", "revoke", "-db", hubDB, "workstation"}, &stdout, &stderr); code != 0 {
		t.Fatalf("hub revoke: exit %d, stderr %q", code, redact(stderr.String()))
	}
	if reg, err := hs.Registration(ctx, "workstation"); err != nil || !reg.Revoked {
		t.Fatalf("Registration(workstation) after revoke = %+v, %v; want revoked", reg, err)
	}
	if code := run(ctx, []string{"hub", "revoke", "-db", hubDB, "nobody"}, &stdout, &stderr); code != 1 {
		t.Fatalf("hub revoke of an unknown client: exit %d, want 1", code)
	}

	// Nothing any command wrote contains the credential; the id is there.
	for name, out := range map[string]string{"stdout": stdout.String(), "stderr": stderr.String()} {
		if strings.Contains(out, cred.Secret) {
			t.Fatalf("%s contains the credential", name)
		}
	}
	if !strings.Contains(stdout.String(), "workstation") || !strings.Contains(stderr.String(), "nobody") {
		t.Fatalf("stdout %q / stderr %q do not name the clients", redact(stdout.String()), redact(stderr.String()))
	}
	if fi, err := os.Stat(credPath); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("credential file mode = %v, %v; want 0600", fi.Mode(), err)
	}
}

// A socket this user may not write to is refused with exit code 3.
func TestPostDenied(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root may connect to any socket")
	}
	socket := filepath.Join(t.TempDir(), "fednet.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if err := os.Chmod(socket, 0o400); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := run(t.Context(), []string{"client", "post", "-socket", socket, "-thread", "t", "hello"}, &stdout, &stderr); code != 3 {
		t.Fatalf("exit code = %d, want 3; stderr %q", code, stderr.String())
	}
}
