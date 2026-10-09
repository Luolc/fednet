package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Luolc/fednet/internal/auth"
	"github.com/Luolc/fednet/internal/handoff"
	"github.com/Luolc/fednet/internal/hook"
	"github.com/Luolc/fednet/internal/inbound"
	"github.com/Luolc/fednet/internal/link"
	"github.com/Luolc/fednet/internal/local"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
	"github.com/Luolc/fednet/internal/upgrade"
)

// asFednet is the environment variable that makes this test binary run as
// fednet: the handoff tests start it as a subprocess, and a handoff starts
// the same binary again with the same arguments and environment.
const asFednet = "FEDNET_TEST_AS_FEDNET"

// testVersion is the environment variable that sets the version the test
// binary reports when it runs as fednet.
const testVersion = "FEDNET_TEST_VERSION"

// The hubs and clients the tests run in this process cannot hand off: the
// real mechanism allows one per OS process. The handoff tests run this
// binary as fednet instead, so nothing is built during the tests.
func TestMain(m *testing.M) {
	if os.Getenv(asFednet) == "1" {
		// The version a release build would have baked in.
		if v := os.Getenv(testVersion); v != "" {
			version = v
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
	}
	newProcess = func(time.Duration) (handoff.Process, error) { return handoff.None{}, nil }
	// No test reaches the real releases: a fetch that slips through fails
	// at once on a port nothing listens on.
	releases = upgrade.Releases{Latest: "http://127.0.0.1:9/latest", Download: "http://127.0.0.1:9/download"}
	os.Exit(m.Run())
}

// fednetBinary returns the path of the binary to run as fednet: this one.
func fednetBinary(t *testing.T) string {
	t.Helper()
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return path
}

// daemon is a fednet process started from the binary. Its output, and that
// of any process it hands off to, is in out.
type daemon struct {
	cmd *exec.Cmd
	out *syncBuffer
}

// startDaemon starts the binary with args and env added to the
// environment. Output goes through a pipe this test owns, so that Wait
// returns when this process exits, not when its successor does.
func startDaemon(t *testing.T, args []string, env ...string) *daemon {
	t.Helper()
	return startDaemonAt(t, fednetBinary(t), args, env...)
}

// startDaemonAt is startDaemon with the binary at path, a copy of this
// one: a handoff starts the binary at the path again, so an upgrade test
// replaces that copy.
func startDaemonAt(t *testing.T, path string, args []string, env ...string) *daemon {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(path, args...)
	cmd.Stdout, cmd.Stderr = w, w
	cmd.Env = append(append(os.Environ(), asFednet+"=1"), env...)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	w.Close()
	d := &daemon{cmd: cmd, out: &syncBuffer{}}
	go func() {
		io.Copy(d.out, r)
		r.Close()
	}()
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
		if t.Failed() {
			t.Logf("output of fednet %s:\n%s", args[0], d.out.String())
		}
	})
	return d
}

// wait waits for the process to exit and returns its exit code.
func (d *daemon) wait(t *testing.T) int {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- d.cmd.Wait() }()
	select {
	case err := <-done:
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return ee.ExitCode()
		}
		if err != nil {
			t.Fatal(err)
		}
		return 0
	case <-time.After(15 * time.Second):
		t.Fatalf("fednet %s (pid %d) did not exit", d.cmd.Args[1], d.cmd.Process.Pid)
		return -1
	}
}

// processGone reports whether pid no longer runs: it does not exist, or is
// a zombie waiting to be reaped by whoever inherited it.
func processGone(pid int) bool {
	if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
		return true
	}
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return true
	}
	// The state follows the parenthesised command name.
	_, after, _ := strings.Cut(string(stat), ") ")
	return strings.HasPrefix(after, "Z")
}

// terminate sends SIGTERM to pid, a process this test does not own, and
// waits for it to go.
func terminate(t *testing.T, pid int) {
	t.Helper()
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	waitFor(t, fmt.Sprintf("pid %d to exit", pid), func() bool { return processGone(pid) })
}

// handoffResult is what the handoff command prints with -json.
type handoffResult struct {
	From, To local.Response
}

// handoffThrough runs the handoff command against socket in this process
// and returns the exit code, what it printed and its stderr.
func handoffThrough(t *testing.T, command, socket string) (int, handoffResult, string) {
	t.Helper()
	var stdout, stderr syncBuffer
	code := run(t.Context(), []string{command, "handoff", "-socket", socket, "-json"}, &stdout, &stderr)
	var res handoffResult
	if code == 0 {
		if err := json.Unmarshal([]byte(stdout.String()), &res); err != nil {
			t.Fatalf("handoff printed %q: %v", stdout.String(), err)
		}
	}
	return code, res, stderr.String()
}

// versionThrough asks the daemon on socket for its version and pid.
func versionThrough(t *testing.T, socket string) local.Response {
	t.Helper()
	res, err := local.Do(t.Context(), socket, local.Request{Cmd: local.Version})
	if err != nil || res.Error != "" {
		t.Fatalf("version through %s = %+v, %v", socket, res, err)
	}
	return res
}

// newCredential makes a client credential, writes it to a file and
// registers it on the hub store.
func newCredential(t *testing.T, hs *store.Hub, id, path string) auth.Credential {
	t.Helper()
	cred, err := auth.New(id)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Write(path, cred); err != nil {
		t.Fatal(err)
	}
	if err := hs.Register(t.Context(), id, cred.Hash()); err != nil {
		t.Fatal(err)
	}
	return cred
}

// openHubStore opens a hub store at path for the test to read and write.
func openHubStore(t *testing.T, path string) *store.Hub {
	t.Helper()
	hs, err := store.OpenHub(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hs.Close() })
	return hs
}

// hookEvents returns the msg_ids the hook script appended to path, one
// event per line.
func hookEvents(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var ids []string
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		var ev hook.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("hook wrote %q: %v", line, err)
		}
		ids = append(ids, ev.MsgID)
	}
	return ids
}

// A client hands off to a new process: messages sent before, during and
// after the handoff all reach the hook once, in order; the old process
// exits, and the socket is answered by the new one.
func TestClientHandoff(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	hs := openHubStore(t, filepath.Join(dir, "hub.db"))
	hub := &link.Hub{Store: hs, Identify: (&auth.Authenticator{Store: hs}).Identify}
	srv := httptest.NewServer(hub.Handler())
	t.Cleanup(func() {
		hub.Close()
		srv.Close()
	})
	credPath := filepath.Join(dir, "credential")
	newCredential(t, hs, "workstation", credPath)
	socket := filepath.Join(dir, "fednet.sock")
	script := filepath.Join(dir, "hook.sh")
	events := filepath.Join(dir, "events")
	if err := os.WriteFile(script, []byte("#!/bin/sh\ncat \"$1\" >> \"$FEDNET_TEST_EVENTS\" && echo >> \"$FEDNET_TEST_EVENTS\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	old := startDaemon(t, []string{"client", "-hub", srv.URL, "-db", filepath.Join(dir, "client.db"), "-credential", credPath,
		"-socket", socket, "-handoff-timeout", "10s", "-hook-env", "FEDNET_TEST_EVENTS", "/bin/sh", script},
		"FEDNET_TEST_EVENTS="+events)
	waitFor(t, "the client's socket", func() bool {
		_, err := os.Stat(socket)
		return err == nil
	})
	if got := versionThrough(t, socket); got.PID != old.cmd.Process.Pid || got.Version != version {
		t.Fatalf("version through the socket = %+v, want pid %d and %q", got, old.cmd.Process.Pid, version)
	}

	before, err := hub.Send(ctx, "workstation", []byte(`{"type":"test","text":"before"}`))
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the first message at the hook", func() bool { return len(hookEvents(t, events)) == 1 })

	type outcome struct {
		code int
		res  handoffResult
		err  string
	}
	handed := make(chan outcome, 1)
	go func() {
		code, res, stderr := handoffThrough(t, "client", socket)
		handed <- outcome{code, res, stderr}
	}()
	during, err := hub.Send(ctx, "workstation", []byte(`{"type":"test","text":"during"}`))
	if err != nil {
		t.Fatal(err)
	}
	var h outcome
	select {
	case h = <-handed:
	case <-time.After(15 * time.Second):
		t.Fatal("handoff did not return")
	}
	if h.code != 0 {
		t.Fatalf("handoff exited %d: %s", h.code, h.err)
	}
	if h.res.From.PID != old.cmd.Process.Pid || h.res.To.PID == 0 || h.res.To.PID == h.res.From.PID || h.res.To.Version != version {
		t.Fatalf("handoff = %+v, want from pid %d to another pid at %q", h.res, old.cmd.Process.Pid, version)
	}
	if code := old.wait(t); code != 0 {
		t.Fatalf("the old client exited %d, want 0", code)
	}
	defer terminate(t, h.res.To.PID)

	after, err := hub.Send(ctx, "workstation", []byte(`{"type":"test","text":"after"}`))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{before.MsgID, during.MsgID, after.MsgID}
	waitFor(t, "every message at the hook", func() bool { return len(hookEvents(t, events)) >= len(want) })
	if got := hookEvents(t, events); !slices.Equal(got, want) {
		t.Fatalf("hook got %v, want %v", got, want)
	}
	// The socket belongs to the new process: an agent's post goes through
	// it to the hub.
	if got := versionThrough(t, socket); got.PID != h.res.To.PID {
		t.Fatalf("version through the socket = %+v, want pid %d", got, h.res.To.PID)
	}
	var stdout, stderr syncBuffer
	if code := run(ctx, []string{"client", "post", "-socket", socket, "-thread", "C1/1700000000.000100", "handed off"}, &stdout, &stderr); code != 0 {
		t.Fatalf("client post: exit %d, stderr %q", code, stderr.String())
	}
	posted := strings.TrimSpace(stdout.String())
	waitFor(t, "the post in the hub inbox", func() bool {
		ms, err := hs.Inbox.Undelivered(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return len(ms) == 1 && ms[0].MsgID == posted
	})
}

// When the new client cannot start, the handoff fails and the old one
// keeps serving.
func TestClientHandoffFailsWhenNewProcessCannotStart(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	hs := openHubStore(t, filepath.Join(dir, "hub.db"))
	hub := &link.Hub{Store: hs, Identify: (&auth.Authenticator{Store: hs}).Identify}
	srv := httptest.NewServer(hub.Handler())
	t.Cleanup(func() {
		hub.Close()
		srv.Close()
	})
	credPath := filepath.Join(dir, "credential")
	newCredential(t, hs, "workstation", credPath)
	socket := filepath.Join(dir, "fednet.sock")
	old := startDaemon(t, []string{"client", "-hub", srv.URL, "-db", filepath.Join(dir, "client.db"), "-credential", credPath,
		"-socket", socket, "-handoff-timeout", "10s"})
	waitFor(t, "the client's socket", func() bool {
		_, err := os.Stat(socket)
		return err == nil
	})
	// The new process refuses a credential file others can read.
	if err := os.Chmod(credPath, 0o644); err != nil {
		t.Fatal(err)
	}
	code, _, stderr := handoffThrough(t, "client", socket)
	if code != 1 || !strings.Contains(stderr, "exited") || !strings.Contains(stderr, "readable by group or others") {
		t.Fatalf("handoff exited %d with %q, want 1, the child's exit and its reason", code, stderr)
	}
	if got := versionThrough(t, socket); got.PID != old.cmd.Process.Pid {
		t.Fatalf("version through the socket = %+v, want pid %d", got, old.cmd.Process.Pid)
	}
	var stdout, stderr2 syncBuffer
	if code := run(ctx, []string{"client", "post", "-socket", socket, "-thread", "C1/1700000000.000100", "still here"}, &stdout, &stderr2); code != 0 {
		t.Fatalf("client post: exit %d, stderr %q", code, stderr2.String())
	}
	posted := strings.TrimSpace(stdout.String())
	waitFor(t, "the post in the hub inbox", func() bool {
		ms, err := hs.Inbox.Undelivered(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return len(ms) == 1 && ms[0].MsgID == posted
	})
	if err := old.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if code := old.wait(t); code != 0 {
		t.Fatalf("the client exited %d, want 0", code)
	}
}

// freePort returns a TCP address on the loopback interface that nothing
// listens on right now.
func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// startHub starts a hub from the binary on a free port with an admin
// socket, and returns it with its address and the socket's path.
func startHub(t *testing.T, dir string, extra ...string) (*daemon, string, string) {
	t.Helper()
	addr := freePort(t)
	admin := filepath.Join(dir, "admin.sock")
	args := append([]string{"hub", "-listen", addr, "-db", filepath.Join(dir, "hub.db"), "-admin-socket", admin, "-handoff-timeout", "10s"}, extra...)
	d := startDaemon(t, args)
	waitFor(t, "the hub to listen", func() bool { return listening.MatchString(d.out.String()) })
	waitFor(t, "the admin socket", func() bool {
		_, err := os.Stat(admin)
		return err == nil
	})
	return d, "http://" + addr, admin
}

// inboxIDs returns the msg_ids in a hub inbox.
func inboxIDs(t *testing.T, hs *store.Hub) []string {
	t.Helper()
	ms, err := hs.Inbox.Undelivered(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(ms))
	for _, m := range ms {
		ids = append(ids, m.MsgID)
	}
	return ids
}

// A hub hands off to a new process: the client's connection moves to the
// new hub, which delivers what the old one had not pushed; posts sent
// before, during and after reach the hub's inbox once; the port and the
// admin socket are the new process's, and the old one exits.
func TestHubHandoff(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	old, hubURL, admin := startHub(t, dir)
	hs := openHubStore(t, filepath.Join(dir, "hub.db"))
	credPath := filepath.Join(dir, "credential")
	cred := newCredential(t, hs, "workstation", credPath)
	cs, err := store.OpenClient(ctx, filepath.Join(dir, "client.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cs.Close() })
	pushed, err := hs.Outbox.Enqueue(ctx, "workstation", []byte(`{"type":"test","text":"before"}`))
	if err != nil {
		t.Fatal(err)
	}
	c := &link.Client{Store: cs, ID: "workstation", Hub: hubURL, Header: cred.Header(version),
		Backoff: link.Backoff{Min: time.Millisecond, Max: 50 * time.Millisecond}, Timeout: 2 * time.Second}
	cctx, cancel := context.WithCancel(ctx)
	ran := make(chan struct{})
	go func() {
		defer close(ran)
		c.Run(cctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-ran
	})
	waitFor(t, "the message pushed by the old hub", func() bool {
		ms, err := cs.Inbox.Undelivered(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return len(ms) == 1
	})
	// Queued in the store without waking the hub: the old hub never pushes
	// it, the new one does when the client reconnects.
	queued, err := hs.Outbox.Enqueue(ctx, "workstation", []byte(`{"type":"test","text":"queued"}`))
	if err != nil {
		t.Fatal(err)
	}
	before, err := c.Post(ctx, []byte(`{"type":"test","text":"before"}`))
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the first post in the hub inbox", func() bool { return len(inboxIDs(t, hs)) == 1 })

	type outcome struct {
		code int
		res  handoffResult
		err  string
	}
	handed := make(chan outcome, 1)
	go func() {
		code, res, stderr := handoffThrough(t, "hub", admin)
		handed <- outcome{code, res, stderr}
	}()
	during, err := c.Post(ctx, []byte(`{"type":"test","text":"during"}`))
	if err != nil {
		t.Fatal(err)
	}
	var h outcome
	select {
	case h = <-handed:
	case <-time.After(15 * time.Second):
		t.Fatal("handoff did not return")
	}
	if h.code != 0 {
		t.Fatalf("handoff exited %d: %s", h.code, h.err)
	}
	if h.res.From.PID != old.cmd.Process.Pid || h.res.To.PID == 0 || h.res.To.PID == h.res.From.PID || h.res.To.Version != version {
		t.Fatalf("handoff = %+v, want from pid %d to another pid at %q", h.res, old.cmd.Process.Pid, version)
	}
	if code := old.wait(t); code != 0 {
		t.Fatalf("the old hub exited %d, want 0", code)
	}
	defer terminate(t, h.res.To.PID)

	after, err := c.Post(ctx, []byte(`{"type":"test","text":"after"}`))
	if err != nil {
		t.Fatal(err)
	}
	wantPosts := []string{before, during, after}
	waitFor(t, "every post in the hub inbox", func() bool { return len(inboxIDs(t, hs)) >= len(wantPosts) })
	if got := inboxIDs(t, hs); !slices.Equal(got, wantPosts) {
		t.Fatalf("hub inbox = %v, want %v", got, wantPosts)
	}
	waitFor(t, "the queued message from the new hub", func() bool {
		ms, err := cs.Inbox.Undelivered(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return len(ms) >= 2
	})
	ms, err := cs.Inbox.Undelivered(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, m := range ms {
		got = append(got, m.MsgID)
	}
	if want := []string{pushed.MsgID, queued.MsgID}; !slices.Equal(got, want) {
		t.Fatalf("client inbox = %v, want %v", got, want)
	}
	// The port and the admin socket are the new hub's.
	res, err := http.Get(hubURL + link.DownlinkPath)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET %s without a credential: %s, want 401 from the new hub", link.DownlinkPath, res.Status)
	}
	if got := versionThrough(t, admin); got.PID != h.res.To.PID {
		t.Fatalf("version through the admin socket = %+v, want pid %d", got, h.res.To.PID)
	}
}

// When the new hub cannot start, the handoff fails and the old one keeps
// serving.
func TestHubHandoffFailsWhenNewProcessCannotStart(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "hub.json")
	if err := os.WriteFile(config, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	old, hubURL, admin := startHub(t, dir, "-config", config)
	// The new process cannot read the config.
	if err := os.WriteFile(config, []byte(`{`), 0o600); err != nil {
		t.Fatal(err)
	}
	code, _, stderr := handoffThrough(t, "hub", admin)
	if code != 1 || !strings.Contains(stderr, "exited") || !strings.Contains(stderr, "hub.json") {
		t.Fatalf("handoff exited %d with %q, want 1, the child's exit and its reason", code, stderr)
	}
	if got := versionThrough(t, admin); got.PID != old.cmd.Process.Pid {
		t.Fatalf("version through the admin socket = %+v, want pid %d", got, old.cmd.Process.Pid)
	}
	res, err := http.Get(hubURL + link.DownlinkPath)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET %s without a credential: %s, want 401 from the old hub", link.DownlinkPath, res.Status)
	}
	if err := old.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if code := old.wait(t); code != 0 {
		t.Fatalf("the hub exited %d, want 0", code)
	}
}

// A handoff lets the run of the hook in hand end: the old client exits only
// once the hook has, with the message marked delivered, and the new client
// does not run the hook for it again.
func TestClientHandoffWaitsForTheHook(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	hs := openHubStore(t, filepath.Join(dir, "hub.db"))
	hub := &link.Hub{Store: hs, Identify: (&auth.Authenticator{Store: hs}).Identify}
	srv := httptest.NewServer(hub.Handler())
	t.Cleanup(func() {
		hub.Close()
		srv.Close()
	})
	credPath := filepath.Join(dir, "credential")
	newCredential(t, hs, "workstation", credPath)
	socket := filepath.Join(dir, "fednet.sock")
	script := filepath.Join(dir, "hook.sh")
	events := filepath.Join(dir, "events")
	fifo := filepath.Join(dir, "fifo")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	// The hook records the event, then waits until the test lets it go.
	body := "#!/bin/sh\ncat \"$1\" >> \"$FEDNET_TEST_EVENTS\" && echo >> \"$FEDNET_TEST_EVENTS\" && cat \"$FEDNET_TEST_FIFO\" > /dev/null\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	release := func() {
		t.Helper()
		w, err := os.OpenFile(fifo, os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		w.Close()
	}
	old := startDaemon(t, []string{"client", "-hub", srv.URL, "-db", filepath.Join(dir, "client.db"), "-credential", credPath,
		"-socket", socket, "-handoff-timeout", "10s", "-hook-timeout", "1m", "-hook-env", "FEDNET_TEST_EVENTS", "-hook-env", "FEDNET_TEST_FIFO",
		"/bin/sh", script},
		"FEDNET_TEST_EVENTS="+events, "FEDNET_TEST_FIFO="+fifo)
	waitFor(t, "the client's socket", func() bool {
		_, err := os.Stat(socket)
		return err == nil
	})
	first, err := hub.Send(ctx, "workstation", []byte(`{"type":"test","text":"first"}`))
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the hook to start", func() bool { return len(hookEvents(t, events)) == 1 })

	code, res, stderr := handoffThrough(t, "client", socket)
	if code != 0 {
		t.Fatalf("handoff exited %d: %s", code, stderr)
	}
	defer terminate(t, res.To.PID)
	// The new client serves, but the old one is still here, in the hook.
	if processGone(old.cmd.Process.Pid) {
		t.Fatal("the old client exited while its hook was running")
	}
	if got := hookEvents(t, events); len(got) != 1 {
		t.Fatalf("hook events after the handoff = %v, want just the first run", got)
	}
	release()
	if code := old.wait(t); code != 0 {
		t.Fatalf("the old client exited %d, want 0", code)
	}
	second, err := hub.Send(ctx, "workstation", []byte(`{"type":"test","text":"second"}`))
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the hook for the second message", func() bool { return len(hookEvents(t, events)) >= 2 })
	release()
	if got := hookEvents(t, events); !slices.Equal(got, []string{first.MsgID, second.MsgID}) {
		t.Fatalf("hook events = %v, want %v: each message once", got, []string{first.MsgID, second.MsgID})
	}
}

// A new hub that cannot report ready, here because systemd's notification
// socket is gone, fails the handoff and leaves the old hub, and its admin
// socket, in place.
func TestHubHandoffFailsWhenSystemdIsGone(t *testing.T) {
	dir := t.TempDir()
	notifyPath := filepath.Join(dir, "notify")
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: notifyPath, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	addr := freePort(t)
	admin := filepath.Join(dir, "admin.sock")
	old := startDaemon(t, []string{"hub", "-listen", addr, "-db", filepath.Join(dir, "hub.db"), "-admin-socket", admin, "-handoff-timeout", "10s"},
		"NOTIFY_SOCKET="+notifyPath)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	if err != nil || string(buf[:n]) != "READY=1\n" {
		t.Fatalf("systemd got %q, %v; want READY=1", buf[:n], err)
	}
	waitFor(t, "the admin socket", func() bool {
		_, err := os.Stat(admin)
		return err == nil
	})
	// systemd's socket goes away: the new process cannot report ready.
	conn.Close()
	if err := os.Remove(notifyPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	code, _, stderr := handoffThrough(t, "hub", admin)
	if code != 1 || !strings.Contains(stderr, "notify systemd") {
		t.Fatalf("handoff exited %d with %q, want 1 and the notify failure", code, stderr)
	}
	if got := versionThrough(t, admin); got.PID != old.cmd.Process.Pid {
		t.Fatalf("version through the admin socket = %+v, want pid %d", got, old.cmd.Process.Pid)
	}
	if err := old.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if code := old.wait(t); code != 0 {
		t.Fatalf("the hub exited %d, want 0", code)
	}
}

// successor is a Process with a predecessor that records whether Ready was
// called; it cannot hand off itself.
type successor struct {
	handoff.None
	ready atomic.Bool
}

func (s *successor) HasParent() bool { return true }

func (s *successor) Ready() error {
	s.ready.Store(true)
	return nil
}

// A new hub with Slack reports ready only once its own connection is up:
// one whose token Slack rejects exits without reporting, so the handoff
// fails and the old hub stays; one that connects reports after that.
func TestHubReadyWaitsForSlack(t *testing.T) {
	for _, tt := range []struct {
		name     string
		fail     error
		wantCode int
	}{
		{"rejected", errors.New("invalid auth"), 1},
		{"connected", nil, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			proc := &successor{}
			prev := newProcess
			newProcess = func(time.Duration) (handoff.Process, error) { return proc, nil }
			t.Cleanup(func() { newProcess = prev })
			dir := t.TempDir()
			flags, _ := fakeSlack(t, dir, &slack.Fake{}, nil)
			connected := make(chan struct{})
			runInbound = func(ctx context.Context, _ string, r *inbound.Receiver, _ time.Duration) error {
				// Slack answers after a while, as it does.
				time.Sleep(50 * time.Millisecond)
				if tt.fail != nil {
					return tt.fail
				}
				if err := r.Connected(ctx); err != nil {
					return err
				}
				close(connected)
				<-ctx.Done()
				return ctx.Err()
			}
			var stdout, stderr syncBuffer
			stop := start(t, append([]string{"hub", "-listen", "127.0.0.1:0", "-db", filepath.Join(dir, "hub.db")}, flags...), &stdout, &stderr)
			if tt.fail == nil {
				select {
				case <-connected:
				case <-time.After(5 * time.Second):
					t.Fatal("the hub did not connect to Slack")
				}
				waitFor(t, "ready after the connection", func() bool { return proc.ready.Load() })
			} else {
				// The hub exits on its own, not on the test's stop.
				waitFor(t, "the hub to report the rejection", func() bool { return strings.Contains(stderr.String(), tt.fail.Error()) })
			}
			if code := stop(); code != tt.wantCode {
				t.Fatalf("hub exited %d, want %d; stderr %q", code, tt.wantCode, stderr.String())
			}
			if tt.fail != nil && proc.ready.Load() {
				t.Fatal("the hub reported ready although Slack rejected its token")
			}
		})
	}
}
