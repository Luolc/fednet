package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Luolc/fednet/internal/auth"
	"github.com/Luolc/fednet/internal/link"
	"github.com/Luolc/fednet/internal/upgrade"
)

// fakeReleases serves a release's assets the way GitHub does, from a
// directory: /download/<version>/<asset>; /latest answers with the
// release published last.
type fakeReleases struct {
	mu     sync.Mutex
	dir    string
	latest string
}

func (f *fakeReleases) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	dir, latest := f.dir, f.latest
	f.mu.Unlock()
	if r.URL.Path == "/latest" {
		fmt.Fprintf(w, `{"tag_name": %q}`, latest)
		return
	}
	http.ServeFile(w, r, filepath.Join(dir, filepath.FromSlash(strings.TrimPrefix(r.URL.Path, "/download/"))))
}

// publish writes a release of version whose binary for this architecture
// is the file at binary, with SHA256SUMS listing sum as its SHA-256, into
// a fresh directory the server serves from.
func (f *fakeReleases) publish(t *testing.T, version, binary string, sum []byte) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), version)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	asset := upgrade.Asset(version, runtime.GOOS, runtime.GOARCH)
	copyFile(t, binary, filepath.Join(dir, asset), 0o644)
	if err := os.WriteFile(filepath.Join(dir, upgrade.Sums), []byte(fmt.Sprintf("%x  %s\n", sum, asset)), 0o644); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.dir = filepath.Dir(dir)
	f.latest = version
	f.mu.Unlock()
}

func copyFile(t *testing.T, from, to string, mode os.FileMode) {
	t.Helper()
	b, err := os.ReadFile(from)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(to, b, mode); err != nil {
		t.Fatal(err)
	}
}

func fileSum(t *testing.T, path string) []byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	return h.Sum(nil)
}

// A client told by the hub to upgrade writes the request itself; `fednet
// upgrade` carries it out: it deletes the request, refuses a release that
// is not newer than what runs, leaves the binary alone when the download
// does not match the release's sum, and otherwise puts the release's
// binary in place and hands the client off to it, so that the socket is
// answered by a new process of the new binary.
func TestUpgradeCommand(t *testing.T) {
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
	request := filepath.Join(dir, "upgrade")
	// The client runs from a copy of this binary, which the upgrade
	// replaces with another copy, served as the release.
	bin := filepath.Join(dir, "bin", "fednet")
	if err := os.MkdirAll(filepath.Dir(bin), 0o755); err != nil {
		t.Fatal(err)
	}
	copyFile(t, fednetBinary(t), bin, 0o755)
	old := startDaemonAt(t, bin, []string{"client", "-hub", srv.URL, "-db", filepath.Join(dir, "client.db"), "-credential", credPath,
		"-socket", socket, "-handoff-timeout", "10s", "-upgrade-request", request}, testVersion+"=v0.1.0")
	waitFor(t, "the client's socket", func() bool {
		_, err := os.Stat(socket)
		return err == nil
	})
	if got := versionThrough(t, socket); got.Version != "v0.1.0" {
		t.Fatalf("the client reports %q, want v0.1.0", got.Version)
	}
	rel := &fakeReleases{}
	relSrv := httptest.NewServer(rel)
	defer relSrv.Close()
	releases = upgrade.Releases{Latest: relSrv.URL + "/latest", Download: relSrv.URL + "/download"}
	t.Cleanup(func() {
		releases = upgrade.Releases{Latest: "http://127.0.0.1:9/latest", Download: "http://127.0.0.1:9/download"}
	})
	before := fileSum(t, bin)
	unchanged := func(t *testing.T, what, result string) {
		t.Helper()
		if _, err := os.Stat(request); err == nil {
			t.Fatalf("after %s the request is still there", what)
		}
		if got := fileSum(t, bin); string(got) != string(before) {
			t.Fatalf("after %s the binary changed", what)
		}
		if _, err := os.Stat(upgrade.Previous(bin)); err == nil {
			t.Fatalf("after %s a previous binary is left behind", what)
		}
		if got := versionThrough(t, socket); got.PID != old.cmd.Process.Pid {
			t.Fatalf("after %s the socket is answered by pid %d, want the old process %d", what, got.PID, old.cmd.Process.Pid)
		}
		if got := upgrade.ReadResult(request); !strings.Contains(got, result) {
			t.Fatalf("after %s the result is %q, want %q in it", what, got, result)
		}
	}
	args := []string{"upgrade", "-request", request, "-binary", bin, "-socket", socket}

	// No request: nothing to do.
	var stdout, stderr syncBuffer
	if code := run(ctx, args, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), "no upgrade request") {
		t.Fatalf("upgrade without a request: exit %d, stdout %q, stderr %q", code, stdout.String(), stderr.String())
	}
	// The hub tells the client to upgrade to what it runs: the notice is
	// dropped, nothing is requested.
	if _, err := hub.Send(ctx, "workstation", []byte(`{"type":"upgrade","version":"v0.1.0"}`)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the hub outbox to be acked", func() bool {
		ds, err := hs.Outbox.After(ctx, "workstation", 0)
		return err == nil && len(ds) == 0
	})
	if _, err := os.Stat(request); err == nil {
		t.Fatal("the client requested an upgrade to the release it runs")
	}
	// A request that is not newer is refused, and deleted.
	if err := upgrade.WriteRequest(request, "v0.1.0"); err != nil {
		t.Fatal(err)
	}
	if code := run(ctx, args, &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "not newer than the running v0.1.0") {
		t.Fatalf("upgrade to the running release: exit %d, stderr %q", code, stderr.String())
	}
	unchanged(t, "a refused request", "failed v0.1.0: refusing")

	// The hub tells the client to upgrade: the client writes the request.
	if _, err := hub.Send(ctx, "workstation", []byte(`{"type":"upgrade","version":"v0.2.0"}`)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the client to request the upgrade", func() bool {
		v, err := upgrade.ReadRequest(request)
		return err == nil && v == "v0.2.0"
	})
	// The download does not match the sum: the binary stays.
	rel.publish(t, "v0.2.0", fednetBinary(t), sha256.New().Sum(nil))
	stderr = syncBuffer{}
	if code := run(ctx, args, &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "does not match its sum") {
		t.Fatalf("upgrade with a bad sum: exit %d, stderr %q", code, stderr.String())
	}
	unchanged(t, "a download that does not match", "failed v0.2.0: upgrade: "+upgrade.Asset("v0.2.0", runtime.GOOS, runtime.GOARCH)+" does not match")

	// The release's binary cannot start: the handoff fails, the old binary
	// is put back, and it still starts.
	if err := upgrade.WriteRequest(request, "v0.2.0"); err != nil {
		t.Fatal(err)
	}
	broken := filepath.Join(dir, "broken")
	if err := os.WriteFile(broken, []byte("#!/bin/sh\necho 'cannot start' >&2\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	rel.publish(t, "v0.2.0", broken, fileSum(t, broken))
	stderr = syncBuffer{}
	if code := run(ctx, args, &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "rolled back to v0.1.0") {
		t.Fatalf("upgrade to a binary that cannot start: exit %d, stderr %q", code, stderr.String())
	}
	unchanged(t, "a handoff that failed", "rolled back to v0.1.0")
	check := exec.Command(bin, "version")
	check.Env = append(os.Environ(), asFednet+"=1")
	if out, err := check.Output(); err != nil || strings.TrimSpace(string(out)) != version {
		t.Fatalf("the restored binary printed %q, %v; want %q", out, err, version)
	}

	// The release is good: the binary is replaced and the client handed
	// off to a process of it.
	if err := upgrade.WriteRequest(request, "v0.2.0"); err != nil {
		t.Fatal(err)
	}
	rel.publish(t, "v0.2.0", fednetBinary(t), fileSum(t, fednetBinary(t)))
	stdout, stderr = syncBuffer{}, syncBuffer{}
	if code := run(ctx, args, &stdout, &stderr); code != 0 {
		t.Fatalf("upgrade: exit %d, stderr %q", code, stderr.String())
	}
	if _, err := os.Stat(request); err == nil {
		t.Fatal("the request is still there after the upgrade")
	}
	if got := upgrade.ReadResult(request); got != "ok v0.2.0" {
		t.Fatalf("the result is %q, want ok v0.2.0", got)
	}
	if _, err := os.Stat(upgrade.Previous(bin)); err == nil {
		t.Fatal("the previous binary is left behind after the upgrade")
	}
	if got, want := fileSum(t, bin), fileSum(t, fednetBinary(t)); string(got) != string(want) {
		t.Fatal("the binary is not the release's")
	}
	now := versionThrough(t, socket)
	if now.PID == 0 || now.PID == old.cmd.Process.Pid {
		t.Fatalf("the socket is answered by pid %d, want a new process", now.PID)
	}
	defer terminate(t, now.PID)
	if !strings.Contains(stdout.String(), fmt.Sprintf("upgraded from v0.1.0 (pid %d) to v0.1.0 (pid %d)", old.cmd.Process.Pid, now.PID)) {
		t.Fatalf("upgrade printed %q", stdout.String())
	}
	if code := old.wait(t); code != 0 {
		t.Fatalf("the old client exited %d, want 0", code)
	}
	if exe, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", now.PID)); err != nil || exe != bin {
		t.Fatalf("the new process runs %q, %v; want %s", exe, err, bin)
	}
}

// supervisor runs a daemon from a binary and starts it again each time it
// exits, a short while later, as launchd does with KeepAlive.
type supervisor struct {
	mu sync.Mutex
	// held, when not nil, holds the next start until it is closed.
	held chan struct{}
	// down gets the pid of each process that exits, while it has room.
	down chan int
	stop chan struct{}
	done chan struct{}
	out  *syncBuffer
}

func supervise(t *testing.T, path string, args []string, env ...string) *supervisor {
	t.Helper()
	s := &supervisor{down: make(chan int, 64), stop: make(chan struct{}), done: make(chan struct{}), out: &syncBuffer{}}
	go func() {
		defer close(s.done)
		for {
			cmd := exec.Command(path, args...)
			cmd.Stdout, cmd.Stderr = s.out, s.out
			cmd.Env = append(append(os.Environ(), asFednet+"=1"), env...)
			if err := cmd.Start(); err != nil {
				t.Error(err)
				return
			}
			exited := make(chan struct{})
			go func() {
				cmd.Wait()
				close(exited)
			}()
			select {
			case <-exited:
			case <-s.stop:
				cmd.Process.Signal(syscall.SIGTERM)
				<-exited
				return
			}
			select {
			case s.down <- cmd.Process.Pid:
			default:
			}
			s.mu.Lock()
			held := s.held
			s.held = nil
			s.mu.Unlock()
			if held == nil {
				held = make(chan struct{})
				close(held)
			}
			select {
			case <-held:
			case <-s.stop:
				return
			}
			select {
			case <-time.After(50 * time.Millisecond):
			case <-s.stop:
				return
			}
		}
	}()
	t.Cleanup(func() {
		close(s.stop)
		<-s.done
		if t.Failed() {
			t.Logf("output of the supervised fednet %s:\n%s", args[0], s.out.String())
		}
	})
	return s
}

// hold holds the next start until the returned channel is closed.
func (s *supervisor) hold() chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.held = make(chan struct{})
	return s.held
}

// versionScript writes a script that runs this binary as fednet of
// version, a release binary as far as the tests can tell.
func versionScript(t *testing.T, path, version string) {
	t.Helper()
	script := fmt.Sprintf("#!/bin/sh\n%s=%s exec '%s' \"$@\"\n", testVersion, version, fednetBinary(t))
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
}

// With -restart, the default on darwin, `fednet upgrade` does not hand
// the client off: it stops it, and what supervises the client, launchd
// there and a loop here, starts the new binary. The hub's upgrade sees the
// client on the release by the version it reports when it connects again,
// and a message sent while it was down reaches its hook once it is back.
// A release that does not come up, because it cannot start or answers with
// another version, is rolled back: the old binary is put back, a process
// of the other version stopped, and the old binary started again.
func TestUpgradeByRestart(t *testing.T) {
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
	newCredential(t, hs, "laptop", credPath)
	socket := filepath.Join(dir, "fednet.sock")
	request := filepath.Join(dir, "upgrade")
	script := filepath.Join(dir, "hook.sh")
	events := filepath.Join(dir, "events")
	if err := os.WriteFile(script, []byte("#!/bin/sh\ncat \"$1\" >> \"$FEDNET_TEST_EVENTS\" && echo >> \"$FEDNET_TEST_EVENTS\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(dir, "bin", "fednet")
	if err := os.MkdirAll(filepath.Dir(bin), 0o755); err != nil {
		t.Fatal(err)
	}
	versionScript(t, bin, "v0.1.0")
	sup := supervise(t, bin, []string{"client", "-hub", srv.URL, "-db", filepath.Join(dir, "client.db"), "-credential", credPath,
		"-socket", socket, "-upgrade-request", request, "-hook-env", "FEDNET_TEST_EVENTS", "/bin/sh", script},
		"FEDNET_TEST_EVENTS="+events)
	waitFor(t, "the client to connect", func() bool { return hub.Online("laptop") })
	old := versionThrough(t, socket)
	if old.Version != "v0.1.0" {
		t.Fatalf("the client reports %q, want v0.1.0", old.Version)
	}

	rel := &fakeReleases{}
	relSrv := httptest.NewServer(rel)
	defer relSrv.Close()
	releases = upgrade.Releases{Latest: relSrv.URL + "/latest", Download: relSrv.URL + "/download"}
	t.Cleanup(func() {
		releases = upgrade.Releases{Latest: "http://127.0.0.1:9/latest", Download: "http://127.0.0.1:9/download"}
	})
	two := filepath.Join(dir, "two")
	versionScript(t, two, "v0.2.0")
	rel.publish(t, "v0.2.0", two, fileSum(t, two))

	// The hub starts an upgrade to v0.2.0, as `client ops upgrade` would.
	said := make(chan string, 64)
	ups := &upgrade.Hub{
		Store: hs, Version: "v0.1.0", Releases: releases, Online: hub.Online,
		Send: func(ctx context.Context, client string, p []byte) error {
			_, err := hub.Send(ctx, client, p)
			return err
		},
		Request: filepath.Join(dir, "hub-upgrade"), Ops: map[string][]string{upgrade.OpUpgrade: {"laptop"}},
		Wait: 30 * time.Second, Poll: 20 * time.Millisecond,
		Alert: func(_ context.Context, text string) error {
			said <- text
			return nil
		},
	}
	runCtx, stopRun := context.WithCancel(ctx)
	ran := make(chan struct{})
	go func() {
		defer close(ran)
		ups.Run(runCtx)
	}()
	t.Cleanup(func() {
		stopRun()
		<-ran
	})
	await := func(t *testing.T, want string) {
		t.Helper()
		for {
			select {
			case s := <-said:
				if strings.Contains(s, want) {
					return
				}
			case <-time.After(15 * time.Second):
				t.Fatalf("the hub did not say %q", want)
			}
		}
	}
	if _, err := ups.Op(ctx, "laptop", upgrade.OpUpgrade); err != nil {
		t.Fatal(err)
	}
	await(t, "先升 client laptop")
	waitFor(t, "the client to request the upgrade", func() bool {
		v, err := upgrade.ReadRequest(request)
		return err == nil && v == "v0.2.0"
	})

	// The upgrader stops the client; while it is down, a message is sent.
	held := sup.hold()
	type outcome struct {
		code           int
		stdout, stderr string
	}
	upgraded := make(chan outcome, 1)
	upgradeArgs := []string{"upgrade", "-request", request, "-binary", bin, "-socket", socket, "-restart", "-restart-timeout", "10s"}
	runUpgrade := func() {
		var stdout, stderr syncBuffer
		code := run(ctx, upgradeArgs, &stdout, &stderr)
		upgraded <- outcome{code, stdout.String(), stderr.String()}
	}
	go runUpgrade()
	select {
	case pid := <-sup.down:
		if pid != old.PID {
			t.Fatalf("pid %d exited, want the client's %d", pid, old.PID)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the upgrade did not stop the client")
	}
	during, err := hub.Send(ctx, "laptop", []byte(`{"type":"test","text":"while down"}`))
	if err != nil {
		t.Fatal(err)
	}
	close(held)
	var o outcome
	select {
	case o = <-upgraded:
	case <-time.After(20 * time.Second):
		t.Fatal("the upgrade did not return")
	}
	now := versionThrough(t, socket)
	if o.code != 0 || now.Version != "v0.2.0" || !strings.Contains(o.stdout, fmt.Sprintf("upgraded from v0.1.0 (pid %d) to v0.2.0 (pid %d)", old.PID, now.PID)) {
		t.Fatalf("upgrade: exit %d, stdout %q, stderr %q; the socket answers %+v", o.code, o.stdout, o.stderr, now)
	}
	if got := upgrade.ReadResult(request); got != "ok v0.2.0" {
		t.Fatalf("the result is %q, want ok v0.2.0", got)
	}
	if _, err := os.Stat(upgrade.Previous(bin)); err == nil {
		t.Fatal("the previous binary is left behind after the upgrade")
	}
	if got, want := fileSum(t, bin), fileSum(t, two); string(got) != string(want) {
		t.Fatal("the binary is not the release's")
	}
	// The hub saw the client come back on the release, and goes on to
	// upgrade itself.
	await(t, "client 都已是 v0.2.0，hub 开始从 v0.1.0 升级")
	if v, err := upgrade.ReadRequest(ups.Request); err != nil || v != "v0.2.0" {
		t.Fatalf("the hub's request is %q, %v; want v0.2.0", v, err)
	}
	waitFor(t, "the message sent while the client was down at the hook", func() bool {
		return slices.Contains(hookEvents(t, events), during.MsgID)
	})
	waitFor(t, "the hub outbox to be acked", func() bool {
		ds, err := hs.Outbox.After(ctx, "laptop", 0)
		return err == nil && len(ds) == 0
	})

	// Releases that do not come up: one cannot start, the other answers
	// with a version other than its own.
	for _, c := range []struct {
		name, script, why string
	}{
		{"a binary that cannot start", "#!/bin/sh\necho 'cannot start' >&2\nexit 1\n", "no new process answers"},
		{"a binary of another version", "", "with v0.9.0, not v0.3.0"},
	} {
		three := filepath.Join(t.TempDir(), "three")
		if c.script == "" {
			versionScript(t, three, "v0.9.0")
		} else if err := os.WriteFile(three, []byte(c.script), 0o755); err != nil {
			t.Fatal(err)
		}
		rel.publish(t, "v0.3.0", three, fileSum(t, three))
		if err := upgrade.WriteRequest(request, "v0.3.0"); err != nil {
			t.Fatal(err)
		}
		before := versionThrough(t, socket)
		upgradeArgs = []string{"upgrade", "-request", request, "-binary", bin, "-socket", socket, "-restart", "-restart-timeout", "2s"}
		runUpgrade()
		o := <-upgraded
		if o.code != 1 || !strings.Contains(o.stderr, c.why) || !strings.Contains(o.stderr, "rolled back to v0.2.0, which pid") {
			t.Fatalf("upgrade to %s: exit %d, stderr %q", c.name, o.code, o.stderr)
		}
		if got := upgrade.ReadResult(request); !strings.Contains(got, "failed v0.3.0: the restart into v0.3.0 failed") || !strings.Contains(got, "rolled back to v0.2.0") {
			t.Fatalf("after %s the result is %q", c.name, got)
		}
		if got, want := fileSum(t, bin), fileSum(t, two); string(got) != string(want) {
			t.Fatalf("after %s the binary is not the one before", c.name)
		}
		if _, err := os.Stat(upgrade.Previous(bin)); err == nil {
			t.Fatalf("after %s a previous binary is left behind", c.name)
		}
		after := versionThrough(t, socket)
		if after.Version != "v0.2.0" || after.PID == before.PID {
			t.Fatalf("after %s the socket answers %+v, want a new process of v0.2.0 (not pid %d)", c.name, after, before.PID)
		}
	}
}
