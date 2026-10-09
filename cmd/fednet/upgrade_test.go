package main

import (
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/Luolc/fednet/internal/auth"
	"github.com/Luolc/fednet/internal/link"
	"github.com/Luolc/fednet/internal/upgrade"
)

// fakeReleases serves a release's assets the way GitHub does, from a
// directory: /download/<version>/<asset>.
type fakeReleases struct {
	mu  sync.Mutex
	dir string
}

func (f *fakeReleases) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	dir := f.dir
	f.mu.Unlock()
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
	asset := upgrade.Asset(version, runtime.GOARCH)
	copyFile(t, binary, filepath.Join(dir, asset), 0o644)
	if err := os.WriteFile(filepath.Join(dir, upgrade.Sums), []byte(fmt.Sprintf("%x  %s\n", sum, asset)), 0o644); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.dir = filepath.Dir(dir)
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
	unchanged(t, "a download that does not match", "failed v0.2.0: upgrade: "+upgrade.Asset("v0.2.0", runtime.GOARCH)+" does not match")

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
