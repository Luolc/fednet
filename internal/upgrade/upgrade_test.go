package upgrade

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// fakeReleases is a release source like GitHub's: /latest answers with the
// latest version, /download/<version>/<asset> with the assets of each
// release.
type fakeReleases struct {
	mu sync.Mutex
	// latest is the tag /latest reports.
	latest string
	// assets maps "<version>/<asset>" to its bytes.
	assets map[string][]byte
	// fetches counts the requests.
	fetches atomic.Int64
	srv     *httptest.Server
}

func newFakeReleases(t *testing.T) *fakeReleases {
	t.Helper()
	f := &fakeReleases{assets: make(map[string][]byte)}
	f.srv = httptest.NewServer(f)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeReleases) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.fetches.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.URL.Path == "/latest" {
		fmt.Fprintf(w, `{"tag_name": %q, "assets": []}`, f.latest)
		return
	}
	b, ok := f.assets[strings.TrimPrefix(r.URL.Path, "/download/")]
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Write(b)
}

// releases returns the Releases that point at f.
func (f *fakeReleases) releases() Releases {
	return Releases{Latest: f.srv.URL + "/latest", Download: f.srv.URL + "/download"}
}

// publish adds a release of version with a binary for each arch in
// binaries, and SHA256SUMS listing them as sha256sum does, and makes it
// the latest.
func (f *fakeReleases) publish(version string, binaries map[string][]byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var sums strings.Builder
	for arch, b := range binaries {
		f.assets[version+"/"+Asset(version, arch)] = b
		fmt.Fprintf(&sums, "%x  %s\n", sha256.Sum256(b), Asset(version, arch))
	}
	f.assets[version+"/"+Sums] = []byte(sums.String())
	f.latest = version
}

// set replaces one asset's bytes.
func (f *fakeReleases) set(version, asset string, b []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.assets[version+"/"+asset] = b
}

func TestLatestVersion(t *testing.T) {
	f := newFakeReleases(t)
	f.publish("v0.2.0", map[string][]byte{"amd64": []byte("two")})
	v, err := f.releases().LatestVersion(t.Context())
	if err != nil || v != "v0.2.0" {
		t.Fatalf("LatestVersion = %q, %v; want v0.2.0", v, err)
	}
	f.mu.Lock()
	f.latest = "nightly"
	f.mu.Unlock()
	if _, err := f.releases().LatestVersion(t.Context()); err == nil {
		t.Fatal("a latest release that is not a release was accepted")
	}
	if _, err := (Releases{Latest: "http://127.0.0.1:9/latest"}).LatestVersion(t.Context()); err == nil {
		t.Fatal("an unreachable source gave a version")
	}
}

// Install puts the release's binary for the architecture in place of the
// old one, executable; it leaves the old one untouched when the sum does
// not match, when the release has no binary for the architecture, when
// the download fails, or when the version is not a release.
func TestInstall(t *testing.T) {
	ctx := t.Context()
	f := newFakeReleases(t)
	f.publish("v0.2.0", map[string][]byte{"amd64": []byte("binary for amd64"), "arm64": []byte("binary for arm64")})
	dir := t.TempDir()
	path := filepath.Join(dir, "fednet")
	if err := os.WriteFile(path, []byte("old binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	old := func(t *testing.T, err error) {
		t.Helper()
		if err == nil {
			t.Fatal("Install did not fail")
		}
		if b, _ := os.ReadFile(path); string(b) != "old binary" {
			t.Fatalf("the binary is %q after a failed Install, want the old one", b)
		}
		if left, _ := filepath.Glob(filepath.Join(dir, ".fednet.upgrade-*")); len(left) != 0 {
			t.Fatalf("a failed Install left %v behind", left)
		}
	}
	old(t, f.releases().Install(ctx, "v0.2.0", "riscv64", path))
	old(t, f.releases().Install(ctx, "v0.3.0", "amd64", path))
	old(t, f.releases().Install(ctx, "nightly", "amd64", path))
	f.set("v0.2.0", Asset("v0.2.0", "arm64"), []byte("tampered"))
	old(t, f.releases().Install(ctx, "v0.2.0", "arm64", path))
	f.set("v0.2.0", Sums, []byte("nonsense\n"))
	old(t, f.releases().Install(ctx, "v0.2.0", "amd64", path))

	f.publish("v0.2.0", map[string][]byte{"amd64": []byte("binary for amd64")})
	if err := f.releases().Install(ctx, "v0.2.0", "amd64", path); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil || string(b) != "binary for amd64" {
		t.Fatalf("the binary is %q, %v after Install, want the release's", b, err)
	}
	if fi, _ := os.Stat(path); fi.Mode().Perm() != 0o755 {
		t.Fatalf("the binary's mode is %04o, want 0755", fi.Mode().Perm())
	}
	// A sums file written with sha256sum -b names the file with a star.
	f.set("v0.2.0", Sums, []byte(fmt.Sprintf("%x *%s\n", sha256.Sum256([]byte("binary for amd64")), Asset("v0.2.0", "amd64"))))
	if err := f.releases().Install(ctx, "v0.2.0", "amd64", path); err != nil {
		t.Fatal(err)
	}
}

// The binary at the path is whole at every moment of an Install: a reader
// that opens it while the download is on sees the old binary to the end.
func TestInstallIsAtomic(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	path := filepath.Join(dir, "fednet")
	if err := os.WriteFile(path, []byte("old binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	// The release source serves the binary in two halves, and between
	// them the test reads what is at the path.
	seen := make(chan string, 1)
	resume := make(chan struct{})
	content := []byte("new binary, in two halves")
	sums := fmt.Sprintf("%x  %s\n", sha256.Sum256(content), Asset("v0.2.0", "amd64"))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, Sums) {
			fmt.Fprint(w, sums)
			return
		}
		w.Write(content[:10])
		w.(http.Flusher).Flush()
		b, _ := os.ReadFile(path)
		seen <- string(b)
		<-resume
		w.Write(content[10:])
	}))
	defer srv.Close()
	rel := Releases{Download: srv.URL + "/download"}
	done := make(chan error, 1)
	go func() { done <- rel.Install(ctx, "v0.2.0", "amd64", path) }()
	if got := <-seen; got != "old binary" {
		t.Fatalf("during the download the binary is %q, want the old one whole", got)
	}
	close(resume)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(path); string(b) != string(content) {
		t.Fatalf("after Install the binary is %q, want the new one whole", b)
	}
}

func TestRequestFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "upgrade")
	if _, err := ReadRequest(path); !errors.Is(err, ErrNoRequest) {
		t.Fatalf("ReadRequest without a file = %v, want ErrNoRequest", err)
	}
	if err := WriteRequest(path, "dev"); err == nil {
		t.Fatal("a request for a version that is not a release was written")
	}
	if err := WriteRequest(path, "v0.2.0"); err != nil {
		t.Fatal(err)
	}
	if v, err := ReadRequest(path); err != nil || v != "v0.2.0" {
		t.Fatalf("ReadRequest = %q, %v; want v0.2.0", v, err)
	}
	if err := WriteRequest(path, "v0.3.0"); err != nil {
		t.Fatal(err)
	}
	if v, _ := ReadRequest(path); v != "v0.3.0" {
		t.Fatalf("ReadRequest after a second write = %q, want v0.3.0", v)
	}
	if err := os.WriteFile(path, []byte("whatever\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadRequest(path); err == nil {
		t.Fatal("a request that names no release was read")
	}
	if left, _ := filepath.Glob(filepath.Join(filepath.Dir(path), ".upgrade-*")); len(left) != 0 {
		t.Fatalf("WriteRequest left %v behind", left)
	}
}
