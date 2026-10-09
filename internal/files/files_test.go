package files

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Luolc/fednet/internal/link"
	"github.com/Luolc/fednet/internal/payload"
)

// hub is a stand-in for the hub: files by id, each fetch counted; a file
// listed in short is served short of its size, as a download the hub broke
// off is.
type hub struct {
	mu      sync.Mutex
	files   map[string]link.File
	content map[string][]byte
	fetched map[string]int
	short   map[string]bool
	// block, if set, is closed to let a fetch go on; a fetch waits for it.
	block chan struct{}
}

func newHub() *hub {
	return &hub{files: map[string]link.File{}, content: map[string][]byte{}, fetched: map[string]int{}, short: map[string]bool{}}
}

func (h *hub) add(id, name string, content []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.files[id] = link.File{Name: name, Mimetype: "application/octet-stream", Size: int64(len(content))}
	h.content[id] = content
}

func (h *hub) fetch(ctx context.Context, id string) (link.File, io.ReadCloser, error) {
	h.mu.Lock()
	f, ok := h.files[id]
	content := h.content[id]
	h.fetched[id]++
	short := h.short[id]
	block := h.block
	h.mu.Unlock()
	if !ok {
		return link.File{}, nil, link.Refuse(link.ErrNotFound, "file %s no longer exists in Slack", id)
	}
	if short {
		content = content[:len(content)/2]
	}
	return f, &blocked{bytes.NewReader(content), block, ctx}, nil
}

// blocked is a download whose first byte waits for block, when that is
// set: the store has made the directory and the temporary file by then.
type blocked struct {
	io.Reader
	block <-chan struct{}
	ctx   context.Context
}

func (b *blocked) Read(p []byte) (int, error) {
	if b.block != nil {
		select {
		case <-b.block:
		case <-b.ctx.Done():
			return 0, b.ctx.Err()
		}
	}
	return b.Reader.Read(p)
}

func (b *blocked) Close() error { return nil }

func (h *hub) count(id string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.fetched[id]
}

func newStore(t *testing.T, h *hub) *Store {
	t.Helper()
	return &Store{Dir: filepath.Join(t.TempDir(), "files"), Fetch: h.fetch, Timeout: 200 * time.Millisecond}
}

func TestGet(t *testing.T) {
	h := newHub()
	h.add("F1", "shot.png", []byte("PNG..."))
	s := newStore(t, h)
	ctx := t.Context()

	path, err := s.Get(ctx, "F1")
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(s.Dir, "F1", "shot.png"); path != want {
		t.Fatalf("Get = %q, want %q", path, want)
	}
	if b, err := os.ReadFile(path); err != nil || string(b) != "PNG..." {
		t.Fatalf("file holds %q, %v; want the content", b, err)
	}
	// The file and its directories are this user's alone.
	for p, want := range map[string]os.FileMode{s.Dir: 0o700, filepath.Dir(path): 0o700, path: 0o600} {
		if fi, err := os.Stat(p); err != nil || fi.Mode().Perm() != want {
			t.Errorf("%s: mode %v, %v; want %04o", p, fi.Mode(), err, want)
		}
	}
	// A second Get is a cache hit: nothing fetched again, the file counts
	// as fetched now.
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	if again, err := s.Get(ctx, "F1"); err != nil || again != path || h.count("F1") != 1 {
		t.Fatalf("second Get = %q, %v, fetched %d times; want the same path from the cache", again, err, h.count("F1"))
	}
	if fi, err := os.Stat(path); err != nil || !fi.ModTime().After(old) {
		t.Fatalf("a cache hit left the file's time at %v, want it refreshed", fi.ModTime())
	}
	// Nothing is left of a fetch that breaks off: no partial file, and the
	// next Get fetches again.
	h.add("F2", "doc.pdf", []byte("a long document"))
	h.short["F2"] = true
	if _, err := s.Get(ctx, "F2"); err == nil || !strings.Contains(err.Error(), "got 7 bytes of F2, the hub said 15") {
		t.Fatalf("Get of a file served short = %v, want an error saying so", err)
	}
	if es, _ := os.ReadDir(filepath.Join(s.Dir, "F2")); len(es) != 0 {
		t.Fatalf("a failed fetch left %v behind", es)
	}
	h.short["F2"] = false
	if _, err := s.Get(ctx, "F2"); err != nil || h.count("F2") != 2 {
		t.Fatalf("Get after a failed fetch = %v, fetched %d times; want it fetched again", err, h.count("F2"))
	}
	// The hub's refusal comes through as it is; nothing is created.
	_, err = s.Get(ctx, "F9")
	if !errors.Is(err, link.ErrNotFound) || !strings.Contains(err.Error(), "no longer exists") {
		t.Fatalf("Get of a missing file = %v, want the hub's not-found refusal", err)
	}
	if _, err := os.Stat(filepath.Join(s.Dir, "F9")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a refused fetch made a directory: %v", err)
	}
	// An id that could name a path is refused before anything is touched.
	if _, err := s.Get(ctx, "../etc"); !errors.Is(err, link.ErrBadRequest) || h.count("../etc") != 0 {
		t.Fatalf("Get(../etc) = %v, fetched %d times; want a bad request and no fetch", err, h.count("../etc"))
	}
}

// Two Gets of one file at once fetch it once.
func TestGetOnce(t *testing.T) {
	h := newHub()
	h.add("F1", "shot.png", []byte("PNG..."))
	h.block = make(chan struct{})
	s := newStore(t, h)
	paths := make(chan string, 2)
	for range 2 {
		go func() {
			p, err := s.Get(t.Context(), "F1")
			if err != nil {
				t.Error(err)
			}
			paths <- p
		}()
	}
	time.Sleep(20 * time.Millisecond)
	close(h.block)
	a, b := <-paths, <-paths
	if a != b || h.count("F1") != 1 {
		t.Fatalf("paths %q and %q, fetched %d times; want one path, fetched once", a, b, h.count("F1"))
	}
}

func TestSafeName(t *testing.T) {
	long := strings.Repeat("é", 200) + ".png" // 404 bytes
	tests := map[string]string{
		"shot.png":               "shot.png",
		"../../etc/passwd":       "....etcpasswd",
		"a\\b\x00c\n.txt":        "abc.txt",
		"":                       "file",
		"..":                     "file",
		"/":                      "file",
		long:                     strings.Repeat("é", 125) + ".png",
		strings.Repeat("x", 300): strings.Repeat("x", 255),
	}
	for name, want := range tests {
		if got := safeName(name); got != want {
			t.Errorf("safeName(%q) = %q, want %q", name, got, want)
		}
	}
}

func TestAttach(t *testing.T) {
	h := newHub()
	h.add("F1", "shot.png", []byte("PNG..."))
	h.add("F3", "big.zip", []byte("zip"))
	s := newStore(t, h)
	// Unknown fields of the payload and a file not marked survive; a
	// marked file the hub does not have gets an error, the message goes on.
	raw := []byte(`{"type":"message","thread":"C1/1.1","later":true,"files":[` +
		`{"id":"F1","name":"shot.png","mimetype":"image/png","size":6,"url":"https://example.invalid/F1","fetch":true},` +
		`{"id":"F2","name":"gone.png","mimetype":"image/png","size":6,"url":"https://example.invalid/F2","fetch":true},` +
		`{"id":"F3","name":"big.zip","mimetype":"application/zip","size":3,"url":"https://example.invalid/F3"}]}`)
	var m struct {
		Later bool           `json:"later"`
		Files []payload.File `json:"files"`
	}
	if err := json.Unmarshal(s.Attach(t.Context(), raw), &m); err != nil {
		t.Fatal(err)
	}
	want := []payload.File{
		{ID: "F1", Name: "shot.png", Mimetype: "image/png", Size: 6, URL: "https://example.invalid/F1", Fetch: true, Path: filepath.Join(s.Dir, "F1", "shot.png")},
		{ID: "F2", Name: "gone.png", Mimetype: "image/png", Size: 6, URL: "https://example.invalid/F2", Fetch: true, Error: "file F2 no longer exists in Slack"},
		{ID: "F3", Name: "big.zip", Mimetype: "application/zip", Size: 3, URL: "https://example.invalid/F3"},
	}
	if !m.Later || len(m.Files) != 3 {
		t.Fatalf("Attach dropped a field: %+v", m)
	}
	for i := range want {
		if m.Files[i] != want[i] {
			t.Errorf("file %d = %+v, want %+v", i, m.Files[i], want[i])
		}
	}
	if h.count("F3") != 0 {
		t.Fatal("a file not marked was fetched")
	}
	// A payload with nothing to fetch, or that is not a message, is
	// returned as it is, byte for byte.
	for _, raw := range []string{`{"type":"message","files":[{"id":"F1","name":"x"}]}`, `{"type":"approval","files":[{"id":"F1","fetch":true}]}`, `not json`} {
		if got := s.Attach(t.Context(), []byte(raw)); string(got) != raw {
			t.Errorf("Attach(%s) = %s, want it unchanged", raw, got)
		}
	}
	// A hub that does not answer holds the message up for Timeout, not
	// longer; the file gets the error.
	h.add("F4", "slow.png", []byte("PNG"))
	h.block = make(chan struct{})
	raw = []byte(`{"type":"message","files":[{"id":"F4","name":"slow.png","fetch":true}]}`)
	start := time.Now()
	out := s.Attach(t.Context(), raw)
	if d := time.Since(start); d < s.Timeout || d > 2*time.Second {
		t.Fatalf("Attach took %v, want about %v", d, s.Timeout)
	}
	if !strings.Contains(string(out), `"error":"fetching F4: context deadline exceeded"`) || strings.Contains(string(out), `"path"`) {
		t.Fatalf("Attach after the timeout = %s, want the file with an error and no path", out)
	}
}

func TestPrune(t *testing.T) {
	h := newHub()
	s := newStore(t, h)
	s.Retention, s.MaxTotal = time.Hour, 100
	now := time.Unix(1700000000, 0)
	s.Now = func() time.Time { return now }
	ctx := t.Context()
	fetch := func(id string, size int, at time.Time) string {
		t.Helper()
		h.add(id, id+".bin", bytes.Repeat([]byte("x"), size))
		now = at
		p, err := s.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	exists := func(id string) bool {
		_, err := os.Stat(filepath.Join(s.Dir, id))
		return err == nil
	}
	base := time.Unix(1700000000, 0)
	// Within the limit nothing is pruned but what expired.
	fetch("F1", 10, base.Add(-2*time.Hour))
	fetch("F2", 10, base.Add(-30*time.Minute))
	now = base
	if err := s.Prune(); err != nil {
		t.Fatal(err)
	}
	if exists("F1") || !exists("F2") {
		t.Fatalf("after pruning: F1 %v, F2 %v; want only the expired F1 gone", exists("F1"), exists("F2"))
	}
	// Over the limit, the longest unfetched go until the total is down to
	// 80% of it: 10 + 50 + 45 = 105 > 100; F2, the oldest, goes, then F3,
	// since 95 is still over 80.
	fetch("F3", 50, base.Add(-20*time.Minute))
	fetch("F4", 45, base.Add(-10*time.Minute))
	now = base
	if err := s.Prune(); err != nil {
		t.Fatal(err)
	}
	if exists("F2") || exists("F3") || !exists("F4") {
		t.Fatalf("after pruning over the limit: F2 %v, F3 %v, F4 %v; want only F4 left", exists("F2"), exists("F3"), exists("F4"))
	}
	// A file being fetched is not pruned, however old its directory, and a
	// leftover of a fetch that never finished is.
	h.add("F5", "F5.bin", []byte("xxxxxxxxxx"))
	h.block = make(chan struct{})
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		_, err := s.Get(ctx, "F5")
		done <- err
	}()
	<-started
	time.Sleep(20 * time.Millisecond)
	if err := os.MkdirAll(filepath.Join(s.Dir, "F6"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.Dir, "F6", tmpPrefix+"123"), []byte("part"), 0o600); err != nil {
		t.Fatal(err)
	}
	now = base.Add(3 * time.Hour)
	if err := s.Prune(); err != nil {
		t.Fatal(err)
	}
	if exists("F6") {
		t.Fatal("the leftover F6 was not pruned")
	}
	if es, _ := os.ReadDir(filepath.Join(s.Dir, "F5")); len(es) != 1 || !strings.HasPrefix(es[0].Name(), tmpPrefix) {
		t.Fatalf("F5 during its fetch holds %v, want just the file being written", es)
	}
	close(h.block)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join(s.Dir, "F5", "F5.bin")); err != nil || len(b) != 10 {
		t.Fatalf("F5 after a prune during its fetch: %q, %v; want it whole", b, err)
	}
	// After the retention from its fetch it goes too.
	now = now.Add(2 * time.Hour)
	if err := s.Prune(); err != nil {
		t.Fatal(err)
	}
	if exists("F5") {
		t.Fatal("F5 was not pruned after the retention")
	}
}
