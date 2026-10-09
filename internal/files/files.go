// Package files is the client's cache of the files people upload in
// Slack: the hub streams a file's content from Slack on request, and the
// client keeps it on disk under the file's Slack id, where the agent reads
// it by path. Slack keeps the file; what is here can be fetched again
// after it is pruned. Store fetches, keeps and prunes; Attach fetches the
// files the hub marked before a message goes to the hook.
package files

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Luolc/fednet/internal/link"
	"github.com/Luolc/fednet/internal/payload"
)

// Defaults for the zero fields of Store.
const (
	DefaultRetention     = 7 * 24 * time.Hour
	DefaultMaxTotal      = 2 << 30
	DefaultTimeout       = 30 * time.Second
	DefaultPruneInterval = time.Hour
	// pruneTo is the share of MaxTotal pruning brings the total down to.
	pruneTo = 0.8
	// maxName is the longest file name kept, in bytes: what most file
	// systems take.
	maxName = 255
)

// Store is the cache: Dir holds one directory per file, named by the
// file's Slack id, with the file in it under its own name; a file being
// fetched is written in Dir itself, under a name starting with tmpPrefix,
// and moved into its directory once whole. Its exported fields are set
// before use and not changed after.
type Store struct {
	// Dir is where the files are kept; a relative Dir is taken from the
	// working directory at each Get, and the paths returned are absolute.
	Dir string
	// Fetch gets a file from the hub; it is link.Client.Fetch.
	Fetch func(ctx context.Context, id string) (link.File, io.ReadCloser, error)
	// Retention is how long a file is kept after it was last fetched,
	// from Slack or from here. Zero means DefaultRetention.
	Retention time.Duration
	// MaxTotal is the most the files add up to; over it, Prune deletes
	// the longest unfetched until they add up to pruneTo of it. Zero
	// means DefaultMaxTotal.
	MaxTotal int64
	// Timeout bounds the fetching Attach does for one message. Zero
	// means DefaultTimeout.
	Timeout time.Duration
	// PruneInterval is how often Run prunes. Zero means
	// DefaultPruneInterval.
	PruneInterval time.Duration
	// Now returns the current time; nil means time.Now.
	Now func() time.Time

	mu sync.Mutex
	// fetching counts the Gets in progress on each id: Prune leaves
	// those directories alone.
	fetching map[string]int
	// locks serializes the Gets of one id, so a file is fetched once;
	// each has room for one holder.
	locks map[string]chan struct{}
}

// fileID matches a Slack file id: nothing a path could be made of.
var fileID = regexp.MustCompile(`^[A-Za-z0-9]+$`)

// tmpPrefix starts the name of a file being written, in Dir: the id and a
// random suffix follow.
const tmpPrefix = ".fetching-"

func (s *Store) retention() time.Duration {
	if s.Retention == 0 {
		return DefaultRetention
	}
	return s.Retention
}

func (s *Store) maxTotal() int64 {
	if s.MaxTotal == 0 {
		return DefaultMaxTotal
	}
	return s.MaxTotal
}

func (s *Store) timeout() time.Duration {
	if s.Timeout == 0 {
		return DefaultTimeout
	}
	return s.Timeout
}

func (s *Store) pruneInterval() time.Duration {
	if s.PruneInterval == 0 {
		return DefaultPruneInterval
	}
	return s.PruneInterval
}

func (s *Store) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// Get returns the path of the file with id, fetching it from the hub
// unless it is here already; either way the file counts as fetched now.
// The file is written in Dir and renamed into its directory once it is
// whole, so the path never names a part of a file. The directory is this
// user's alone (0700), and so is the file (0600). Two Gets of one file at
// once fetch it once: the second waits for the first, or gives up when
// its context ends.
func (s *Store) Get(ctx context.Context, id string) (string, error) {
	if !fileID.MatchString(id) {
		return "", fmt.Errorf("%w: %q is not a file id", link.ErrBadRequest, id)
	}
	root, err := filepath.Abs(s.Dir)
	if err != nil {
		return "", err
	}
	unlock, err := s.lock(ctx, id)
	if err != nil {
		return "", err
	}
	defer unlock()
	dir := filepath.Join(root, id)
	if path, ok := cached(dir); ok {
		if err := os.Chtimes(path, s.now(), s.now()); err != nil {
			return "", err
		}
		return path, nil
	}
	f, body, err := s.Fetch(ctx, id)
	if err != nil {
		return "", err
	}
	defer body.Close()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(root, tmpPrefix+id+"-")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name())
	n, err := io.Copy(tmp, io.LimitReader(body, f.Size+1))
	if err == nil && n != f.Size {
		err = fmt.Errorf("got %d bytes of %s, the hub said %d", n, id, f.Size)
	}
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return "", fmt.Errorf("fetching %s: %w", id, err)
	}
	path := filepath.Join(dir, safeName(f.Name))
	if err := os.Chtimes(tmp.Name(), s.now(), s.now()); err != nil {
		return "", err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return "", err
	}
	return path, nil
}

// lock marks id as being fetched and takes its lock, or gives up when
// ctx ends first; the returned func undoes both.
func (s *Store) lock(ctx context.Context, id string) (func(), error) {
	s.mu.Lock()
	if s.fetching == nil {
		s.fetching, s.locks = make(map[string]int), make(map[string]chan struct{})
	}
	s.fetching[id]++
	l := s.locks[id]
	if l == nil {
		l = make(chan struct{}, 1)
		s.locks[id] = l
	}
	s.mu.Unlock()
	release := func() {
		s.mu.Lock()
		if s.fetching[id]--; s.fetching[id] == 0 {
			delete(s.fetching, id)
			delete(s.locks, id)
		}
		s.mu.Unlock()
	}
	select {
	case l <- struct{}{}:
	case <-ctx.Done():
		release()
		return nil, fmt.Errorf("waiting for another fetch of %s: %w", id, ctx.Err())
	}
	return func() {
		<-l
		release()
	}, nil
}

// cached returns the file in dir, if there is one: the regular file
// there.
func cached(dir string) (string, bool) {
	for _, e := range entries(dir) {
		if e.Type().IsRegular() {
			return filepath.Join(dir, e.Name()), true
		}
	}
	return "", false
}

func entries(dir string) []os.DirEntry {
	es, _ := os.ReadDir(dir)
	return es
}

// safeName makes name one a file can be stored under: no path separators
// or control characters, at most maxName bytes with the extension kept,
// "file" when nothing is left.
func safeName(name string) string {
	name = strings.Map(func(r rune) rune {
		if r == '/' || r == '\\' || unicode.IsControl(r) || r == utf8.RuneError {
			return -1
		}
		return r
	}, name)
	if name == "" || name == "." || name == ".." {
		return "file"
	}
	if len(name) <= maxName {
		return name
	}
	ext := filepath.Ext(name)
	if len(ext) > maxName/2 {
		ext = ""
	}
	stem := name[:len(name)-len(ext)]
	for len(stem)+len(ext) > maxName {
		_, size := utf8.DecodeLastRuneInString(stem)
		stem = stem[:len(stem)-size]
	}
	return stem + ext
}

// Attach fetches the files of the message in raw, a payload, that the
// hub marked for fetching, within Timeout for them all, and returns the
// payload with each such file's "path", or "error" when it could not be
// fetched: the message goes to the hook either way. Any other payload is
// returned as it is; the fields of the payload and of its files, known
// here or not, are kept.
func (s *Store) Attach(ctx context.Context, raw []byte) []byte {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil || string(m["type"]) != `"`+payload.Inbound+`"` {
		return raw
	}
	var fs []map[string]json.RawMessage
	if err := json.Unmarshal(m["files"], &fs); err != nil || !slices.ContainsFunc(fs, marked) {
		return raw
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout())
	defer cancel()
	for _, f := range fs {
		if !marked(f) {
			continue
		}
		var id string
		json.Unmarshal(f["id"], &id)
		path, err := s.Get(ctx, id)
		if err != nil {
			f["error"], _ = json.Marshal(err.Error())
			continue
		}
		f["path"], _ = json.Marshal(path)
	}
	b, err := json.Marshal(fs)
	if err != nil {
		return raw
	}
	m["files"] = b
	out, err := json.Marshal(m)
	if err != nil {
		return raw
	}
	return out
}

// marked reports whether the hub marked the file f for fetching.
func marked(f map[string]json.RawMessage) bool { return string(f["fetch"]) == "true" }

// Run prunes every PruneInterval until ctx is done.
func (s *Store) Run(ctx context.Context) {
	t := time.NewTicker(s.pruneInterval())
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-ctx.Done():
			return
		}
		if err := s.Prune(); err != nil {
			slog.Warn("files: prune", "err", err)
		}
	}
}

// Prune deletes the files not fetched for Retention and, while the rest
// add up to more than MaxTotal, the longest unfetched of them, until they
// add up to pruneTo of it; a file being fetched is left alone, and so is
// its directory. Leftovers of fetches that never finished are deleted.
// Errors are returned together; what could be pruned was.
func (s *Store) Prune() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	type file struct {
		id, path string
		size     int64
		used     time.Time
	}
	var files []file
	var total int64
	var errs []error
	for _, d := range entries(s.Dir) {
		id := d.Name()
		if !d.IsDir() {
			// A file being written, or left over from a fetch that never
			// finished: the name says which id it was for.
			if left, ok := strings.CutPrefix(id, tmpPrefix); ok && s.fetching[left[:max(strings.LastIndex(left, "-"), 0)]] == 0 {
				errs = append(errs, os.Remove(filepath.Join(s.Dir, id)))
			}
			continue
		}
		if s.fetching[id] > 0 {
			continue
		}
		dir := filepath.Join(s.Dir, id)
		for _, e := range entries(dir) {
			path := filepath.Join(dir, e.Name())
			fi, err := e.Info()
			if err != nil {
				errs = append(errs, err)
				continue
			}
			files = append(files, file{id, path, fi.Size(), fi.ModTime()})
			total += fi.Size()
		}
		if len(entries(dir)) == 0 {
			errs = append(errs, os.Remove(dir))
		}
	}
	slices.SortFunc(files, func(a, b file) int { return a.used.Compare(b.used) })
	expired := s.now().Add(-s.retention())
	// Once over the limit, the pruning goes on down to the target.
	limit := s.maxTotal()
	for _, f := range files {
		if !f.used.Before(expired) && total <= limit {
			break
		}
		if err := os.RemoveAll(filepath.Join(s.Dir, f.id)); err != nil {
			errs = append(errs, err)
			continue
		}
		total -= f.size
		if !f.used.Before(expired) {
			limit = int64(float64(s.maxTotal()) * pruneTo)
		}
	}
	return errors.Join(errs...)
}
