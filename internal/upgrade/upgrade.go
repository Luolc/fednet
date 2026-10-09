// Package upgrade moves a hub or a client to a newer release. A hub or
// client process never changes its own binary: it writes an upgrade
// request, a file naming the release, and a root unit on the machine runs
// `fednet upgrade`, which is Install here followed by a handoff: it
// downloads the release's binary for this architecture and its SHA256SUMS,
// checks the sum, puts the binary in place atomically, and hands the
// running process off to it. Hub, in hub.go, decides when: on `/fednet
// upgrade` in Slack and on the hourly check for a new release, it tells
// every client that is online, waits for them, then requests its own
// upgrade. Client, in client.go, is the client's end: it turns the hub's
// notice into the request file.
package upgrade

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Luolc/fednet/internal/release"
)

// Sums is the name of the release asset that lists the SHA-256 of each
// binary, one a line, as sha256sum writes them.
const Sums = "SHA256SUMS"

// Asset is the name of the release asset holding the binary of version
// for arch, a GOARCH such as amd64.
func Asset(version, arch string) string { return "fednet_" + version + "_linux_" + arch }

// Limits on what is downloaded: a sums file is a few lines, a binary a
// few tens of megabytes.
const (
	maxSums   = 64 << 10
	maxBinary = 256 << 20
)

// DefaultTimeout bounds one request to the release source when
// Releases.HTTP is nil.
const DefaultTimeout = 5 * time.Minute

// Releases is where releases are published. GitHub is fednet's.
type Releases struct {
	// Latest is the URL that answers with the latest release as GitHub's
	// API does: a JSON object whose tag_name is the version.
	Latest string
	// Download is the URL under which <version>/<asset> is each asset of
	// a release.
	Download string
	// HTTP makes the requests. Nil means a client with DefaultTimeout.
	HTTP *http.Client
}

// GitHub is fednet's own releases.
var GitHub = Releases{
	Latest:   "https://api.github.com/repos/Luolc/fednet/releases/latest",
	Download: "https://github.com/Luolc/fednet/releases/download",
}

func (r Releases) http() *http.Client {
	if r.HTTP != nil {
		return r.HTTP
	}
	return &http.Client{Timeout: DefaultTimeout}
}

// get fetches url and returns the body, which the caller closes; a status
// other than 200 is an error.
func (r Releases) get(ctx context.Context, url string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json, application/octet-stream")
	res, err := r.http().Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		res.Body.Close()
		return nil, fmt.Errorf("%s: %s", url, res.Status)
	}
	return res.Body, nil
}

// LatestVersion returns the version of the latest release, which must be
// a release.
func (r Releases) LatestVersion(ctx context.Context) (string, error) {
	body, err := r.get(ctx, r.Latest)
	if err != nil {
		return "", fmt.Errorf("upgrade: latest release: %w", err)
	}
	defer body.Close()
	var latest struct {
		Tag string `json:"tag_name"`
	}
	if err := json.NewDecoder(io.LimitReader(body, maxSums)).Decode(&latest); err != nil {
		return "", fmt.Errorf("upgrade: latest release: %v", err)
	}
	if !release.IsRelease(latest.Tag) {
		return "", fmt.Errorf("upgrade: the latest release is tagged %q, not a release", latest.Tag)
	}
	return latest.Tag, nil
}

// sum returns the SHA-256 the release's sums file gives for asset.
func (r Releases) sum(ctx context.Context, version, asset string) ([]byte, error) {
	body, err := r.get(ctx, r.Download+"/"+version+"/"+Sums)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	sc := bufio.NewScanner(io.LimitReader(body, maxSums))
	for sc.Scan() {
		// "<hex>  <name>"; sha256sum -b writes "<hex> *<name>".
		fields := strings.Fields(sc.Text())
		if len(fields) != 2 || strings.TrimPrefix(fields[1], "*") != asset {
			continue
		}
		sum, err := hex.DecodeString(fields[0])
		if err != nil || len(sum) != sha256.Size {
			return nil, fmt.Errorf("%s lists a malformed sum for %s", Sums, asset)
		}
		return sum, nil
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("reading %s: %w", Sums, err)
	}
	return nil, fmt.Errorf("%s of %s does not list %s", Sums, version, asset)
}

// Previous is the path the binary Install replaced is kept at, a hard
// link to it next to path, for Restore.
func Previous(path string) string { return path + ".prev" }

// Install downloads the binary of version for arch and puts it at path,
// in place of what is there, once its SHA-256 matches the release's sums
// file. The binary there before is kept at Previous(path). The file at
// path is replaced in one rename, so a reader sees the old binary or the
// new one, never a part; nothing is replaced when the sum does not match,
// when the release has no binary for arch, or when the download fails.
func (r Releases) Install(ctx context.Context, version, arch, path string) error {
	if !release.IsRelease(version) {
		return fmt.Errorf("upgrade: %q is not a release", version)
	}
	asset := Asset(version, arch)
	want, err := r.sum(ctx, version, asset)
	if err != nil {
		return fmt.Errorf("upgrade: %w", err)
	}
	body, err := r.get(ctx, r.Download+"/"+version+"/"+asset)
	if err != nil {
		return fmt.Errorf("upgrade: %w", err)
	}
	defer body.Close()
	// The new binary is written next to the old one, so that the rename
	// stays on one file system.
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".upgrade-*")
	if err != nil {
		return fmt.Errorf("upgrade: %w", err)
	}
	defer os.Remove(f.Name())
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), io.LimitReader(body, maxBinary+1))
	if err == nil && n > maxBinary {
		err = fmt.Errorf("%s is over %d bytes", asset, maxBinary)
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("upgrade: downloading %s: %w", asset, err)
	}
	if got := h.Sum(nil); !bytes.Equal(got, want) {
		return fmt.Errorf("upgrade: %s does not match its sum in %s: got %x, want %x", asset, Sums, got, want)
	}
	if err := os.Chmod(f.Name(), 0o755); err != nil {
		return fmt.Errorf("upgrade: %w", err)
	}
	// The old binary stays reachable as a second name of the same file,
	// so path always names a whole binary: the old one until the rename,
	// the new one after.
	prev := Previous(path)
	if err := os.Remove(prev); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("upgrade: %w", err)
	}
	if err := os.Link(path, prev); err != nil {
		return fmt.Errorf("upgrade: keeping the old binary: %w", err)
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return fmt.Errorf("upgrade: %w", err)
	}
	return nil
}

// Restore puts the binary Install kept at Previous(path) back at path, in
// one rename.
func Restore(path string) error {
	if err := os.Rename(Previous(path), path); err != nil {
		return fmt.Errorf("upgrade: restoring the old binary: %w", err)
	}
	return nil
}

// WriteResult writes what became of the request at path, for whoever
// wrote the request to read: the text, in the file Result names.
func WriteResult(path, text string) error {
	return os.WriteFile(Result(path), []byte(text+"\n"), 0o644)
}

// Result is the path the upgrader writes the outcome of the request at
// path to.
func Result(path string) string { return path + ".result" }

// ReadResult returns the outcome written for the request at path, and
// deletes it; "" when there is none.
func ReadResult(path string) string {
	b, err := os.ReadFile(Result(path))
	if err != nil {
		return ""
	}
	os.Remove(Result(path))
	return strings.TrimSpace(string(b))
}

// ErrNoRequest is returned by ReadRequest when there is no request file.
var ErrNoRequest = errors.New("upgrade: no request")

// WriteRequest writes an upgrade request for version at path, whole: it is
// written next to path first and renamed into place, so a watcher never
// reads a partial request.
func WriteRequest(path, version string) error {
	if !release.IsRelease(version) {
		return fmt.Errorf("upgrade: %q is not a release", version)
	}
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+"-*")
	if err != nil {
		return fmt.Errorf("upgrade: writing the request: %w", err)
	}
	_, err = f.WriteString(version + "\n")
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(f.Name(), path)
	}
	if err != nil {
		os.Remove(f.Name())
		return fmt.Errorf("upgrade: writing the request: %w", err)
	}
	return nil
}

// ReadRequest returns the version the request file at path names, which
// must be a release. A missing file is ErrNoRequest.
func ReadRequest(path string) (string, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", ErrNoRequest
	}
	if err != nil {
		return "", err
	}
	v := strings.TrimSpace(string(b))
	if !release.IsRelease(v) {
		return "", fmt.Errorf("upgrade: the request at %s names %q, not a release", path, v)
	}
	return v, nil
}
