package local

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Luolc/fednet/internal/hubapi"
	"github.com/Luolc/fednet/internal/link"
	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
)

// serve runs a Server for c on a new socket and returns the socket's path.
func serve(t *testing.T, c *link.Client) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fednet.sock")
	ln, err := Listen(path, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- (&Server{Post: c.Post, Request: c.Request}).Serve(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve: %v", err)
		}
	})
	return path
}

func openClientStore(t *testing.T) *store.Client {
	t.Helper()
	st, err := store.OpenClient(t.Context(), filepath.Join(t.TempDir(), "client.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func TestPost(t *testing.T) {
	ctx := t.Context()
	st := openClientStore(t)
	path := serve(t, &link.Client{Store: st})

	res, err := Do(ctx, path, Request{Cmd: Post, Thread: "C1/1700000000.000100", Text: "build is green"})
	if err != nil || res.Error != "" || res.MsgID == "" {
		t.Fatalf("Do(post) = %+v, %v; want a msg_id", res, err)
	}
	ms, err := st.Outbox.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 1 || ms[0].MsgID != res.MsgID {
		t.Fatalf("outbox = %+v, want just %s", ms, res.MsgID)
	}
	var got payload.Message
	if err := json.Unmarshal(ms[0].Payload, &got); err != nil {
		t.Fatal(err)
	}
	want := payload.Message{Type: payload.Post, Thread: "C1/1700000000.000100", Text: "build is green"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("payload = %+v, want %+v", got, want)
	}
}

func TestBadRequest(t *testing.T) {
	st := openClientStore(t)
	path := serve(t, &link.Client{Store: st})
	tests := []struct {
		name string
		req  Request
	}{
		{"unknown command", Request{Cmd: "shout", Thread: "t", Text: "x"}},
		{"post without a thread", Request{Cmd: Post, Text: "x"}},
		{"post without a text", Request{Cmd: Post, Thread: "t"}},
		{"post over the payload limit", Request{Cmd: Post, Thread: "t", Text: strings.Repeat("x", link.MaxPayload)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := Do(t.Context(), path, tt.req)
			if err != nil || res.Kind != BadRequest || res.Error == "" || res.MsgID != "" {
				t.Fatalf("Do = %+v, %v; want a bad request", res, err)
			}
		})
	}
	if ms, err := st.Outbox.Pending(t.Context()); err != nil || len(ms) != 0 {
		t.Fatalf("outbox = %+v, %v; want empty", ms, err)
	}
}

// A post is queued while the hub is down and reaches it once it is up.
func TestPostWhileHubDown(t *testing.T) {
	ctx := t.Context()
	hs, err := store.OpenHub(ctx, filepath.Join(t.TempDir(), "hub.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hs.Close() })
	hub := &link.Hub{Store: hs}
	// The listener exists but nothing serves it until Start, so every
	// request until then times out.
	srv := httptest.NewUnstartedServer(hub.Handler())
	t.Cleanup(func() {
		hub.Close()
		srv.Close()
	})

	cs := openClientStore(t)
	c := &link.Client{
		Store:      cs,
		ID:         "workstation",
		Hub:        "http://" + srv.Listener.Addr().String(),
		Backoff:    link.Backoff{Min: time.Millisecond, Max: 10 * time.Millisecond},
		Timeout:    50 * time.Millisecond,
		HTTPClient: &http.Client{Transport: &http.Transport{}},
	}
	rctx, cancel := context.WithCancel(ctx)
	ran := make(chan struct{})
	go func() {
		defer close(ran)
		c.Run(rctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-ran
		c.HTTPClient.CloseIdleConnections()
	})
	path := serve(t, c)

	res, err := Do(ctx, path, Request{Cmd: Post, Thread: "t", Text: "while the hub is down"})
	if err != nil || res.MsgID == "" {
		t.Fatalf("Do(post) = %+v, %v; want a msg_id", res, err)
	}
	if ms, err := hs.Inbox.Undelivered(ctx); err != nil || len(ms) != 0 {
		t.Fatalf("hub inbox before the hub is up = %+v, %v; want empty", ms, err)
	}

	srv.Start()
	deadline := time.Now().Add(5 * time.Second)
	for {
		ms, err := hs.Inbox.Undelivered(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(ms) == 1 && ms[0].MsgID == res.MsgID {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("hub inbox = %+v, want %s", ms, res.MsgID)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestListen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fednet.sock")
	// A socket left behind by a daemon that did not shut down.
	stale, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	stale.(*net.UnixListener).SetUnlinkOnClose(false)
	stale.Close()

	ln, err := Listen(path, "")
	if err != nil {
		t.Fatal(err)
	}
	checkMode(t, path, 0o600, os.Getgid())
	// A second client on the same path is refused, and the first one keeps
	// the socket.
	if ln2, err := Listen(path, ""); err == nil {
		ln2.Close()
		t.Fatal("Listen on a path a client is listening on succeeded")
	}
	accepted := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			conn.Close()
		}
		accepted <- err
	}()
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if err := <-accepted; err != nil {
		t.Fatalf("the first listener did not get the connection: %v", err)
	}
	ln.Close()

	// Once the first client has closed, a new one takes the path over; it
	// uses a group this user belongs to, other than its primary group if it
	// has one, so that the group visibly changes.
	gid := os.Getgid()
	if gids, err := os.Getgroups(); err == nil {
		for _, id := range gids {
			if id != gid {
				gid = id
				break
			}
		}
	}
	g, err := user.LookupGroupId(strconv.Itoa(gid))
	if err != nil {
		t.Skip(err)
	}
	ln, err = Listen(path, g.Name)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	checkMode(t, path, 0o660, gid)
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 2 {
		t.Fatalf("dir holds %v, %v; want only the socket and its lock", entries, err)
	}
}

func checkMode(t *testing.T, path string, mode os.FileMode, gid int) {
	t.Helper()
	fi, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Type() != os.ModeSocket || fi.Mode().Perm() != mode || int(fi.Sys().(*syscall.Stat_t).Gid) != gid {
		t.Fatalf("%s: mode %v, gid %d; want a socket with mode %v, gid %d", path, fi.Mode(), fi.Sys().(*syscall.Stat_t).Gid, mode, gid)
	}
}

func TestListenRefusesAFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fednet.sock")
	if err := os.WriteFile(path, []byte("not a socket"), 0o600); err != nil {
		t.Fatal(err)
	}
	if ln, err := Listen(path, ""); err == nil {
		ln.Close()
		t.Fatal("Listen replaced a regular file")
	}
	if b, err := os.ReadFile(path); err != nil || string(b) != "not a socket" {
		t.Fatalf("file now holds %q, %v", b, err)
	}
}

// A client that holds the lock but has not published its socket yet, as
// one does between taking the lock and the rename, keeps the path: another
// Listen fails and publishes nothing.
func TestListenWhileAnotherIsStarting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fednet.sock")
	lock, err := os.OpenFile(lockPath(path), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	if ln, err := Listen(path, ""); err == nil {
		ln.Close()
		t.Fatal("Listen succeeded while another client held the lock")
	}
	if _, err := os.Lstat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Lstat(%s) = %v; want nothing published", path, err)
	}
}

// Of several clients starting on one path at once, exactly one gets it.
func TestListenConcurrent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fednet.sock")
	const n = 8
	start := make(chan struct{})
	results := make(chan net.Listener, n)
	for range n {
		go func() {
			<-start
			ln, err := Listen(path, "")
			if err != nil {
				results <- nil
				return
			}
			results <- ln
		}()
	}
	close(start)
	var won []net.Listener
	for range n {
		if ln := <-results; ln != nil {
			won = append(won, ln)
		}
	}
	for _, ln := range won {
		defer ln.Close()
	}
	if len(won) != 1 {
		t.Fatalf("%d of %d concurrent Listen calls succeeded, want 1", len(won), n)
	}
	// Connections to the path reach the one that won.
	accepted := make(chan error, 1)
	go func() {
		conn, err := won[0].Accept()
		if err == nil {
			conn.Close()
		}
		accepted <- err
	}()
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if err := <-accepted; err != nil {
		t.Fatalf("the winner did not get the connection: %v", err)
	}
}

// Requests for the hub are answered with the hub's reply, and a failure
// says why.
func TestAsk(t *testing.T) {
	ctx := t.Context()
	hs, err := store.OpenHub(ctx, filepath.Join(t.TempDir(), "hub.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hs.Close() })
	f := &slack.Fake{}
	f.AddChannel("C1", "repo: fednet")
	ts, err := f.Start("C1", "U1", "please fix the build")
	if err != nil {
		t.Fatal(err)
	}
	hub := &link.Hub{Store: hs, Answer: (&hubapi.Server{Store: hs, Slack: f, Users: map[string]string{"U1": "maintainer"}}).Answer}
	srv := httptest.NewServer(hub.Handler())
	t.Cleanup(func() {
		hub.Close()
		srv.Close()
	})
	path := serve(t, &link.Client{ID: "workstation", Hub: srv.URL})

	res, err := Do(ctx, path, Request{Cmd: hubapi.ReadThread, Thread: slack.ThreadKey("C1", ts)})
	want := []slack.Message{{TS: ts, User: "U1", Text: "please fix the build"}}
	if err != nil || res.Error != "" || !reflect.DeepEqual(res.Messages, want) {
		t.Fatalf("Do(read-thread) = %+v, %v; want %+v", res, err, want)
	}
	res, err = Do(ctx, path, Request{Cmd: hubapi.GetChannelContext, Channel: "C1"})
	if err != nil || res.Error != "" || res.Text != "repo: fednet" {
		t.Fatalf("Do(channel-context-get) = %+v, %v; want the purpose", res, err)
	}
	res, err = Do(ctx, path, Request{Cmd: hubapi.Users})
	if users := []hubapi.User{{ID: "U1", Name: "maintainer"}}; err != nil || res.Error != "" || !slices.Equal(res.Users, users) {
		t.Fatalf("Do(users) = %+v, %v; want %+v", res, err, users)
	}
	res, err = Do(ctx, path, Request{Cmd: hubapi.DM, User: "U1", Text: "daily report"})
	if err != nil || res.Error != "" || !slices.Equal(f.DMs("U1"), []string{"daily report"}) {
		t.Fatalf("Do(dm) = %+v, %v; DMs to U1 = %q", res, err, f.DMs("U1"))
	}

	tests := []struct {
		name string
		req  Request
		kind string
	}{
		{"a malformed thread key", Request{Cmd: hubapi.ReadThread, Thread: "C1"}, BadRequest},
		{"a thread that does not exist", Request{Cmd: hubapi.ReadThread, Thread: "C1/1600000000.000001"}, NotFound},
		{"a dm to a user not on the list", Request{Cmd: hubapi.DM, User: "U9", Text: "x"}, Denied},
	}
	for _, tt := range tests {
		res, err := Do(ctx, path, tt.req)
		if err != nil || res.Error == "" || res.Kind != tt.kind {
			t.Errorf("%s: Do = %+v, %v; want kind %q", tt.name, res, err, tt.kind)
		}
	}
}

// With the hub down a request for it fails at once; it is not queued.
func TestAskWhileHubDown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hub := "http://" + ln.Addr().String()
	ln.Close()
	st := openClientStore(t)
	path := serve(t, &link.Client{Store: st, ID: "workstation", Hub: hub})
	res, err := Do(t.Context(), path, Request{Cmd: hubapi.Adopt, Thread: "C1/1700000000.000100"})
	if err != nil || res.Error == "" || res.Kind != Unreachable {
		t.Fatalf("Do(adopt) = %+v, %v; want kind %q", res, err, Unreachable)
	}
	if ms, err := st.Outbox.Pending(t.Context()); err != nil || len(ms) != 0 {
		t.Fatalf("outbox = %+v, %v; want empty", ms, err)
	}
}

// fetch-file hands the id to the daemon's Fetch and replies with the path;
// the failures come back with their kind.
func TestFetchFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fednet.sock")
	ln, err := Listen(path, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	s := &Server{Fetch: func(_ context.Context, id string) (string, error) {
		switch id {
		case "F1":
			return "/var/lib/fednet-client/files/F1/shot.png", nil
		case "F2":
			return "", link.Refuse(link.ErrNotFound, "file F2 no longer exists in Slack")
		}
		return "", link.ErrUnreachable
	}}
	go func() { done <- s.Serve(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve: %v", err)
		}
	})
	res, err := Do(t.Context(), path, Request{Cmd: FetchFile, File: "F1"})
	if err != nil || res.Error != "" || res.Path != "/var/lib/fednet-client/files/F1/shot.png" {
		t.Fatalf("Do(fetch-file F1) = %+v, %v; want the path", res, err)
	}
	for id, kind := range map[string]string{"F2": NotFound, "F3": Unreachable, "": BadRequest} {
		res, err := Do(t.Context(), path, Request{Cmd: FetchFile, File: id})
		if err != nil || res.Error == "" || res.Kind != kind || res.Path != "" {
			t.Errorf("Do(fetch-file %q) = %+v, %v; want kind %q", id, res, err, kind)
		}
	}
	// A socket that does not serve it says so.
	s.Fetch = nil
	if res, err := Do(t.Context(), path, Request{Cmd: FetchFile, File: "F1"}); err != nil || res.Kind != BadRequest {
		t.Fatalf("Do(fetch-file) without Fetch = %+v, %v; want a bad request", res, err)
	}
}

// A post with files goes to the daemon's Upload with the content that
// follows the request, and waits for it; one without files is queued as
// before.
func TestPostWithFiles(t *testing.T) {
	st := openClientStore(t)
	path := filepath.Join(t.TempDir(), "fednet.sock")
	ln, err := Listen(path, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	var got []string
	c := &link.Client{Store: st, ID: "workstation", Hub: "http://127.0.0.1:1"}
	s := &Server{Post: c.Post, Upload: func(_ context.Context, u link.Upload, body io.Reader) error {
		if u.Thread == "C1/bad" {
			return link.Refuse(link.ErrBadRequest, "too big")
		}
		b, err := io.ReadAll(body)
		if err != nil {
			return err
		}
		got = append(got, u.Thread, u.Text, string(b))
		for _, f := range u.Files {
			got = append(got, f.Name)
		}
		return nil
	}}
	go func() { done <- s.Serve(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve: %v", err)
		}
	})
	req := Request{Cmd: Post, Thread: "C1/1.1", Text: "see", Files: []link.FileHeader{{Name: "a.png", Size: 3}, {Name: "b.log", Size: 2}}}
	res, err := Do(t.Context(), path, req, strings.NewReader("PNG"), strings.NewReader("er"))
	if err != nil || res.Error != "" || res.MsgID != "" {
		t.Fatalf("Do(post with files) = %+v, %v; want nothing but success", res, err)
	}
	if want := []string{"C1/1.1", "see", "PNGer", "a.png", "b.log"}; !slices.Equal(got, want) {
		t.Fatalf("Upload got %q, want %q", got, want)
	}
	if ms, _ := st.Outbox.Pending(t.Context()); len(ms) != 0 {
		t.Fatal("a post with files was queued")
	}
	res, err = Do(t.Context(), path, Request{Cmd: Post, Thread: "C1/bad", Files: req.Files}, strings.NewReader("PNG"), strings.NewReader("er"))
	if err != nil || res.Kind != BadRequest {
		t.Fatalf("Do(refused post) = %+v, %v; want kind %q", res, err, BadRequest)
	}
	// A file shorter than declared is an error on the caller's side.
	if _, err := Do(t.Context(), path, req, strings.NewReader("PN"), strings.NewReader("er")); err == nil || !strings.Contains(err.Error(), "sending a.png") {
		t.Fatalf("Do with a short file = %v, want an error naming it", err)
	}
	if _, err := Do(t.Context(), path, req, strings.NewReader("PNG")); err == nil {
		t.Fatal("Do with fewer readers than files did not fail")
	}
	// A refusal that comes before the content has been read, for a file
	// larger than the socket's buffer, still reaches the caller: the
	// write that fails after it does not hide it.
	big := Request{Cmd: Post, Thread: "C1/bad", Files: []link.FileHeader{{Name: "big.bin", Size: 1 << 20}}}
	res, err = Do(t.Context(), path, big, bytes.NewReader(make([]byte, 1<<20)))
	if err != nil || res.Kind != BadRequest || !strings.Contains(res.Error, "too big") {
		t.Fatalf("Do(refused big post) = %+v, %v; want the refusal", res, err)
	}
}

// The content may take longer to arrive than the request itself: the
// request's deadline, not the header's, bounds it.
func TestPostContentOutlivesTheHeaderTimeout(t *testing.T) {
	headerTimeout = 50 * time.Millisecond
	t.Cleanup(func() { headerTimeout = Timeout })
	path := filepath.Join(t.TempDir(), "fednet.sock")
	ln, err := Listen(path, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	var got string
	s := &Server{Upload: func(_ context.Context, u link.Upload, body io.Reader) error {
		b, err := io.ReadAll(body)
		got = string(b)
		return err
	}}
	go func() { done <- s.Serve(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve: %v", err)
		}
	})
	slow := Request{Cmd: Post, Thread: "C1/1.1", Files: []link.FileHeader{{Name: "slow.bin", Size: 3}}}
	res, err := Do(t.Context(), path, slow, &delayed{strings.NewReader("abc"), 150 * time.Millisecond})
	if err != nil || res.Error != "" || got != "abc" {
		t.Fatalf("Do(slow post) = %+v, %v, Upload got %q; want it to succeed", res, err, got)
	}
}

// delayed is a reader whose first Read waits for d.
type delayed struct {
	io.Reader
	d time.Duration
}

func (r *delayed) Read(p []byte) (int, error) {
	if r.d > 0 {
		time.Sleep(r.d)
		r.d = 0
	}
	return r.Reader.Read(p)
}
