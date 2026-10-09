package local

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Luolc/fednet/internal/link"
	"github.com/Luolc/fednet/internal/payload"
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
	go func() { done <- (&Server{Post: c.Post}).Serve(ctx, ln) }()
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
	if got != want {
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
			if err != nil || !res.BadRequest || res.Error == "" || res.MsgID != "" {
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
	ln.Close()

	// With a group this user belongs to, other than its primary group if it
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
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 1 {
		t.Fatalf("dir holds %v, %v; want only the socket", entries, err)
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
