package hubapi

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"testing"

	"github.com/Luolc/fednet/internal/link"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
)

func testServer(t *testing.T) (*Server, *slack.Fake) {
	t.Helper()
	st, err := store.OpenHub(t.Context(), filepath.Join(t.TempDir(), "hub.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	f := &slack.Fake{}
	f.AddChannel("C1", "repo: fednet")
	return &Server{Store: st, Slack: f}, f
}

// answer sends r to s as client and decodes the reply.
func answer(t *testing.T, s *Server, client string, r Request) (Reply, error) {
	t.Helper()
	req, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Answer(t.Context(), client, req)
	if err != nil {
		return Reply{}, err
	}
	var reply Reply
	if err := json.Unmarshal(b, &reply); err != nil {
		t.Fatal(err)
	}
	return reply, nil
}

func TestReadThread(t *testing.T) {
	s, f := testServer(t)
	ts, err := f.Start("C1", "U1", "please fix the build")
	if err != nil {
		t.Fatal(err)
	}
	reply, err := f.Reply("C1", ts, "B1", "on it")
	if err != nil {
		t.Fatal(err)
	}
	key := slack.ThreadKey("C1", ts)

	got, err := answer(t, s, "workstation", Request{Cmd: ReadThread, Thread: key})
	if err != nil {
		t.Fatal(err)
	}
	want := []slack.Message{{TS: ts, User: "U1", Text: "please fix the build"}, {TS: reply, User: "B1", Text: "on it"}}
	if !slices.Equal(got.Messages, want) {
		t.Fatalf("read-thread = %+v, want %+v", got.Messages, want)
	}

	tests := []struct {
		thread string
		want   error
	}{
		{slack.ThreadKey("C1", "1600000000.000001"), link.ErrNotFound},
		{slack.ThreadKey("C9", ts), link.ErrNotFound},
		{"C1", link.ErrBadRequest},
		{"", link.ErrBadRequest},
	}
	for _, tt := range tests {
		if _, err := answer(t, s, "workstation", Request{Cmd: ReadThread, Thread: tt.thread}); !errors.Is(err, tt.want) {
			t.Errorf("read-thread %q = %v, want %v", tt.thread, err, tt.want)
		}
	}
}

func TestAdopt(t *testing.T) {
	s, _ := testServer(t)
	ctx := t.Context()
	const key = "C1/1700000000.000100"
	if _, _, err := s.Store.ClaimAndEnqueue(ctx, key, "old-workstation", []byte("first")); err != nil {
		t.Fatal(err)
	}
	if _, err := answer(t, s, "workstation", Request{Cmd: Adopt, Thread: key}); err != nil {
		t.Fatal(err)
	}
	if owner, err := s.Store.Owner(ctx, key); err != nil || owner != "workstation" {
		t.Fatalf("owner after adopt = %q, %v; want workstation", owner, err)
	}

	// A thread no one owns is not adopted.
	const unowned = "C1/1700000000.000200"
	if _, err := answer(t, s, "workstation", Request{Cmd: Adopt, Thread: unowned}); !errors.Is(err, link.ErrNotFound) {
		t.Fatalf("adopt of an unowned thread = %v, want ErrNotFound", err)
	}
	if _, err := s.Store.Owner(ctx, unowned); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("owner of the unowned thread after adopt: %v, want none", err)
	}
}

func TestChannelContext(t *testing.T) {
	s, _ := testServer(t)
	got, err := answer(t, s, "workstation", Request{Cmd: GetChannelContext, Channel: "C1"})
	if err != nil || got.Text != "repo: fednet" {
		t.Fatalf("channel-context get = %+v, %v; want the purpose", got, err)
	}
	if _, err := answer(t, s, "workstation", Request{Cmd: SetChannelContext, Channel: "C1", Text: "repos: fednet, fleet"}); err != nil {
		t.Fatal(err)
	}
	if got, err := answer(t, s, "workstation", Request{Cmd: GetChannelContext, Channel: "C1"}); err != nil || got.Text != "repos: fednet, fleet" {
		t.Fatalf("channel-context get after set = %+v, %v; want the new purpose", got, err)
	}
	for _, r := range []Request{
		{Cmd: GetChannelContext, Channel: "C9"},
		{Cmd: SetChannelContext, Channel: "C9", Text: "x"},
	} {
		if _, err := answer(t, s, "workstation", r); !errors.Is(err, link.ErrNotFound) {
			t.Errorf("%s of an unknown channel = %v, want ErrNotFound", r.Cmd, err)
		}
	}
}

func TestBadRequest(t *testing.T) {
	s, _ := testServer(t)
	for _, req := range []string{`not json`, `{"cmd":"shout"}`, `{"cmd":"channel-context-get"}`} {
		if _, err := s.Answer(t.Context(), "workstation", []byte(req)); !errors.Is(err, link.ErrBadRequest) {
			t.Errorf("Answer(%s) = %v, want ErrBadRequest", req, err)
		}
	}
}

// Without Slack, requests that need it fail; adopt does not need it.
func TestWithoutSlack(t *testing.T) {
	s, _ := testServer(t)
	s.Slack = nil
	const key = "C1/1700000000.000100"
	if _, _, err := s.Store.ClaimAndEnqueue(t.Context(), key, "old-workstation", []byte("first")); err != nil {
		t.Fatal(err)
	}
	if _, err := answer(t, s, "workstation", Request{Cmd: ReadThread, Thread: key}); !errors.Is(err, errNoSlack) {
		t.Errorf("read-thread without Slack = %v, want errNoSlack", err)
	}
	if _, err := answer(t, s, "workstation", Request{Cmd: Adopt, Thread: key}); err != nil {
		t.Errorf("adopt without Slack = %v", err)
	}
}

// countPosts is a Slack API that counts the calls to Post.
type countPosts struct {
	slack.API
	n int
}

func (c *countPosts) Post(ctx context.Context, channel, text string) (string, error) {
	c.n++
	return c.API.Post(ctx, channel, text)
}

func TestOpenThread(t *testing.T) {
	s, f := testServer(t)
	f.AddChannel("C2", "")
	posts := &countPosts{API: f}
	s.Slack = posts
	s.OpenThread = map[string][]string{"C1": {"workstation"}, "C9": {"workstation"}}
	ctx := t.Context()

	got, err := answer(t, s, "workstation", Request{Cmd: OpenThread, Channel: "C1", Text: "nightly report"})
	if err != nil {
		t.Fatal(err)
	}
	channel, ts, ok := slack.ParseThreadKey(got.Thread)
	if !ok || channel != "C1" {
		t.Fatalf("open-thread = %+v, want a thread key in C1", got)
	}
	if ms, err := f.Replies(ctx, channel, ts); err != nil || len(ms) != 1 || ms[0].Text != "nightly report" {
		t.Fatalf("Slack thread = %+v, %v; want the text", ms, err)
	}
	if owner, err := s.Store.Owner(ctx, got.Thread); err != nil || owner != "workstation" {
		t.Fatalf("owner of the new thread = %q, %v; want workstation", owner, err)
	}

	tests := []struct {
		name   string
		client string
		req    Request
		want   error
	}{
		{"a client the channel does not list", "datamachine", Request{Cmd: OpenThread, Channel: "C1", Text: "x"}, link.ErrDenied},
		{"a channel the config does not list", "workstation", Request{Cmd: OpenThread, Channel: "C2", Text: "x"}, link.ErrDenied},
		{"a channel Slack does not know", "workstation", Request{Cmd: OpenThread, Channel: "C9", Text: "x"}, link.ErrNotFound},
		{"no text", "workstation", Request{Cmd: OpenThread, Channel: "C1"}, link.ErrBadRequest},
	}
	for _, tt := range tests {
		if _, err := answer(t, s, tt.client, tt.req); !errors.Is(err, tt.want) {
			t.Errorf("%s: open-thread = %v, want %v", tt.name, err, tt.want)
		}
	}
	// Only the open-thread to a channel Slack does not know reached Slack,
	// besides the first one.
	if posts.n != 2 {
		t.Fatalf("Slack got %d posts, want 2", posts.n)
	}
}

func TestThreads(t *testing.T) {
	s, _ := testServer(t)
	for thread, client := range map[string]string{"C1/2": "workstation", "C1/1": "workstation", "C1/3": "datamachine"} {
		if err := s.Store.Claim(t.Context(), thread, client); err != nil {
			t.Fatal(err)
		}
	}
	got, err := answer(t, s, "workstation", Request{Cmd: Threads})
	if err != nil || !slices.Equal(got.Threads, []string{"C1/1", "C1/2"}) {
		t.Fatalf("threads = %+v, %v; want [C1/1 C1/2]", got, err)
	}
	if got, err := answer(t, s, "nobody", Request{Cmd: Threads}); err != nil || len(got.Threads) != 0 {
		t.Fatalf("threads of a client with none = %+v, %v; want none", got, err)
	}
}

func TestUsers(t *testing.T) {
	s, _ := testServer(t)
	s.Users = map[string]string{"U2": "", "U1": "maintainer"}
	got, err := answer(t, s, "workstation", Request{Cmd: Users})
	want := []User{{ID: "U1", Name: "maintainer"}, {ID: "U2"}}
	if err != nil || !slices.Equal(got.Users, want) {
		t.Fatalf("users = %+v, %v; want %+v", got, err, want)
	}
}

func TestDM(t *testing.T) {
	s, f := testServer(t)
	s.Users = map[string]string{"U1": "maintainer"}
	if _, err := answer(t, s, "workstation", Request{Cmd: DM, User: "U1", Text: "daily report"}); err != nil {
		t.Fatal(err)
	}
	if got := f.DMs("U1"); !slices.Equal(got, []string{"daily report"}) {
		t.Fatalf("DMs to U1 = %q, want the report", got)
	}

	tests := []struct {
		name string
		req  Request
		want error
	}{
		{"a user not on the list", Request{Cmd: DM, User: "U9", Text: "x"}, link.ErrDenied},
		{"no user", Request{Cmd: DM, Text: "x"}, link.ErrBadRequest},
		{"no text", Request{Cmd: DM, User: "U1"}, link.ErrBadRequest},
	}
	for _, tt := range tests {
		if _, err := answer(t, s, "workstation", tt.req); !errors.Is(err, tt.want) {
			t.Errorf("%s: dm = %v, want %v", tt.name, err, tt.want)
		}
	}
	if got := f.DMs("U9"); len(got) != 0 {
		t.Fatalf("DMs to the user not on the list = %q, want none", got)
	}
}
