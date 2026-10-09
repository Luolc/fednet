package hubapi

import (
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
