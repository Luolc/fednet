package hubapi

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Luolc/fednet/internal/approval"
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
	want := []slack.Message{{TS: ts, User: "U1", Text: "please fix the build"}, {TS: reply, User: "B1", Text: "on it", ThreadTS: ts}}
	if !reflect.DeepEqual(got.Messages, want) {
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
	if _, _, err := s.Store.ClaimAndEnqueue(ctx, key, "old-workstation", "", []byte("first")); err != nil {
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
	if _, _, err := s.Store.ClaimAndEnqueue(t.Context(), key, "old-workstation", "", []byte("first")); err != nil {
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
	f.RenameChannel("C1", "example-one")
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
	if name, err := s.Store.ChannelName(ctx, got.Thread); err != nil || name != "example-one" {
		t.Fatalf("channel name of the new thread = %q, %v; want example-one", name, err)
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

// noChannelInfo is a Slack whose ChannelInfo fails.
type noChannelInfo struct{ slack.API }

func (noChannelInfo) ChannelInfo(context.Context, string) (slack.ChannelInfo, error) {
	return slack.ChannelInfo{}, errors.New("flaky")
}

// A thread whose channel's name cannot be read is opened all the same,
// with no name recorded.
func TestOpenThreadWithoutChannelName(t *testing.T) {
	s, f := testServer(t)
	f.RenameChannel("C1", "example-one")
	s.Slack = noChannelInfo{f}
	s.OpenThread = map[string][]string{"C1": {"workstation"}}
	got, err := answer(t, s, "workstation", Request{Cmd: OpenThread, Channel: "C1", Text: "nightly report"})
	if err != nil {
		t.Fatal(err)
	}
	if owner, err := s.Store.Owner(t.Context(), got.Thread); err != nil || owner != "workstation" {
		t.Fatalf("owner of the new thread = %q, %v; want workstation", owner, err)
	}
	if name, err := s.Store.ChannelName(t.Context(), got.Thread); err != nil || name != "" {
		t.Fatalf("channel name of the new thread = %q, %v; want none", name, err)
	}
}

// lastPost is a Slack API that remembers the ts of the last Post and can
// fail Delete.
type lastPost struct {
	slack.API
	ts         string
	failDelete bool
	// hangDelete makes Delete wait until its context is done.
	hangDelete bool
}

func (l *lastPost) Post(ctx context.Context, channel, text string) (string, error) {
	ts, err := l.API.Post(ctx, channel, text)
	l.ts = ts
	return ts, err
}

func (l *lastPost) Delete(ctx context.Context, channel, ts string) error {
	if l.failDelete {
		return errors.New("slack is down")
	}
	if l.hangDelete {
		<-ctx.Done()
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return l.API.Delete(ctx, channel, ts)
}

// When the owner of a new thread cannot be recorded, its message is
// deleted and the caller gets an error.
func TestOpenThreadUndoneWhenClaimFails(t *testing.T) {
	s, f := testServer(t)
	posts := &lastPost{API: f}
	s.Slack = posts
	s.OpenThread = map[string][]string{"C1": {"workstation"}}
	s.Store.Close()
	ctx := t.Context()

	_, err := answer(t, s, "workstation", Request{Cmd: OpenThread, Channel: "C1", Text: "nightly report"})
	if err == nil || !strings.Contains(err.Error(), "deleted") {
		t.Fatalf("open-thread with a failing store = %v, want an error saying the message was deleted", err)
	}
	if posts.ts == "" {
		t.Fatal("open-thread did not post")
	}
	if _, err := f.Replies(ctx, "C1", posts.ts); !errors.Is(err, slack.ErrNotFound) {
		t.Fatalf("the thread is still in Slack: %v", err)
	}

	// The caller has given up: the message is deleted all the same.
	req := []byte(`{"cmd":"open-thread","channel":"C1","text":"nightly report"}`)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.Answer(canceled, "workstation", req); err == nil || !strings.Contains(err.Error(), "deleted") {
		t.Fatalf("open-thread for a caller that gave up = %v, want the message deleted", err)
	}
	if _, err := f.Replies(ctx, "C1", posts.ts); !errors.Is(err, slack.ErrNotFound) {
		t.Fatalf("the thread of a caller that gave up is still in Slack: %v", err)
	}

	posts.failDelete = true
	_, err = answer(t, s, "workstation", Request{Cmd: OpenThread, Channel: "C1", Text: "nightly report"})
	if err == nil || !strings.Contains(err.Error(), "stays in Slack") {
		t.Fatalf("open-thread when the delete fails too = %v, want an error saying the thread stays", err)
	}

	// A delete that hangs gives up at deleteTimeout, even for a caller that
	// gave up.
	posts.failDelete, posts.hangDelete = false, true
	defer func(d time.Duration) { deleteTimeout = d }(deleteTimeout)
	deleteTimeout = 10 * time.Millisecond
	if _, err := s.Answer(canceled, "workstation", req); err == nil || !strings.Contains(err.Error(), "stays in Slack") {
		t.Fatalf("open-thread when the delete hangs = %v, want an error saying the thread stays", err)
	}
}

func TestThreads(t *testing.T) {
	s, _ := testServer(t)
	for thread, client := range map[string]string{"C1/2": "workstation", "C1/1": "workstation", "C1/3": "datamachine"} {
		if err := s.Store.Claim(t.Context(), thread, client, ""); err != nil {
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

// A request for approval is refused when the hub runs no approvals, or
// when it is not well formed; otherwise it gets an approval id and a
// card.
func TestRequestApproval(t *testing.T) {
	s, f := testServer(t)
	f.AddChannel("C9", "approvals")
	action := []byte(`{"op": "delete"}`)
	good := Request{Cmd: RequestApproval, Agent: "ops-exec", Text: "delete b", Action: action}
	if _, err := answer(t, s, "workstation", good); !errors.Is(err, link.ErrDenied) || !strings.Contains(err.Error(), "not set up") {
		t.Fatalf("request-approval without approvals = %v, want denied as not set up", err)
	}
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	s.Approvals = &approval.Flow{Store: s.Store, Slack: f, Key: priv, Channel: "C9", Approvers: []string{"U1"}}
	tests := []struct {
		name string
		req  Request
		want error
	}{
		{"no agent", Request{Cmd: RequestApproval, Text: "x", Action: action}, link.ErrBadRequest},
		{"no text", Request{Cmd: RequestApproval, Agent: "a", Action: action}, link.ErrBadRequest},
		{"no action", Request{Cmd: RequestApproval, Agent: "a", Text: "x"}, link.ErrBadRequest},
		{"action not JSON", Request{Cmd: RequestApproval, Agent: "a", Text: "x", Action: []byte("{")}, link.ErrBadRequest},
		{"action too big for a card", Request{Cmd: RequestApproval, Agent: "a", Text: "x", Action: append([]byte(`"`), append(bytes.Repeat([]byte("x"), slack.MaxParamChunks*slack.ParamChunk), '"')...)}, link.ErrBadRequest},
	}
	for _, tt := range tests {
		if _, err := answer(t, s, "workstation", tt.req); !errors.Is(err, tt.want) {
			t.Errorf("%s: request-approval = %v, want %v", tt.name, err, tt.want)
		}
	}
	if _, cards := f.Cards("C9"); len(cards) != 0 {
		t.Fatalf("cards after refused requests = %+v, want none", cards)
	}
	reply, err := answer(t, s, "workstation", good)
	if err != nil || reply.ApprovalID == "" {
		t.Fatalf("request-approval = %+v, %v; want an approval id", reply, err)
	}
	_, cards := f.Cards("C9")
	if len(cards) != 1 || cards[0].ID != reply.ApprovalID || cards[0].Params != string(action) || cards[0].Machine != "workstation" || cards[0].Agent != "ops-exec" {
		t.Fatalf("cards = %+v, want one for %s from ops-exec on workstation with the action as given", cards, reply.ApprovalID)
	}
}

// Fetch serves any file the bot can see to any client, within the size
// limit; a file Slack no longer has is not found, and nothing is served
// without Slack.
func TestFetch(t *testing.T) {
	s, f := testServer(t)
	s.MaxFetchBytes = 10
	ctx := t.Context()
	f.AddFile(slack.File{ID: "F1", Name: "shot.png", Mimetype: "image/png", URL: "https://example.invalid/F1"}, []byte("PNG..."))
	f.AddFile(slack.File{ID: "F2", Name: "big.png", Mimetype: "image/png", URL: "https://example.invalid/F2"}, []byte("a dozen bytes"))
	file, body, err := s.Fetch(ctx, "workstation", "F1")
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(body)
	body.Close()
	if err != nil || string(b) != "PNG..." || file != (link.File{Name: "shot.png", Mimetype: "image/png", Size: 6}) {
		t.Fatalf("Fetch(F1) = %+v, %q, %v; want the file and its content", file, b, err)
	}
	if _, _, err := s.Fetch(ctx, "workstation", "F2"); !errors.Is(err, link.ErrDenied) || !strings.Contains(err.Error(), "13 bytes, over the hub's limit of 10") {
		t.Fatalf("Fetch(F2) = %v, want it denied for its size", err)
	}
	f.RemoveFile("F1")
	if _, _, err := s.Fetch(ctx, "workstation", "F1"); !errors.Is(err, link.ErrNotFound) || !strings.Contains(err.Error(), "no longer exists in Slack") {
		t.Fatalf("Fetch of a deleted file = %v, want not found", err)
	}
	if _, _, err := s.Fetch(ctx, "workstation", "../F1"); !errors.Is(err, link.ErrBadRequest) {
		t.Fatalf("Fetch(../F1) = %v, want a bad request", err)
	}
	s.Slack = nil
	if _, _, err := s.Fetch(ctx, "workstation", "F2"); !errors.Is(err, errNoSlack) {
		t.Fatalf("Fetch without Slack = %v, want errNoSlack", err)
	}
}

// Upload posts the files in the thread as a message from the client,
// within the limits, which are checked before any content is read; a
// file short of its size fails the upload.
func TestUpload(t *testing.T) {
	s, f := testServer(t)
	s.MaxUploadBytes, s.MaxUploadFiles = 10, 2
	ctx := t.Context()
	ts, err := f.Start("C1", "U1", "please fix the build")
	if err != nil {
		t.Fatal(err)
	}
	thread := slack.ThreadKey("C1", ts)
	u := link.Upload{Thread: thread, Text: "see these", Files: []link.FileHeader{{Name: "shot.png", Size: 6}, {Name: "build.log", Size: 5}}}
	if err := s.Upload(ctx, "workstation", u, strings.NewReader("PNG...error")); err != nil {
		t.Fatal(err)
	}
	ms, err := f.Replies(ctx, "C1", ts)
	if err != nil || len(ms) != 2 || ms[1].Text != "see these" || ms[1].Machine != "workstation" || len(ms[1].Files) != 2 || ms[1].Files[0].Name != "shot.png" || ms[1].Files[1].Size != 5 {
		t.Fatalf("thread = %+v, %v; want the upload as a message from workstation with both files", ms, err)
	}
	if body, err := f.Download(ctx, ms[1].Files[0]); err != nil {
		t.Fatal(err)
	} else if b, _ := io.ReadAll(body); string(b) != "PNG..." {
		t.Fatalf("the uploaded file holds %q", b)
	}
	refused := []struct {
		name string
		u    link.Upload
		kind error
		msg  string
	}{
		{"no files", link.Upload{Thread: thread}, link.ErrBadRequest, "at least one file"},
		{"too many files", link.Upload{Thread: thread, Files: []link.FileHeader{{Name: "a", Size: 1}, {Name: "b", Size: 1}, {Name: "c", Size: 1}}}, link.ErrBadRequest, "at most 2 files, got 3"},
		{"too big a file", link.Upload{Thread: thread, Files: []link.FileHeader{{Name: "a", Size: 11}}}, link.ErrBadRequest, "11 bytes, over the hub's limit of 10"},
		{"an empty file", link.Upload{Thread: thread, Files: []link.FileHeader{{Name: "a", Size: 0}}}, link.ErrBadRequest, "file a is empty"},
		{"a path for a name", link.Upload{Thread: thread, Files: []link.FileHeader{{Name: "../a", Size: 1}}}, link.ErrBadRequest, "is not a file name"},
		{"a malformed thread", link.Upload{Thread: "C1", Files: []link.FileHeader{{Name: "a", Size: 1}}}, link.ErrBadRequest, "not a thread key"},
		{"a thread that does not exist", link.Upload{Thread: "C1/1600000000.000001", Files: []link.FileHeader{{Name: "a", Size: 1}}}, link.ErrNotFound, "no thread"},
	}
	for _, tt := range refused {
		r := strings.NewReader("abcdefghijklmnop")
		err := s.Upload(ctx, "workstation", tt.u, r)
		if !errors.Is(err, tt.kind) || !strings.Contains(err.Error(), tt.msg) {
			t.Errorf("%s: Upload = %v, want %v saying %q", tt.name, err, tt.kind, tt.msg)
		}
		if tt.kind == link.ErrBadRequest && r.Len() != 16 {
			t.Errorf("%s: %d bytes of content were read before the refusal", tt.name, 16-r.Len())
		}
	}
	// Content short of the declared size fails, and posts nothing.
	short := link.Upload{Thread: thread, Files: []link.FileHeader{{Name: "a.txt", Size: 5}}}
	if err := s.Upload(ctx, "workstation", short, strings.NewReader("abc")); err == nil || !strings.Contains(err.Error(), "file a.txt ended 2 bytes short") {
		t.Fatalf("Upload of a short file = %v, want an error saying so", err)
	}
	if ms, _ := f.Replies(ctx, "C1", ts); len(ms) != 2 {
		t.Fatalf("thread has %d messages after a failed upload, want 2", len(ms))
	}
	s.Slack = nil
	if err := s.Upload(ctx, "workstation", u, strings.NewReader("PNG...error")); !errors.Is(err, errNoSlack) {
		t.Fatalf("Upload without Slack = %v, want errNoSlack", err)
	}
}
