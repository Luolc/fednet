package hubapi

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Luolc/fednet/internal/approval"
	"github.com/Luolc/fednet/internal/ask"
	"github.com/Luolc/fednet/internal/link"
	"github.com/Luolc/fednet/internal/outbound"
	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/question"
	"github.com/Luolc/fednet/internal/route"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
	slackgo "github.com/slack-go/slack"
)

func testServer(t *testing.T) (*Server, *slack.Fake) {
	t.Helper()
	return testServerAt(t, filepath.Join(t.TempDir(), "hub.db"))
}

// testServerAt is testServer with its database at path.
func testServerAt(t *testing.T, path string) (*Server, *slack.Fake) {
	t.Helper()
	st, err := store.OpenHub(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	f := &slack.Fake{}
	f.AddChannel("C1", "repo: fednet")
	return &Server{Store: st, Slack: f}, f
}

// setRoute makes rt the route of its channel.
func setRoute(t *testing.T, h *store.Hub, rt store.Route) {
	t.Helper()
	if err := h.SetRoute(t.Context(), rt.Channel, &rt, store.RouteChange{}); err != nil {
		t.Fatal(err)
	}
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

// React and Unreact put the bot's reaction on a thread's first message
// and take it off, and doing either twice is no error.
func TestReact(t *testing.T) {
	s, f := testServer(t)
	ts, err := f.Start("C1", "U1", "please fix the build")
	if err != nil {
		t.Fatal(err)
	}
	key := slack.ThreadKey("C1", ts)
	steps := []struct {
		cmd, name string
		want      []string
	}{
		{React, "hourglass_flowing_sand", []string{"hourglass_flowing_sand"}},
		{React, "hourglass_flowing_sand", []string{"hourglass_flowing_sand"}},
		{React, "question", []string{"hourglass_flowing_sand", "question"}},
		{Unreact, "hourglass_flowing_sand", []string{"question"}},
		{Unreact, "hourglass_flowing_sand", []string{"question"}},
	}
	for _, st := range steps {
		if _, err := answer(t, s, "workstation", Request{Cmd: st.cmd, Thread: key, Name: st.name}); err != nil {
			t.Fatalf("%s %s = %v", st.cmd, st.name, err)
		}
		if got := f.Reactions("C1", ts); !slices.Equal(got, st.want) {
			t.Fatalf("after %s %s the reactions are %v, want %v", st.cmd, st.name, got, st.want)
		}
	}

	tests := []struct {
		thread, name string
		want         error
	}{
		{slack.ThreadKey("C1", "1600000000.000001"), "question", link.ErrNotFound},
		{"C1", "question", link.ErrBadRequest},
		{key, "", link.ErrBadRequest},
		{key, ":question:", link.ErrBadRequest},
	}
	for _, tt := range tests {
		if _, err := answer(t, s, "workstation", Request{Cmd: React, Thread: tt.thread, Name: tt.name}); !errors.Is(err, tt.want) {
			t.Errorf("react %q %q = %v, want %v", tt.thread, tt.name, err, tt.want)
		}
	}

	s.Slack = noReactionScope{f}
	_, err = answer(t, s, "workstation", Request{Cmd: React, Thread: key, Name: "question"})
	if !errors.Is(err, link.ErrDenied) || !strings.Contains(err.Error(), "reactions:write") {
		t.Fatalf("react without the scope = %v, want denied naming reactions:write", err)
	}
}

// noReactionScope is a Slack API whose token lacks reactions:write.
type noReactionScope struct{ slack.API }

func (noReactionScope) React(context.Context, string, string, string) error {
	return fmt.Errorf("slack: reactions.add: %w", slackgo.SlackErrorResponse{Err: "missing_scope"})
}

func TestAdopt(t *testing.T) {
	s, _ := testServer(t)
	ctx := t.Context()
	const key = "C1/1700000000.000100"
	if _, _, err := s.Store.ClaimAndEnqueue(ctx, key, "old-workstation", "", "", []byte("first")); err != nil {
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
	if _, _, err := s.Store.ClaimAndEnqueue(t.Context(), key, "old-workstation", "", "", []byte("first")); err != nil {
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
	setRoute(t, s.Store, store.Route{Channel: "C1", Scope: "control-plane", OpenThread: []string{"workstation"}})
	setRoute(t, s.Store, store.Route{Channel: "C9", OpenThread: []string{"workstation"}})
	setRoute(t, s.Store, store.Route{Channel: store.DMRoute, Machine: "workstation"})
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
	if scope, err := s.Store.ThreadScope(ctx, got.Thread); err != nil || scope != "control-plane" {
		t.Fatalf("scope of the new thread = %q, %v; want control-plane, its channel's", scope, err)
	}

	tests := []struct {
		name   string
		client string
		req    Request
		want   error
	}{
		{"a client the channel does not list", "datamachine", Request{Cmd: OpenThread, Channel: "C1", Text: "x"}, link.ErrDenied},
		{"a channel with no route", "workstation", Request{Cmd: OpenThread, Channel: "C2", Text: "x"}, link.ErrDenied},
		{"direct messages", "workstation", Request{Cmd: OpenThread, Channel: store.DMRoute, Text: "x"}, link.ErrDenied},
		{"a channel Slack does not know", "workstation", Request{Cmd: OpenThread, Channel: "C9", Text: "x"}, link.ErrNotFound},
		{"no text", "workstation", Request{Cmd: OpenThread, Channel: "C1"}, link.ErrBadRequest},
		{"a text over MaxTextChars", "workstation", Request{Cmd: OpenThread, Channel: "C1", Text: strings.Repeat("字", MaxTextChars+1)}, link.ErrBadRequest},
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
	setRoute(t, s.Store, store.Route{Channel: "C1", OpenThread: []string{"workstation"}})
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
	// posted, if set, is called after a Post, once.
	posted func()
}

func (l *lastPost) Post(ctx context.Context, channel, text string) (string, error) {
	ts, err := l.API.Post(ctx, channel, text)
	l.ts = ts
	if l.posted != nil {
		l.posted()
		l.posted = nil
	}
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
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "hub.db")
	s, f := testServerAt(t, path)
	posts := &lastPost{API: f}
	s.Slack = posts
	setRoute(t, s.Store, store.Route{Channel: "C1", OpenThread: []string{"workstation"}})
	// Every owner the hub records fails, after the route is read.
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "CREATE TRIGGER no_owner BEFORE INSERT ON owner BEGIN SELECT RAISE(ABORT, 'no owner'); END"); err != nil {
		t.Fatal(err)
	}

	_, err = answer(t, s, "workstation", Request{Cmd: OpenThread, Channel: "C1", Text: "nightly report"})
	if err == nil || !strings.Contains(err.Error(), "deleted") {
		t.Fatalf("open-thread with a failing store = %v, want an error saying the message was deleted", err)
	}
	if posts.ts == "" {
		t.Fatal("open-thread did not post")
	}
	if _, err := f.Replies(ctx, "C1", posts.ts); !errors.Is(err, slack.ErrNotFound) {
		t.Fatalf("the thread is still in Slack: %v", err)
	}

	// The caller gives up once the message is posted: it is deleted all
	// the same.
	req := []byte(`{"cmd":"open-thread","channel":"C1","text":"nightly report"}`)
	givesUp := func() context.Context {
		c, cancel := context.WithCancel(ctx)
		posts.posted = cancel
		return c
	}
	if _, err := s.Answer(givesUp(), "workstation", req); err == nil || !strings.Contains(err.Error(), "deleted") {
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
	if _, err := s.Answer(givesUp(), "workstation", req); err == nil || !strings.Contains(err.Error(), "stays in Slack") {
		t.Fatalf("open-thread when the delete hangs = %v, want an error saying the thread stays", err)
	}
}

func TestThreads(t *testing.T) {
	s, _ := testServer(t)
	for thread, client := range map[string]string{"C1/2": "workstation", "C1/1": "workstation", "C1/3": "datamachine"} {
		if err := s.Store.Claim(t.Context(), thread, client, "", ""); err != nil {
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
	s.Users = map[string]string{"U1": "maintainer", "U2": ""}
	if _, err := answer(t, s, "workstation", Request{Cmd: DM, User: "U1", Text: "daily report"}); err != nil {
		t.Fatal(err)
	}
	if got := f.DMs("U1"); !slices.Equal(got, []string{"daily report"}) {
		t.Fatalf("DMs to U1 = %q, want the report", got)
	}
	// The limit counts characters, not bytes.
	full := strings.Repeat("字", MaxTextChars)
	if _, err := answer(t, s, "workstation", Request{Cmd: DM, User: "U2", Text: full}); err != nil {
		t.Fatalf("dm of MaxTextChars characters = %v, want it sent", err)
	}

	tests := []struct {
		name string
		req  Request
		want error
	}{
		{"a user not on the list", Request{Cmd: DM, User: "U9", Text: "x"}, link.ErrDenied},
		{"no user", Request{Cmd: DM, Text: "x"}, link.ErrBadRequest},
		{"no text", Request{Cmd: DM, User: "U1"}, link.ErrBadRequest},
		{"a text over MaxTextChars", Request{Cmd: DM, User: "U1", Text: full + "字"}, link.ErrBadRequest},
	}
	for _, tt := range tests {
		if _, err := answer(t, s, "workstation", tt.req); !errors.Is(err, tt.want) {
			t.Errorf("%s: dm = %v, want %v", tt.name, err, tt.want)
		}
	}
	if got := f.DMs("U9"); len(got) != 0 {
		t.Fatalf("DMs to the user not on the list = %q, want none", got)
	}
	if got := f.DMs("U1"); len(got) != 1 {
		t.Fatalf("DMs to U1 = %q, want only the report", got)
	}
}

func TestCheckMentions(t *testing.T) {
	s, _ := testServer(t)
	s.Users = map[string]string{"U1": "maintainer", "U2": ""}
	if _, err := answer(t, s, "workstation", Request{Cmd: CheckMentions, Mentions: []string{"U1", "U2"}}); err != nil {
		t.Fatalf("check-mentions of users on the list = %v", err)
	}
	if _, err := answer(t, s, "workstation", Request{Cmd: CheckMentions, Mentions: []string{"U1", "U9"}}); !errors.Is(err, link.ErrDenied) {
		t.Fatalf("check-mentions of a user not on the list = %v, want ErrDenied", err)
	}
	if _, err := answer(t, s, "workstation", Request{Cmd: CheckMentions}); !errors.Is(err, link.ErrBadRequest) {
		t.Fatalf("check-mentions of no one = %v, want ErrBadRequest", err)
	}
}

// A client deletes its own post, every message it went out as, and
// nothing else: not another client's post, not a post still on its way,
// not a progress card.
func TestDelete(t *testing.T) {
	s, f := testServer(t)
	root, err := f.Start("C1", "U1", "please fix the build")
	if err != nil {
		t.Fatal(err)
	}
	key := slack.ThreadKey("C1", root)
	poster := &outbound.Poster{Store: s.Store, Slack: f, MaxChars: 5}
	n := 0
	put := func(client string, m payload.Message) string {
		t.Helper()
		b, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		n++
		id := fmt.Sprintf("m%d", n)
		if _, err := s.Store.Inbox.PutFrom(t.Context(), client, store.Message{MsgID: id, Payload: b}); err != nil {
			t.Fatal(err)
		}
		return id
	}
	post := func(client, text string) string {
		return put(client, payload.Message{Type: payload.Post, Thread: key, Text: text})
	}
	long := post("workstation", "abcdefgh")
	other := post("datamachine", "mine")
	card := put("workstation", payload.Message{Type: payload.Progress, Thread: key, Title: "building"})
	cut := post("workstation", "ijklmnop")
	if err := poster.Pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	pending := post("workstation", "later")
	thread := func() []string {
		t.Helper()
		ms, err := f.Replies(t.Context(), "C1", root)
		if err != nil {
			t.Fatal(err)
		}
		var texts []string
		for _, m := range ms {
			texts = append(texts, m.Text)
		}
		return texts
	}
	if got, want := thread(), []string{"please fix the build", "abcde", "fgh", "mine", "进度：building", "ijklm", "nop"}; !slices.Equal(got, want) {
		t.Fatalf("thread = %q, want %q", got, want)
	}

	if _, err := answer(t, s, "workstation", Request{Cmd: Delete, MsgID: long}); err != nil {
		t.Fatal(err)
	}
	// One message of a post already gone from Slack does not stop the
	// rest from going.
	p, err := s.Store.Posted(t.Context(), cut)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Delete(t.Context(), "C1", p.TS[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := answer(t, s, "workstation", Request{Cmd: Delete, MsgID: cut}); err != nil {
		t.Fatal(err)
	}
	if got, want := thread(), []string{"please fix the build", "mine", "进度：building"}; !slices.Equal(got, want) {
		t.Fatalf("thread after the deletes = %q, want %q", got, want)
	}

	tests := []struct {
		name   string
		client string
		msgID  string
		want   error
	}{
		{"another client's post", "workstation", other, link.ErrDenied},
		{"a post deleted already", "workstation", long, link.ErrNotFound},
		{"no such msg_id", "workstation", "m99", link.ErrNotFound},
		{"a post not in Slack yet", "workstation", pending, link.ErrBusy},
		{"a progress card", "workstation", card, link.ErrBadRequest},
		{"no msg_id", "workstation", "", link.ErrBadRequest},
	}
	for _, tt := range tests {
		if _, err := answer(t, s, tt.client, Request{Cmd: Delete, MsgID: tt.msgID}); !errors.Is(err, tt.want) {
			t.Errorf("%s: delete = %v, want %v", tt.name, err, tt.want)
		}
	}
	if got := thread(); !slices.Contains(got, "mine") {
		t.Fatalf("thread = %q, want the other client's post still there", got)
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

// routes answers each route with its channel's name, but not the route
// of direct messages; a channel Slack gives no name for goes without one,
// and the routes go anyway. A name is not asked for again at once, so a
// rename shows later.
func TestRoutesWithNames(t *testing.T) {
	s, f := testServer(t)
	f.RenameChannel("C1", "repo-fednet")
	for _, ch := range []string{"C1", "C8", store.DMRoute} {
		setRoute(t, s.Store, store.Route{Channel: ch, Machine: "workstation"})
	}
	want := []store.Route{{Channel: "C1", Machine: "workstation", Name: "repo-fednet"}, {Channel: "C8", Machine: "workstation"}, {Channel: store.DMRoute, Machine: "workstation"}}
	got, err := answer(t, s, "workstation", Request{Cmd: Routes})
	if err != nil || !reflect.DeepEqual(got.Routes, want) {
		t.Fatalf("routes = %+v, %v; want %+v", got.Routes, err, want)
	}
	f.RenameChannel("C1", "repo-renamed")
	if got, err := answer(t, s, "workstation", Request{Cmd: Routes}); err != nil || got.Routes[0].Name != "repo-fednet" {
		t.Fatalf("routes right after a rename = %+v, %v; want the name as it was", got.Routes, err)
	}
}

// request-route checks the change against the routes and the clients the
// hub knows, makes it whole and posts its card; the hub needs no key for
// it, but approvers and their channel.
func TestRequestRoute(t *testing.T) {
	s, f := testServer(t)
	f.AddChannel("C9", "approvals")
	ctx := t.Context()
	for _, c := range []string{"workstation", "datamachine", "retired"} {
		if err := s.Store.Register(ctx, c, []byte("h")); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Store.Revoke(ctx, "retired"); err != nil {
		t.Fatal(err)
	}
	setRoute(t, s.Store, store.Route{Channel: "C2", Machine: "datamachine", OpenThread: []string{"datamachine", "workstation"}})
	req := func(q RouteRequest) Request {
		return Request{Cmd: RequestRoute, Agent: "lead", Text: "new repo", Route: &q}
	}
	add := req(RouteRequest{Channel: "C1", Machine: "workstation", Scope: "control-plane"})
	if _, err := answer(t, s, "workstation", add); !errors.Is(err, link.ErrDenied) || !strings.Contains(err.Error(), "not set up") {
		t.Fatalf("request-route without approvals = %v, want denied as not set up", err)
	}
	s.Approvals = &approval.Flow{Store: s.Store, Slack: f, Channel: "C9", Approvers: []string{"U1"}}
	none := []string{}
	tests := []struct {
		name string
		req  Request
		want error
	}{
		{"no agent", Request{Cmd: RequestRoute, Text: "x", Route: &RouteRequest{Channel: "C1", Machine: "workstation"}}, link.ErrBadRequest},
		{"no text", Request{Cmd: RequestRoute, Agent: "a", Route: &RouteRequest{Channel: "C1", Machine: "workstation"}}, link.ErrBadRequest},
		{"no route", Request{Cmd: RequestRoute, Agent: "a", Text: "x"}, link.ErrBadRequest},
		{"not a channel id", req(RouteRequest{Channel: "#repo-example", Machine: "workstation"}), link.ErrBadRequest},
		{"a scope out of [a-z0-9-]", req(RouteRequest{Channel: "C1", Machine: "workstation", Scope: "Control_Plane"}), link.ErrBadRequest},
		{"an unknown machine", req(RouteRequest{Channel: "C1", Machine: "nobody"}), link.ErrBadRequest},
		{"a revoked machine", req(RouteRequest{Channel: "C1", Machine: "retired"}), link.ErrBadRequest},
		{"an unknown client to open threads", req(RouteRequest{Channel: "C1", Machine: "workstation", OpenThread: &[]string{"nobody"}}), link.ErrBadRequest},
		{"a route that does nothing", req(RouteRequest{Channel: "C1", OpenThread: &none}), link.ErrBadRequest},
		{"direct messages with no machine", req(RouteRequest{Channel: store.DMRoute}), link.ErrBadRequest},
		{"direct messages with open_thread", req(RouteRequest{Channel: store.DMRoute, Machine: "workstation", OpenThread: &none}), link.ErrBadRequest},
		{"a delete of no route", req(RouteRequest{Channel: "C1", Delete: true}), link.ErrNotFound},
		{"a delete with a machine", req(RouteRequest{Channel: "C2", Machine: "workstation", Delete: true}), link.ErrBadRequest},
	}
	for _, tt := range tests {
		if _, err := answer(t, s, "workstation", tt.req); !errors.Is(err, tt.want) {
			t.Errorf("%s: request-route = %v, want %v", tt.name, err, tt.want)
		}
	}
	if _, cards := f.Cards("C9"); len(cards) != 0 {
		t.Fatalf("cards after refused requests = %+v, want none", cards)
	}

	// The card shows the route the channel will have: a new route lists
	// its machine to open threads, one there is keeps its list unless
	// given another.
	for _, tt := range []struct {
		req    Request
		params string
	}{
		{add, `{"channel":"C1","machine":"workstation","scope":"control-plane","open_thread":["workstation"]}`},
		{req(RouteRequest{Channel: "C2", Machine: "workstation"}), `{"channel":"C2","machine":"workstation","open_thread":["datamachine","workstation"]}`},
		{req(RouteRequest{Channel: "C2", Machine: "workstation", OpenThread: &none}), `{"channel":"C2","machine":"workstation"}`},
		{req(RouteRequest{Channel: store.DMRoute, Machine: "datamachine", Scope: "personal"}), `{"channel":"dm","machine":"datamachine","scope":"personal"}`},
		{req(RouteRequest{Channel: "C2", Delete: true}), `{"channel":"C2","delete":true}`},
	} {
		reply, err := answer(t, s, "workstation", tt.req)
		if err != nil || reply.ApprovalID == "" {
			t.Fatalf("request-route %+v = %+v, %v; want an approval id", tt.req.Route, reply, err)
		}
		_, cards := f.Cards("C9")
		c := cards[len(cards)-1]
		var params bytes.Buffer
		if err := json.Compact(&params, []byte(c.Params)); err != nil {
			t.Fatal(err)
		}
		if c.ID != reply.ApprovalID || params.String() != tt.params || c.Summary != "改路由：new repo" || c.Machine != "workstation" || c.Agent != "lead" {
			t.Errorf("card for %+v = %+v, want %s with %s", tt.req.Route, c, reply.ApprovalID, tt.params)
		}
		// While the card waits, the channel takes no other request.
		if _, err := answer(t, s, "workstation", tt.req); !errors.Is(err, link.ErrBusy) {
			t.Errorf("a second request for %s while one waits = %v, want busy", tt.req.Route.Channel, err)
		}
		if _, err := s.Store.DecideApproval(ctx, reply.ApprovalID, func(*store.Hub, store.Approval) (store.Decision, error) {
			return store.Decision{Status: payload.Rejected, By: "U1", At: time.Now()}, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	// Nothing changes before an approval.
	if got, err := answer(t, s, "datamachine", Request{Cmd: Routes}); err != nil || len(got.Routes) != 1 || got.Routes[0].Machine != "datamachine" {
		t.Fatalf("routes = %+v, %v; want C2 to the data machine as before", got.Routes, err)
	}
}

// request-channel checks the channel to create and its route, and posts
// a card that shows both; while one waits, another for the same name is
// refused.
func TestRequestChannel(t *testing.T) {
	s, f := testServer(t)
	f.AddChannel("C9", "approvals")
	ctx := t.Context()
	s.Users = map[string]string{"U1": "maintainer", "U2": "colleague"}
	for _, c := range []string{"workstation", "datamachine"} {
		if err := s.Store.Register(ctx, c, []byte("h")); err != nil {
			t.Fatal(err)
		}
	}
	req := func(q RouteRequest) Request {
		return Request{Cmd: RequestChannel, Agent: "lead", Text: "new repo", Route: &q}
	}
	good := req(RouteRequest{Machine: "workstation", Scope: "data", Create: &approval.NewChannel{Name: "repo-example", Purpose: "the example repo", Invite: []string{"U2"}}})
	if _, err := answer(t, s, "workstation", good); !errors.Is(err, link.ErrDenied) || !strings.Contains(err.Error(), "not set up") {
		t.Fatalf("request-channel without approvals = %v, want denied as not set up", err)
	}
	s.Approvals = &approval.Flow{Store: s.Store, Slack: f, Channel: "C9", Approvers: []string{"U1"}}
	named := func(name string) *approval.NewChannel { return &approval.NewChannel{Name: name} }
	tests := []struct {
		name string
		req  Request
	}{
		{"no channel to create", req(RouteRequest{Machine: "workstation"})},
		{"upper case in the name", req(RouteRequest{Machine: "workstation", Create: named("Repo-Example")})},
		{"a name over 80", req(RouteRequest{Machine: "workstation", Create: named(strings.Repeat("a", 81))})},
		{"a channel id given", req(RouteRequest{Channel: "C1", Machine: "workstation", Create: named("repo-example")})},
		{"no machine", req(RouteRequest{Create: named("repo-example")})},
		{"an unknown machine", req(RouteRequest{Machine: "nobody", Create: named("repo-example")})},
		{"a bad scope", req(RouteRequest{Machine: "workstation", Scope: "Data", Create: named("repo-example")})},
		{"someone off the user list to invite", req(RouteRequest{Machine: "workstation", Create: &approval.NewChannel{Name: "repo-example", Invite: []string{"U9"}}})},
		{"a create through request-route", Request{Cmd: RequestRoute, Agent: "lead", Text: "x", Route: &RouteRequest{Channel: "C1", Machine: "workstation", Create: named("repo-example")}}},
	}
	for _, tt := range tests {
		if _, err := answer(t, s, "workstation", tt.req); !errors.Is(err, link.ErrBadRequest) {
			t.Errorf("%s: %v, want a bad request", tt.name, err)
		}
	}
	reply, err := answer(t, s, "workstation", good)
	if err != nil {
		t.Fatal(err)
	}
	_, cards := f.Cards("C9")
	var params bytes.Buffer
	if len(cards) != 1 || json.Compact(&params, []byte(cards[0].Params)) != nil {
		t.Fatalf("cards = %+v, want one", cards)
	}
	want := `{"machine":"workstation","scope":"data","open_thread":["workstation"],"create":{"name":"repo-example","purpose":"the example repo","invite":["U2"]}}`
	if cards[0].ID != reply.ApprovalID || params.String() != want || cards[0].Summary != "建 channel 并写路由：new repo" {
		t.Fatalf("card = %+v, want %s with %s", cards[0], reply.ApprovalID, want)
	}
	if _, err := answer(t, s, "workstation", good); !errors.Is(err, link.ErrBusy) {
		t.Fatalf("a second request for repo-example while one waits = %v, want busy", err)
	}
	if _, err := answer(t, s, "workstation", req(RouteRequest{Machine: "workstation", Create: named("repo-other")})); err != nil {
		t.Fatalf("a request for another name = %v, want it taken", err)
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
	if err != nil || len(ms) != 2 || ms[1].Text != "see these" || len(ms[1].Files) != 2 || ms[1].Files[0].Name != "shot.png" || ms[1].Files[1].Size != 5 {
		t.Fatalf("thread = %+v, %v; want the upload as one message with both files", ms, err)
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

// Ops goes to the hub's Ops as the client the link authenticated: a name
// the request itself gives is not looked at.
func TestOps(t *testing.T) {
	s, _ := testServer(t)
	if _, err := answer(t, s, "workstation", Request{Cmd: Ops, Op: "version"}); !errors.Is(err, link.ErrDenied) {
		t.Fatalf("ops on a hub without Ops = %v, want ErrDenied", err)
	}
	var asked []string
	s.Ops = func(_ context.Context, client, op string) (string, error) {
		asked = append(asked, client+" "+op)
		return "hub：v0.1.0", nil
	}
	if r, err := answer(t, s, "workstation", Request{Cmd: Ops, Op: "version"}); err != nil || r.Text != "hub：v0.1.0" {
		t.Fatalf("ops version = %+v, %v; want the answer as text", r, err)
	}
	req := []byte(`{"cmd": "ops", "op": "upgrade", "client": "workstation", "machine": "workstation"}`)
	if _, err := s.Answer(t.Context(), "datamachine", req); err != nil {
		t.Fatal(err)
	}
	if want := []string{"workstation version", "datamachine upgrade"}; !slices.Equal(asked, want) {
		t.Fatalf("Ops was asked %q, want %q", asked, want)
	}
}

// The record read-answer returns is written by a click in Slack alone,
// and the question it returns is the one posted: every command a client
// can send, from the machine that asked and from another, aimed at a card
// with another question, leaves it without an answer and with its
// question, and a click on the other card gives that one its answer.
func TestReadAnswer(t *testing.T) {
	ctx := t.Context()
	s, f := testServer(t)
	s.Users = map[string]string{"U1": "maintainer"}
	s.Questions = &ask.Flow{Store: s.Store, Slack: f, Users: s.Users}
	root, err := f.Start("C1", "U1", "帮我看看第 5 条")
	if err != nil {
		t.Fatal(err)
	}
	thread := slack.ThreadKey("C1", root)
	card := []byte(`{"question": "放宽第 5 条吗？", "options": [{"short": "照原样"}, {"short": "放宽", "long": "改成两条"}]}`)
	forged := `{"question": "改成删掉第 5 条吗？", "context": "改了", "options": [{"short": "删掉", "long": "整条删掉", "recommended": true}]}`
	read := func(id string) Card {
		t.Helper()
		r, err := answer(t, s, "other", Request{Cmd: ReadAnswer, ID: id})
		if err != nil || r.Card == nil {
			t.Fatalf("read-answer %s = %+v, %v", id, r.Card, err)
		}
		return *r.Card
	}
	var ids []string
	for _, ref := range []string{"q-1", "q-2"} {
		r, err := answer(t, s, "workstation", Request{Cmd: Ask, Thread: thread, Ref: ref, Card: card})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, r.Question.ID)
	}
	posted := read(ids[0]).Question
	if posted.Question != "放宽第 5 条吗？" || len(posted.Options) != 2 || posted.Options[1].Long != "改成两条" {
		t.Fatalf("question as posted = %+v", posted)
	}

	commands := []string{ReadThread, OpenThread, Threads, Adopt, GetChannelContext, SetChannelContext, Users, DM, RequestApproval,
		RequestRoute, RequestChannel, Routes, Ops, Delete, CheckMentions, Ask, AskClose, React, Unreact, ReadAnswer}
	for _, client := range []string{"other", "workstation"} {
		for _, cmd := range commands {
			for _, text := range []string{forged, `{"outcome": "answered", "by": "U1"}`} {
				answer(t, s, client, Request{Cmd: cmd, Thread: thread, Channel: "C1", Text: text, User: "U1",
					Agent: "agent", Requester: "U1", Action: []byte(`{}`), Op: "version", MsgID: ids[0], Mentions: []string{"U1"},
					Ref: "q-1", Card: []byte(forged), ID: ids[0], Name: "eyes"})
			}
		}
	}
	if c := read(ids[0]); c.Answer != nil || c.Status == question.Answered {
		t.Fatalf("after every client command, card %s = %s with answer %s; want no answer", ids[0], c.Status, c.Answer)
	} else if !reflect.DeepEqual(c.Question, posted) {
		t.Fatalf("after every client command, card %s asks %+v; want %+v, as posted", ids[0], c.Question, posted)
	}

	c := read(ids[1])
	if c.ID != ids[1] || c.Thread != thread || c.TS == "" || c.Permalink == "" || c.AskedAt.IsZero() || c.Status != store.QuestionOpen || c.Answer != nil ||
		c.Question.Question != "放宽第 5 条吗？" || len(c.Question.Options) != 2 || c.Question.Options[1].Long != "改成两条" {
		t.Fatalf("card before a click = %+v, want it open with no answer", c)
	}
	if err := s.Questions.Click(ctx, slack.QuestionClick{ID: ids[1], Action: slack.QuestionSubmitAction, Choice: "1", Note: "可以", User: "U1", Channel: "C1", TS: c.TS}); err != nil {
		t.Fatal(err)
	}
	c = read(ids[1])
	var a question.Answer
	if err := json.Unmarshal(c.Answer, &a); err != nil || c.Status != question.Answered || a.CardID != ids[1] || a.QuestionID != "q-2" ||
		a.Outcome != question.Answered || a.Choice == nil || a.Choice.Index != 1 || a.Note != "可以" || a.By != "U1" || a.At == "" {
		t.Fatalf("card after a click = %s with answer %s (%v), want (b) answered by U1 with the note", c.Status, c.Answer, err)
	}
	if !reflect.DeepEqual(c.Question, posted) {
		t.Fatalf("after a click, card %s asks %+v; want %+v, as posted", ids[1], c.Question, posted)
	}

	if _, err := answer(t, s, "other", Request{Cmd: ReadAnswer, ID: "NOSUCHCARD"}); !errors.Is(err, link.ErrNotFound) {
		t.Errorf("read-answer of no card = %v, want ErrNotFound", err)
	}
	if _, err := answer(t, s, "other", Request{Cmd: ReadAnswer}); !errors.Is(err, link.ErrBadRequest) {
		t.Errorf("read-answer without an id = %v, want ErrBadRequest", err)
	}
}

// An Ask goes out after what its client posted before: it waits until
// that is in Slack, closes the thread's progress card, and only then
// posts the card, which lands below the post. Another client's posts do
// not hold it up; its own that never go out make it busy, with no card.
func TestAskAfterQueuedPosts(t *testing.T) {
	ctx := t.Context()
	s, f := testServer(t)
	s.Questions = &ask.Flow{Store: s.Store, Slack: f}
	poster := &outbound.Poster{Store: s.Store, Slack: f}
	s.CatchUp = poster.CatchUp
	root, err := f.Start("C1", "U1", "please fix the build")
	if err != nil {
		t.Fatal(err)
	}
	key := slack.ThreadKey("C1", root)
	n := 0
	put := func(client string, m payload.Message) {
		t.Helper()
		b, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		n++
		if _, err := s.Store.Inbox.PutFrom(ctx, client, store.Message{MsgID: fmt.Sprintf("m%d", n), Payload: b}); err != nil {
			t.Fatal(err)
		}
	}
	askFrom := func(client, ref string) <-chan error {
		req, err := json.Marshal(Request{Cmd: Ask, Thread: key, Ref: ref, Card: []byte(`{"question": "合并吗？"}`)})
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			_, err := s.Answer(ctx, client, req)
			done <- err
		}()
		return done
	}

	put("workstation", payload.Message{Type: payload.Progress, Thread: key, Title: "building", Items: []slack.ProgressItem{{Text: "build", State: slack.Doing}}})
	if err := poster.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	put("workstation", payload.Message{Type: payload.Post, Thread: key, Text: "build is green"})
	done := askFrom("workstation", "q-1")
	select {
	case err := <-done:
		t.Fatalf("Ask = %v before the post went out", err)
	case <-time.After(20 * time.Millisecond):
	}
	if tss, _ := f.Questions("C1"); len(tss) != 0 {
		t.Fatalf("cards %q in Slack before the post went out, want none", tss)
	}
	if err := poster.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	ms, err := f.Replies(ctx, "C1", root)
	if err != nil {
		t.Fatal(err)
	}
	tss, _ := f.Questions("C1")
	if len(ms) != 4 || ms[1].Text != "进度：building" || ms[2].Text != "build is green" || len(tss) != 1 || ms[3].TS != tss[0] {
		t.Fatalf("thread = %+v, cards at %q; want the progress card, the post, then the question card", ms, tss)
	}
	if card, ok := f.Progress("C1", ms[1].TS); !ok || card.Items[0].State != slack.Done {
		t.Fatalf("progress card = %+v, want it closed", card)
	}

	put("datamachine", payload.Message{Type: payload.Post, Thread: key, Text: "not out yet"})
	if err := <-askFrom("workstation", "q-2"); err != nil {
		t.Fatalf("Ask behind another client's post = %v, want the card", err)
	}

	askWait = 5 * time.Millisecond
	t.Cleanup(func() { askWait = 15 * time.Second })
	put("workstation", payload.Message{Type: payload.Post, Thread: key, Text: "stuck"})
	if err := <-askFrom("workstation", "q-3"); !errors.Is(err, link.ErrBusy) {
		t.Fatalf("Ask behind a post that does not go out = %v, want busy", err)
	}
	if tss, _ := f.Questions("C1"); len(tss) != 2 {
		t.Fatalf("%d cards, want 2: none for the ask that gave up", len(tss))
	}
}

// queued returns the payloads queued for client, decoded.
func queued(t *testing.T, s *Server, client string) []payload.Message {
	t.Helper()
	ds, err := s.Store.Outbox.After(t.Context(), client, 0)
	if err != nil {
		t.Fatal(err)
	}
	var ms []payload.Message
	for _, d := range ds {
		var m payload.Message
		if err := json.Unmarshal(d.Payload, &m); err != nil {
			t.Fatal(err)
		}
		ms = append(ms, m)
	}
	return ms
}

// open-thread for the route's machine makes that machine the owner and
// queues the text for it as a handoff, from the caller, hop 1, and wakes
// the downlinks; only a caller the channel lists and the relay list lets
// deliver to that machine may, and a refused one posts nothing. A caller
// that is the route's machine gets a thread of its own, with nothing
// queued.
func TestOpenThreadForRoute(t *testing.T) {
	s, f := testServer(t)
	f.RenameChannel("C1", "data-jobs")
	posts := &countPosts{API: f}
	s.Slack = posts
	woken := 0
	s.Stored = func() { woken++ }
	s.Relays = route.Relays{"workstation": {"datamachine"}}
	setRoute(t, s.Store, store.Route{Channel: "C1", Machine: "datamachine", Scope: "data", OpenThread: []string{"workstation", "datamachine", "other"}})
	setRoute(t, s.Store, store.Route{Channel: "C2", OpenThread: []string{"workstation"}})
	ctx := t.Context()

	got, err := answer(t, s, "workstation", Request{Cmd: OpenThreadForRoute, Channel: "C1", Text: "new rows to load"})
	if err != nil {
		t.Fatal(err)
	}
	if owner, err := s.Store.Owner(ctx, got.Thread); err != nil || owner != "datamachine" {
		t.Fatalf("owner = %q, %v; want datamachine", owner, err)
	}
	_, ts, _ := slack.ParseThreadKey(got.Thread)
	want := []payload.Message{{Type: payload.Inbound, Thread: got.Thread, Text: "new rows to load", TS: ts, Trigger: payload.Handoff,
		FromClient: "workstation", Hop: 1, Scope: "data", ChannelName: "data-jobs"}}
	if ms := queued(t, s, "datamachine"); !reflect.DeepEqual(ms, want) {
		t.Fatalf("queued for datamachine = %+v, want %+v", ms, want)
	}
	if woken != 1 {
		t.Fatalf("downlinks woken %d times, want 1", woken)
	}

	got, err = answer(t, s, "datamachine", Request{Cmd: OpenThreadForRoute, Channel: "C1", Text: "my own"})
	if err != nil {
		t.Fatal(err)
	}
	if owner, err := s.Store.Owner(ctx, got.Thread); err != nil || owner != "datamachine" || len(queued(t, s, "datamachine")) != 1 || woken != 1 {
		t.Fatalf("the route's machine's own thread: owner %q, %v, %d queued, woken %d; want its own, nothing more", owner, err, len(queued(t, s, "datamachine")), woken)
	}

	for _, tt := range []struct {
		name   string
		client string
		relays route.Relays
		req    Request
	}{
		{"a client the relay list does not list", "other", s.Relays, Request{Cmd: OpenThreadForRoute, Channel: "C1", Text: "x"}},
		{"no relay list", "workstation", nil, Request{Cmd: OpenThreadForRoute, Channel: "C1", Text: "x"}},
		{"a client the channel does not list", "unlisted", route.Relays{"unlisted": {"datamachine"}}, Request{Cmd: OpenThreadForRoute, Channel: "C1", Text: "x"}},
		{"a route with no machine", "workstation", s.Relays, Request{Cmd: OpenThreadForRoute, Channel: "C2", Text: "x"}},
	} {
		s.Relays = tt.relays
		if _, err := answer(t, s, tt.client, tt.req); !errors.Is(err, link.ErrDenied) {
			t.Errorf("%s: open-thread-route = %v, want denied", tt.name, err)
		}
	}
	if posts.n != 2 {
		t.Fatalf("Slack got %d posts, want 2", posts.n)
	}

	// From a thread the caller owns, the hop goes on from that thread's;
	// from one at the limit nothing is posted, and the hub alerts.
	s.Relays = route.Relays{"workstation": {"datamachine"}}
	var alerts []string
	s.Alert = func(_ context.Context, text string) error {
		alerts = append(alerts, text)
		return nil
	}
	if err := s.Store.Claim(ctx, "C9/1.0", "workstation", "", ""); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.SetHop(ctx, "C9/1.0", 2); err != nil {
		t.Fatal(err)
	}
	got, err = answer(t, s, "workstation", Request{Cmd: OpenThreadForRoute, Channel: "C1", Thread: "C9/1.0", Text: "back to you"})
	if err != nil {
		t.Fatal(err)
	}
	if ms := queued(t, s, "datamachine"); len(ms) != 2 || ms[1].Thread != got.Thread || ms[1].Hop != 3 {
		t.Fatalf("queued for datamachine = %+v, want the handoff at hop 3", ms)
	}
	if hop, err := s.Store.Hop(ctx, got.Thread); err != nil || hop != 3 {
		t.Fatalf("hop of the new thread = %d, %v; want 3", hop, err)
	}
	if err := s.Store.SetHop(ctx, "C9/1.0", route.MaxHop); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.Claim(ctx, "C9/2.0", "datamachine", "", ""); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name, from string
		want       error
	}{
		{"from a thread at the limit", "C9/1.0", link.ErrDenied},
		{"from a thread another client owns", "C9/2.0", link.ErrDenied},
		{"from a thread with no owner", "C9/3.0", link.ErrNotFound},
	} {
		if _, err := answer(t, s, "workstation", Request{Cmd: OpenThreadForRoute, Channel: "C1", Thread: tt.from, Text: "x"}); !errors.Is(err, tt.want) {
			t.Errorf("%s: open-thread-route = %v, want %v", tt.name, err, tt.want)
		}
	}
	if posts.n != 3 || len(queued(t, s, "datamachine")) != 2 {
		t.Fatalf("Slack got %d posts and %d are queued, want 3 and 2", posts.n, len(queued(t, s, "datamachine")))
	}
	if len(alerts) != 1 || !strings.Contains(alerts[0], "out of thread C9/1.0 not delivered") {
		t.Fatalf("alerts = %q, want one for C9/1.0", alerts)
	}
	// The route's machine opening a thread of its own delivers nothing,
	// so the hop of the thread it names does not matter.
	if err := s.Store.SetHop(ctx, "C9/2.0", route.MaxHop); err != nil {
		t.Fatal(err)
	}
	if _, err := answer(t, s, "datamachine", Request{Cmd: OpenThreadForRoute, Channel: "C1", Thread: "C9/2.0", Text: "my own"}); err != nil || len(alerts) != 1 {
		t.Fatalf("the route's machine's own thread from a thread at the limit: %v, %d alerts; want it opened, no alert", err, len(alerts))
	}
}

// check-relay lets a client deliver in a thread whose owner it is, or
// whose owner the relay list lets it deliver to; a thread with no owner
// is not found.
func TestCheckRelay(t *testing.T) {
	s, _ := testServer(t)
	s.Relays = route.Relays{"workstation": {"datamachine"}}
	if err := s.Store.Claim(t.Context(), "C1/1.0", "datamachine", "", ""); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		client, thread string
		want           error
	}{
		{"workstation", "C1/1.0", nil},
		{"datamachine", "C1/1.0", nil},
		{"other", "C1/1.0", link.ErrDenied},
		{"workstation", "C1/2.0", link.ErrNotFound},
		{"workstation", "C1", link.ErrBadRequest},
	} {
		if _, err := answer(t, s, tt.client, Request{Cmd: CheckRelay, Thread: tt.thread}); !errors.Is(err, tt.want) {
			t.Errorf("check-relay by %s in %s = %v, want %v", tt.client, tt.thread, err, tt.want)
		}
	}
}
