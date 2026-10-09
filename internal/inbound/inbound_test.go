package inbound

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	slackgo "github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"

	"github.com/Luolc/fednet/internal/approval"
	"github.com/Luolc/fednet/internal/outbound"
	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/route"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
	"github.com/Luolc/fednet/internal/watch"
)

// The fake Slack's clock starts at this ts; tests set Now near it.
var epoch = time.Unix(1700000000, 0)

var users = map[string]string{"U1": "maintainer", "U2": ""}

// hey mentions the bot, as a message that hands its thread to an agent
// must.
const hey = "<@" + slack.FakeBot + "> "

func openHub(t *testing.T, path string) *store.Hub {
	t.Helper()
	h, err := store.OpenHub(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

// newReceiver returns a Receiver on a fresh hub database with C1 routed to
// the workstation, C2 to the data machine, direct messages to the
// workstation, and C3 to no machine.
func newReceiver(t *testing.T) (*Receiver, *slack.Fake) {
	t.Helper()
	f := &slack.Fake{}
	f.AddChannel("C1", "repo: fednet")
	f.AddChannel("C2", "the data channel")
	f.AddChannel("C3", "")
	f.AddIM("D1")
	r := &Receiver{
		Store: openHub(t, filepath.Join(t.TempDir(), "hub.db")),
		Slack: f,
		Route: route.Config{Defaults: map[string]string{"C1": "workstation", "C2": "datamachine"}, DM: "workstation"},
		Users: users,
		Bot:   slack.FakeBot,
		Now:   func() time.Time { return epoch.Add(time.Hour) },
	}
	return r, f
}

// queued returns the inbound messages queued for client, in order.
func queued(t *testing.T, h *store.Hub, client string) []payload.Message {
	t.Helper()
	ds, err := h.Outbox.After(t.Context(), client, 0)
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

// texts returns the texts of the inbound messages queued for client, the
// mention of the bot left off.
func texts(t *testing.T, h *store.Hub, client string) []string {
	t.Helper()
	var ts []string
	for _, m := range queued(t, h, client) {
		if m.Type != payload.Inbound {
			t.Fatalf("queued for %s: type %q, want %q", client, m.Type, payload.Inbound)
		}
		ts = append(ts, strings.TrimPrefix(m.Text, hey))
	}
	return ts
}

// all returns the texts queued for the workstation and the data machine.
func all(t *testing.T, h *store.Hub) []string {
	t.Helper()
	return append(texts(t, h, "workstation"), texts(t, h, "datamachine")...)
}

func handle(t *testing.T, r *Receiver, ev Event) {
	t.Helper()
	if err := r.Handle(t.Context(), ev); err != nil {
		t.Fatal(err)
	}
}

func connected(t *testing.T, r *Receiver) {
	t.Helper()
	if err := r.Connected(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func runBackfill(t *testing.T, r *Receiver) {
	t.Helper()
	if err := r.Backfill(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// message is a plain message event from U1.
func message(id, channel, ts, threadTS, text string) Event {
	return Event{ID: id, Channel: channel, Message: slack.Message{TS: ts, ThreadTS: threadTS, User: "U1", Text: text}}
}

func TestHandleRoutes(t *testing.T) {
	ctx := t.Context()
	r, _ := newReceiver(t)
	h := r.Store

	// A message in a channel that mentions nobody is not the agents'
	// business: it goes nowhere and its thread gets no owner. One that
	// mentions the bot hands its thread to the channel's default machine,
	// with the channel's purpose and the sender's name; a reply in it goes
	// to the owner, mention or not, without the purpose, even after the
	// default changed.
	handle(t, r, message("Ev0", "C2", "1.0", "", "just chatting"))
	if _, err := h.Owner(ctx, "C2/1.0"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Owner(C2/1.0) after a message that mentions nobody: err = %v, want ErrNotFound", err)
	}
	handle(t, r, message("Ev1", "C2", "1.1", "", hey+"first"))
	if owner, err := h.Owner(ctx, "C2/1.1"); err != nil || owner != "datamachine" {
		t.Fatalf("Owner(C2/1.1) = %q, %v; want datamachine", owner, err)
	}
	r.Route = route.Config{Defaults: map[string]string{"C2": "workstation"}, DM: "workstation"}
	handle(t, r, message("Ev2", "C2", "1.2", "1.1", "reply"))
	me := message("Ev3", "C2", "1.3", "1.1", "shrugs")
	me.SubType = "me_message"
	handle(t, r, me)
	handle(t, r, message("Ev4", "C2", "1.4", "1.1", hey+"again"))
	got := queued(t, h, "datamachine")
	want := []payload.Message{
		{Type: payload.Inbound, Thread: "C2/1.1", Text: hey + "first", User: "U1", UserName: "maintainer", TS: "1.1", Context: "the data channel", Trigger: payload.Mention},
		{Type: payload.Inbound, Thread: "C2/1.1", Text: "reply", User: "U1", UserName: "maintainer", TS: "1.2", Trigger: payload.Reply},
		{Type: payload.Inbound, Thread: "C2/1.1", Text: "shrugs", User: "U1", UserName: "maintainer", TS: "1.3", Trigger: payload.Reply},
		{Type: payload.Inbound, Thread: "C2/1.1", Text: hey + "again", User: "U1", UserName: "maintainer", TS: "1.4", Trigger: payload.Reply},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("queued for datamachine = %+v, want %+v", got, want)
	}

	// Every message in a direct message conversation goes to the DM
	// machine, mention or not: each that is not a reply starts a thread,
	// with no context; a reply follows the owner.
	dm := message("Ev5", "D1", "2.1", "", "psst")
	dm.IM = true
	handle(t, r, dm)
	dm = message("Ev6", "D1", "2.2", "", "again")
	dm.IM = true
	handle(t, r, dm)
	reply := message("Ev7", "D1", "2.3", "2.1", "more")
	reply.IM = true
	handle(t, r, reply)
	got = queued(t, h, "workstation")
	want = []payload.Message{
		{Type: payload.Inbound, Thread: "D1/2.1", Text: "psst", User: "U1", UserName: "maintainer", TS: "2.1", Trigger: payload.DM},
		{Type: payload.Inbound, Thread: "D1/2.2", Text: "again", User: "U1", UserName: "maintainer", TS: "2.2", Trigger: payload.DM},
		{Type: payload.Inbound, Thread: "D1/2.1", Text: "more", User: "U1", UserName: "maintainer", TS: "2.3", Trigger: payload.DM},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("queued for workstation = %+v, want %+v", got, want)
	}

	// A reply that mentions nobody in a thread with no owner goes nowhere,
	// whether or not Slack knows the thread.
	handle(t, r, message("Ev8", "C2", "1.5", "1.0", "still chatting"))
	handle(t, r, message("Ev9", "C2", "3.2", "3.1", "orphan"))
	if n := len(all(t, h)); n != 7 {
		t.Fatalf("%d queued after replies in threads with no owner, want 7", n)
	}
}

// A mention in a thread that has no owner hands the thread over with the
// messages before it, the thread's first message included, and the
// purpose; later replies carry no history. A machine's own reply is
// named by its machine and has no user.
func TestHandleMentionInThread(t *testing.T) {
	ctx := t.Context()
	r, f := newReceiver(t)
	root := post(t, f, "C1", slack.Message{User: "U1", Text: "CI is red again"})
	handle(t, r, root)
	post(t, f, "C1", slack.Message{User: "U2", Text: "which job?", ThreadTS: root.TS})
	if _, err := f.PostReply(ctx, "C1", root.TS, "datamachine", "not me"); err != nil {
		t.Fatal(err)
	}
	withFile := post(t, f, "C1", slack.Message{User: "U1", Text: "this one", ThreadTS: root.TS, SubType: "file_share", Files: []slack.File{{Name: "ci.png", Mimetype: "image/png", Size: 183204, URL: "https://example.invalid/ci.png"}}})
	handle(t, r, withFile)
	if _, err := r.Store.Owner(ctx, "C1/"+root.TS); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Owner before any mention: err = %v, want ErrNotFound", err)
	}
	ask := post(t, f, "C1", slack.Message{User: "U1", Text: hey + "have a look", ThreadTS: root.TS})
	handle(t, r, ask)
	later := post(t, f, "C1", slack.Message{User: "U2", Text: "thanks", ThreadTS: root.TS})
	handle(t, r, later)
	got := queued(t, r.Store, "workstation")
	thread := "C1/" + root.TS
	want := []payload.Message{
		{Type: payload.Inbound, Thread: thread, Text: hey + "have a look", User: "U1", UserName: "maintainer", TS: ask.TS, Context: "repo: fednet", Trigger: payload.Mention, History: &payload.History{
			Total: 4, Included: 4, Omitted: 0,
			Messages: []payload.HistoryMessage{
				{TS: root.TS, User: "U1", Name: "maintainer", Text: "CI is red again", Files: []payload.File{}},
				{TS: "1700000000.100002", User: "U2", Name: "", Text: "which job?", Files: []payload.File{}},
				{TS: "1700000000.100003", User: "", Name: "fednet (datamachine)", Text: "not me", Files: []payload.File{}},
				{TS: withFile.TS, User: "U1", Name: "maintainer", Text: "this one", Files: []payload.File{{Name: "ci.png", Mimetype: "image/png", Size: 183204, URL: "https://example.invalid/ci.png"}}},
			},
			ReadMore: "fednet client read-thread -socket <socket> " + thread,
		}},
		{Type: payload.Inbound, Thread: thread, Text: "thanks", User: "U2", TS: later.TS, Trigger: payload.Reply},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("queued for workstation = %s, want %s", pretty(t, got), pretty(t, want))
	}
	// Sent down, every message of the history has its fields, files and
	// truncated included, even when empty; the reply has no history.
	ds, err := r.Store.Outbox.After(ctx, "workstation", 0)
	if err != nil {
		t.Fatal(err)
	}
	type sentHistory struct {
		History *struct {
			Messages []map[string]json.RawMessage `json:"messages"`
		} `json:"history"`
	}
	var sent []sentHistory
	for _, d := range ds {
		var m sentHistory
		if err := json.Unmarshal(d.Payload, &m); err != nil {
			t.Fatal(err)
		}
		sent = append(sent, m)
	}
	if sent[0].History == nil || sent[1].History != nil {
		t.Fatalf("histories sent = %+v, want one on the mention only", sent)
	}
	for i, m := range sent[0].History.Messages {
		for _, key := range []string{"ts", "user", "name", "text", "truncated", "files"} {
			if _, ok := m[key]; !ok {
				t.Errorf("history message %d lacks %q: %v", i, key, m)
			}
		}
	}
}

// pretty writes v as indented JSON.
func pretty(t *testing.T, v any) string {
	t.Helper()
	b, err := json.MarshalIndent(v, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// countingReplies is a Slack that counts the calls to Replies.
type countingReplies struct {
	slack.API
	calls int
}

func (c *countingReplies) Replies(ctx context.Context, channel, ts string) ([]slack.Message, error) {
	c.calls++
	return c.API.Replies(ctx, channel, ts)
}

// A mention on a thread's first message carries no history, there being
// nothing before it, and does not read the thread.
func TestHandleMentionOnRootReadsNoThread(t *testing.T) {
	r, f := newReceiver(t)
	c := &countingReplies{API: f}
	r.Slack = c
	root := post(t, f, "C1", slack.Message{User: "U1", Text: hey + "first"})
	handle(t, r, root)
	got := queued(t, r.Store, "workstation")
	if len(got) != 1 || got[0].History != nil || got[0].Trigger != payload.Mention {
		t.Fatalf("queued = %+v, want one mention without history", got)
	}
	if c.calls != 0 {
		t.Fatalf("Replies was called %d times for a mention on a first message, want 0", c.calls)
	}
}

// noReplies is a Slack whose Replies fails.
type noReplies struct{ slack.API }

func (noReplies) Replies(context.Context, string, string) ([]slack.Message, error) {
	return nil, errors.New("flaky")
}

// When the thread cannot be read, the mention goes anyway, without a
// history and with its text as it is.
func TestHandleMentionWithoutHistory(t *testing.T) {
	r, f := newReceiver(t)
	root := post(t, f, "C1", slack.Message{User: "U1", Text: "chatter"})
	handle(t, r, root)
	r.Slack = noReplies{f}
	ask := post(t, f, "C1", slack.Message{User: "U1", Text: hey + "look", ThreadTS: root.TS})
	handle(t, r, ask)
	got := queued(t, r.Store, "workstation")
	want := []payload.Message{{Type: payload.Inbound, Thread: "C1/" + root.TS, Text: hey + "look", User: "U1", UserName: "maintainer", TS: ask.TS, Context: "repo: fednet", Trigger: payload.Mention}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("queued when the thread cannot be read = %+v, want %+v", got, want)
	}
	if owner, err := r.Store.Owner(t.Context(), "C1/"+root.TS); err != nil || owner != "workstation" {
		t.Fatalf("Owner = %q, %v; want workstation", owner, err)
	}
}

// The history holds the latest messages within the limits: at most
// MaxMessages, their texts adding up to at most MaxChars, the latest one
// always, each text cut to MaxMessageChars; the trigger's text is cut the
// same way. Characters, not bytes, are counted.
func TestHistoryLimits(t *testing.T) {
	r, f := newReceiver(t)
	r.History = Limits{MaxMessages: 3, MaxChars: 10, MaxMessageChars: 4}
	root := post(t, f, "C1", slack.Message{User: "U1", Text: "ab"})
	handle(t, r, root)
	for _, text := range []string{"cd", "二三四五六", "gh", "ij"} {
		post(t, f, "C1", slack.Message{User: "U2", Text: text, ThreadTS: root.TS})
	}
	ask := post(t, f, "C1", slack.Message{User: "U1", Text: hey + "一二三四五", ThreadTS: root.TS})
	handle(t, r, ask)
	got := queued(t, r.Store, "workstation")
	if len(got) != 1 || got[0].History == nil {
		t.Fatalf("queued = %+v, want one mention with a history", got)
	}
	if got[0].Text != hey[:4]+TruncatedMark {
		t.Fatalf("the trigger's text = %q, want it cut to 4 characters", got[0].Text)
	}
	h := got[0].History
	var texts []string
	for _, m := range h.Messages {
		texts = append(texts, m.Text)
	}
	// Of the 5 before, at most 3: "ij", "gh" fit (4 characters); the
	// cut "二三四五六" would take the total to 4+4+len(mark), over 10.
	if h.Total != 5 || h.Included != 2 || h.Omitted != 3 || !slices.Equal(texts, []string{"gh", "ij"}) {
		t.Fatalf("history = %s, want the latest 2 of 5", pretty(t, h))
	}
	// With room for the characters but not for more messages, the count
	// is what stops.
	r.History = Limits{MaxMessages: 2, MaxChars: 1000, MaxMessageChars: 1000}
	root = post(t, f, "C1", slack.Message{User: "U1", Text: "one"})
	handle(t, r, root)
	post(t, f, "C1", slack.Message{User: "U2", Text: "two", ThreadTS: root.TS})
	post(t, f, "C1", slack.Message{User: "U2", Text: "three", ThreadTS: root.TS})
	ask = post(t, f, "C1", slack.Message{User: "U1", Text: hey + "count", ThreadTS: root.TS})
	handle(t, r, ask)
	got = queued(t, r.Store, "workstation")
	if len(got) != 2 || got[1].History == nil {
		t.Fatalf("queued = %+v, want a second mention with a history", got)
	}
	h = got[1].History
	texts = nil
	for _, m := range h.Messages {
		texts = append(texts, m.Text)
	}
	if h.Total != 3 || h.Included != 2 || h.Omitted != 1 || !slices.Equal(texts, []string{"two", "three"}) {
		t.Fatalf("history under the message limit = %s, want the latest 2 of 3", pretty(t, h))
	}

	// With room for more messages but not for the characters, the latest
	// one is included anyway, cut.
	r.History = Limits{MaxMessages: 3, MaxChars: 1, MaxMessageChars: 4}
	root = post(t, f, "C1", slack.Message{User: "U1", Text: "ab"})
	handle(t, r, root)
	post(t, f, "C1", slack.Message{User: "U2", Text: "二三四五六", ThreadTS: root.TS})
	ask = post(t, f, "C1", slack.Message{User: "U1", Text: hey + "once more", ThreadTS: root.TS})
	handle(t, r, ask)
	got = queued(t, r.Store, "workstation")
	if len(got) != 3 || got[2].History == nil {
		t.Fatalf("queued = %+v, want a third mention with a history", got)
	}
	h = got[2].History
	if h.Total != 2 || h.Included != 1 || len(h.Messages) != 1 || h.Messages[0].Text != "二三四五"+TruncatedMark || !h.Messages[0].Truncated {
		t.Fatalf("history over the character limit = %s, want the latest message alone, cut", pretty(t, h))
	}
}

// Replies in a thread an agent opened go to it without a mention, as
// replies, with no history.
func TestHandleRepliesInAgentThread(t *testing.T) {
	ctx := t.Context()
	r, f := newReceiver(t)
	ts, err := f.Post(ctx, "C2", "I opened this")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Store.Claim(ctx, "C2/"+ts, "workstation", ""); err != nil {
		t.Fatal(err)
	}
	reply := post(t, f, "C2", slack.Message{User: "U1", Text: "noted", ThreadTS: ts})
	handle(t, r, reply)
	got := queued(t, r.Store, "workstation")
	want := []payload.Message{{Type: payload.Inbound, Thread: "C2/" + ts, Text: "noted", User: "U1", UserName: "maintainer", TS: reply.TS, Trigger: payload.Reply}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("queued for the agent's thread = %+v, want %+v", got, want)
	}
	if n := len(queued(t, r.Store, "datamachine")); n != 0 {
		t.Fatalf("%d queued for the channel's default machine, want 0", n)
	}
}

// noChannelInfo is a Slack whose ChannelInfo fails.
type noChannelInfo struct{ slack.API }

func (noChannelInfo) ChannelInfo(context.Context, string) (slack.ChannelInfo, error) {
	return slack.ChannelInfo{}, errors.New("flaky")
}

func TestHandleWithoutChannelInfo(t *testing.T) {
	r, f := newReceiver(t)
	f.RenameChannel("C1", "repo-fednet")
	r.Slack = noChannelInfo{f}
	handle(t, r, message("Ev1", "C1", "1.1", "", hey+"first"))
	got := queued(t, r.Store, "workstation")
	want := []payload.Message{{Type: payload.Inbound, Thread: "C1/1.1", Text: hey + "first", User: "U1", UserName: "maintainer", TS: "1.1", Trigger: payload.Mention}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("queued when the channel's name and purpose cannot be read = %+v, want %+v", got, want)
	}
}

// The message that hands a thread over carries the channel's name as
// Slack has it then, and every reply in the thread carries the same name,
// even after the channel is renamed; a thread owned with no name recorded
// and a direct message carry none.
func TestHandleChannelName(t *testing.T) {
	r, f := newReceiver(t)
	f.RenameChannel("C1", "repo-fednet")
	handle(t, r, message("Ev1", "C1", "1.1", "", hey+"first"))
	f.RenameChannel("C1", "repo-renamed")
	handle(t, r, message("Ev2", "C1", "1.2", "1.1", "reply"))
	handle(t, r, message("Ev3", "C1", "1.3", "", hey+"second"))
	if err := r.Store.Claim(t.Context(), "C1/1.4", "workstation", ""); err != nil {
		t.Fatal(err)
	}
	handle(t, r, message("Ev4", "C1", "1.5", "1.4", "in a thread with no name"))
	// Slack gives a direct message no name; one is set here so that a
	// direct message carrying it would show.
	f.RenameChannel("D1", "a-dm")
	dm := message("Ev5", "D1", "2.1", "", "psst")
	dm.IM = true
	handle(t, r, dm)
	var got []string
	for _, m := range queued(t, r.Store, "workstation") {
		got = append(got, m.Trigger+":"+m.ChannelName)
	}
	if want := []string{"mention:repo-fednet", "reply:repo-fednet", "mention:repo-renamed", "reply:", "dm:"}; !slices.Equal(got, want) {
		t.Fatalf("triggers and channel names = %q, want %q", got, want)
	}
}

func TestHandleFiles(t *testing.T) {
	r, _ := newReceiver(t)
	ev := message("Ev1", "C1", "1.1", "", hey+"see these")
	ev.SubType = "file_share"
	ev.Files = []slack.File{{Name: "a.txt", Mimetype: "text/plain", Size: 12, URL: "https://example.invalid/a"}, {Name: "b c.png", URL: "https://example.invalid/b"}}
	handle(t, r, ev)
	ev = message("Ev2", "C1", "1.2", "1.1", "")
	ev.Files = []slack.File{{Name: "d.log", URL: "https://example.invalid/d"}}
	handle(t, r, ev)
	want := []string{
		"see these\nfile: a.txt https://example.invalid/a\nfile: b c.png https://example.invalid/b",
		"file: d.log https://example.invalid/d",
	}
	if got := texts(t, r.Store, "workstation"); !slices.Equal(got, want) {
		t.Fatalf("texts = %q, want %q", got, want)
	}
	// The files' metadata goes along too.
	got := queued(t, r.Store, "workstation")
	wantFiles := [][]payload.File{
		{{Name: "a.txt", Mimetype: "text/plain", Size: 12, URL: "https://example.invalid/a"}, {Name: "b c.png", URL: "https://example.invalid/b"}},
		{{Name: "d.log", URL: "https://example.invalid/d"}},
	}
	for i, m := range got {
		if !reflect.DeepEqual(m.Files, wantFiles[i]) {
			t.Fatalf("files of message %d = %+v, want %+v", i, m.Files, wantFiles[i])
		}
	}
}

func TestHandleDedupsEvents(t *testing.T) {
	r, _ := newReceiver(t)
	// Slack redelivers an event it got no ack for; the same message may
	// also come under another event id, and later from history.
	handle(t, r, message("Ev1", "C1", "1.1", "", hey+"first"))
	handle(t, r, message("Ev1", "C1", "1.1", "", hey+"first"))
	handle(t, r, message("Ev2", "C1", "1.1", "", hey+"first"))
	handle(t, r, message("", "C1", "1.1", "", hey+"first"))
	handle(t, r, message("Ev3", "C1", "1.2", "1.1", "reply"))
	handle(t, r, message("Ev3", "C1", "1.2", "1.1", "reply"))
	if got := texts(t, r.Store, "workstation"); !slices.Equal(got, []string{"first", "reply"}) {
		t.Fatalf("texts = %q, want [first reply]", got)
	}
}

// Two hubs on the same database, as during a handoff, both get the same
// event: it is queued once. Each has its own connection and its own
// memory; only the database is shared.
func TestHandleDedupsAcrossInstances(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hub.db")
	r1, _ := newReceiver(t)
	r1.Store = openHub(t, path)
	r2, _ := newReceiver(t)
	r2.Store = openHub(t, path)
	handle(t, r1, message("Ev1", "C1", "1.1", "", hey+"first"))
	handle(t, r2, message("Ev1", "C1", "1.1", "", hey+"first"))
	handle(t, r2, message("Ev2", "C1", "1.2", "1.1", "reply"))
	handle(t, r1, message("Ev2", "C1", "1.2", "1.1", "reply"))
	for i, r := range []*Receiver{r1, r2} {
		if got := texts(t, r.Store, "workstation"); !slices.Equal(got, []string{"first", "reply"}) {
			t.Fatalf("texts seen by hub %d = %q, want [first reply]", i+1, got)
		}
	}
}

// A message is recorded, and queued, before Handle returns, so that the
// event is acked only after that; when the record fails, Handle fails, and
// nothing is queued.
func TestHandleFailsWhenStoreFails(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "hub.db")
	r, _ := newReceiver(t)
	r.Store = openHub(t, path)
	handle(t, r, message("Ev1", "C1", "1.1", "", hey+"first"))
	r.Store.Close()
	if err := r.Handle(ctx, message("Ev2", "C1", "1.2", "1.1", "reply")); err == nil {
		t.Fatal("Handle with the database closed returned no error")
	}
	r.Store = openHub(t, path)
	if got := texts(t, r.Store, "workstation"); !slices.Equal(got, []string{"first"}) {
		t.Fatalf("texts after a failed Handle = %q, want [first]", got)
	}
	// The same event, not acked, comes again and is taken in.
	handle(t, r, message("Ev2", "C1", "1.2", "1.1", "reply"))
	if got := texts(t, r.Store, "workstation"); !slices.Equal(got, []string{"first", "reply"}) {
		t.Fatalf("texts after the event came again = %q, want [first reply]", got)
	}
}

func TestHandleFilters(t *testing.T) {
	r, _ := newReceiver(t)
	handle(t, r, message("Ev0", "C1", "1.0", "", hey+"first"))
	for _, tt := range []struct {
		name string
		edit func(ev *Event)
	}{
		{"not on the user list", func(ev *Event) { ev.User = "U9" }},
		{"a bot, this one included", func(ev *Event) { ev.BotID = "B1" }},
		{"an edit", func(ev *Event) { ev.SubType = "message_changed" }},
		{"a deletion", func(ev *Event) { ev.SubType = "message_deleted" }},
		{"someone joining", func(ev *Event) { ev.SubType = "channel_join" }},
		{"a topic change", func(ev *Event) { ev.SubType = "channel_topic" }},
	} {
		ev := message("Ev"+tt.name, "C1", "1.1", "", hey+tt.name)
		tt.edit(&ev)
		handle(t, r, ev)
		ev = message("Ev"+tt.name+" reply", "C1", "1.2", "1.0", tt.name)
		tt.edit(&ev)
		handle(t, r, ev)
	}
	if got := texts(t, r.Store, "workstation"); !slices.Equal(got, []string{"first"}) {
		t.Fatalf("texts = %q, want only [first]", got)
	}
	// A filtered message is not recorded, so U1 can still post with its ts.
	handle(t, r, message("Ev1", "C1", "1.1", "", hey+"second"))
	if got := texts(t, r.Store, "workstation"); !slices.Equal(got, []string{"first", "second"}) {
		t.Fatalf("texts = %q, want [first second]", got)
	}
}

// posts returns the undelivered posts in the hub's inbox, as client/thread/text.
func posts(t *testing.T, h *store.Hub) []string {
	t.Helper()
	us, err := h.Inbox.UndeliveredFrom(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var ps []string
	for _, u := range us {
		var m payload.Message
		if err := json.Unmarshal(u.Payload, &m); err != nil {
			t.Fatal(err)
		}
		ps = append(ps, u.Client+"/"+m.Thread+"/"+m.Text)
	}
	return ps
}

func TestHandleNoMachine(t *testing.T) {
	ctx := t.Context()
	r, f := newReceiver(t)
	ts, err := f.Start("C3", "U1", hey+"anyone?")
	if err != nil {
		t.Fatal(err)
	}
	// A mention in a channel no machine takes is recorded, once, but not
	// answered: the hub's inbox stays empty and so does the thread.
	ev := message("Ev1", "C3", ts, "", hey+"anyone?")
	handle(t, r, ev)
	handle(t, r, ev)
	if got := posts(t, r.Store); len(got) != 0 {
		t.Fatalf("hub inbox after a mention no machine takes = %q, want nothing", got)
	}
	if err := (&outbound.Poster{Store: r.Store, Slack: f}).Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if ms, err := f.Replies(ctx, "C3", ts); err != nil || len(ms) != 1 {
		t.Fatalf("thread after a mention no machine takes = %+v, %v; want only the mention", ms, err)
	}
	if _, err := r.Store.Owner(ctx, "C3/"+ts); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Owner: err = %v, want ErrNotFound", err)
	}
	// A reply in that thread is dropped quietly.
	handle(t, r, message("Ev2", "C3", "9.9", ts, "still there?"))
	if got := all(t, r.Store); len(got) != 0 {
		t.Fatalf("queued = %q, want nothing", got)
	}
	// A new direct message with no DM machine is told so, by a post in
	// the hub's inbox, once, that the outbound side delivers like any
	// other post, under the hub's name.
	r.Route.DM = ""
	dm := message("Ev3", "D1", "5.1", "", "psst")
	dm.IM = true
	handle(t, r, dm)
	handle(t, r, dm)
	if got := posts(t, r.Store); !slices.Equal(got, []string{HubName + "/D1/5.1/" + NoMachineText}) {
		t.Fatalf("hub inbox after a DM no machine takes = %q", got)
	}
}

// post adds a message from user to the fake and returns the Event that
// Slack would have delivered for it.
func post(t *testing.T, f *slack.Fake, channel string, m slack.Message) Event {
	t.Helper()
	ts, err := f.Add(channel, m)
	if err != nil {
		t.Fatal(err)
	}
	m.TS = ts
	return Event{ID: "Ev" + ts, Channel: channel, IM: strings.HasPrefix(channel, "D"), Message: m}
}

func TestBackfill(t *testing.T) {
	r, f := newReceiver(t)
	h := r.Store

	// Nothing is read before the hub has seen a message.
	for _, ch := range []string{"C1", "C2", "D1"} {
		post(t, f, ch, slack.Message{User: "U1", Text: "before fednet"})
	}
	connected(t, r)
	runBackfill(t, r)
	if n := len(all(t, h)); n != 0 {
		t.Fatalf("%d queued by a backfill before any message was seen, want 0", n)
	}

	// Live: a thread in C1, a thread in C2, a DM thread.
	c1 := post(t, f, "C1", slack.Message{User: "U1", Text: hey + "c1 first"})
	handle(t, r, c1)
	c2 := post(t, f, "C2", slack.Message{User: "U1", Text: hey + "c2 first"})
	handle(t, r, c2)
	d1 := post(t, f, "D1", slack.Message{User: "U1", Text: "dm first"})
	handle(t, r, d1)
	// The last live message, then the connection drops: in the gap, a
	// reply in each thread, a new thread in C2, a new DM, a bot message
	// and a message from someone else, which are not for the agents.
	handle(t, r, post(t, f, "C1", slack.Message{User: "U1", Text: "c1 last live", ThreadTS: c1.TS}))
	r.Disconnected()
	gap := post(t, f, "C1", slack.Message{User: "U1", Text: "c1 gap reply", ThreadTS: c1.TS})
	post(t, f, "C2", slack.Message{User: "U2", Text: "c2 gap reply", ThreadTS: c2.TS})
	post(t, f, "C2", slack.Message{User: "U1", Text: hey + "c2 gap thread"})
	post(t, f, "D1", slack.Message{User: "U1", Text: "dm gap reply", ThreadTS: d1.TS})
	post(t, f, "D1", slack.Message{User: "U1", Text: "dm gap thread"})
	post(t, f, "C1", slack.Message{User: "U1", Text: hey + "bot", BotID: "B1"})
	post(t, f, "C1", slack.Message{User: "U9", Text: hey + "stranger"})
	// Reconnected: Slack redelivers the event in flight when the
	// connection dropped, and a live message arrives before the backfill
	// gets to run.
	connected(t, r)
	gap.ID = "Ev-redelivered"
	handle(t, r, gap)
	handle(t, r, post(t, f, "C1", slack.Message{User: "U1", Text: "c1 live before backfill", ThreadTS: c1.TS}))
	runBackfill(t, r)
	handle(t, r, post(t, f, "C1", slack.Message{User: "U1", Text: "c1 after", ThreadTS: c1.TS}))

	if got, want := texts(t, h, "workstation"), []string{"c1 first", "dm first", "c1 last live", "c1 gap reply", "c1 live before backfill", "dm gap thread", "dm gap reply", "c1 after"}; !slices.Equal(got, want) {
		t.Fatalf("texts for workstation = %q, want %q", got, want)
	}
	if got, want := texts(t, h, "datamachine"), []string{"c2 first", "c2 gap thread", "c2 gap reply"}; !slices.Equal(got, want) {
		t.Fatalf("texts for datamachine = %q, want %q", got, want)
	}
	// A second connection, with nothing new, delivers nothing again.
	r.Disconnected()
	connected(t, r)
	runBackfill(t, r)
	if n := len(all(t, h)); n != 11 {
		t.Fatalf("%d queued after a second backfill, want 11", n)
	}
}

// A reply can arrive before the message that started its thread: both were
// in flight at a disconnection, and Slack redelivers them in any order.
// The hub reads the thread's first message from Slack and takes it in
// first; a reply in a thread too old for a backfill still goes nowhere.
func TestHandleReplyBeforeRoot(t *testing.T) {
	r, f := newReceiver(t)
	handle(t, r, post(t, f, "C1", slack.Message{User: "U1", Text: hey + "first"}))
	r.Disconnected()
	root := post(t, f, "C1", slack.Message{User: "U1", Text: hey + "gap root"})
	reply := post(t, f, "C1", slack.Message{User: "U1", Text: "gap reply", ThreadTS: root.TS})
	connected(t, r)
	handle(t, r, reply)
	handle(t, r, root)
	runBackfill(t, r)
	if got, want := texts(t, r.Store, "workstation"), []string{"first", "gap root", "gap reply"}; !slices.Equal(got, want) {
		t.Fatalf("texts = %q, want %q", got, want)
	}
	if m := queued(t, r.Store, "workstation")[1]; m.Context != "repo: fednet" {
		t.Fatalf("the root taken in before its reply has context %q, want the channel's purpose", m.Context)
	}
	old := post(t, f, "C1", slack.Message{TS: slackTS(epoch.Add(-2 * DefaultWindow)), User: "U1", Text: hey + "long ago"})
	handle(t, r, post(t, f, "C1", slack.Message{User: "U1", Text: "late reply", ThreadTS: old.TS}))
	if got := texts(t, r.Store, "workstation"); len(got) != 3 {
		t.Fatalf("texts after a reply in an old thread = %q, want the 3 before", got)
	}
}

func TestBackfillWindow(t *testing.T) {
	r, f := newReceiver(t)
	r.Window = time.Hour
	// Seen a message two hours ago, then nothing live.
	old := post(t, f, "C1", slack.Message{TS: slackTS(epoch.Add(-2 * time.Hour)), User: "U1", Text: hey + "seen"})
	handle(t, r, old)
	post(t, f, "C1", slack.Message{TS: slackTS(epoch.Add(-90 * time.Minute)), User: "U1", Text: "too old", ThreadTS: old.TS})
	post(t, f, "C1", slack.Message{TS: slackTS(epoch.Add(-80 * time.Minute)), User: "U1", Text: hey + "too old too"})
	post(t, f, "C1", slack.Message{TS: slackTS(epoch.Add(-30 * time.Minute)), User: "U1", Text: "in the window", ThreadTS: old.TS})
	post(t, f, "C1", slack.Message{TS: slackTS(epoch.Add(-20 * time.Minute)), User: "U1", Text: hey + "in the window too"})
	r.Now = func() time.Time { return epoch }
	connected(t, r)
	runBackfill(t, r)
	if got, want := texts(t, r.Store, "workstation"), []string{"seen", "in the window too", "in the window"}; !slices.Equal(got, want) {
		t.Fatalf("texts = %q, want %q", got, want)
	}
}

func TestBackfillAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hub.db")
	r, f := newReceiver(t)
	r.Store = openHub(t, path)
	connected(t, r)
	first := post(t, f, "C1", slack.Message{User: "U1", Text: hey + "first"})
	handle(t, r, first)
	// The hub dies without a word; messages arrive; it comes back up on
	// the same database and a live message gets in before the backfill.
	r.Store.Close()
	post(t, f, "C1", slack.Message{User: "U1", Text: "while down", ThreadTS: first.TS})
	post(t, f, "C1", slack.Message{User: "U1", Text: hey + "also while down"})
	r = &Receiver{Store: openHub(t, path), Slack: f, Route: r.Route, Users: r.Users, Bot: r.Bot, Now: r.Now}
	connected(t, r)
	handle(t, r, post(t, f, "C1", slack.Message{User: "U1", Text: "live after restart", ThreadTS: first.TS}))
	runBackfill(t, r)
	if got, want := texts(t, r.Store, "workstation"), []string{"first", "live after restart", "also while down", "while down"}; !slices.Equal(got, want) {
		t.Fatalf("texts after restart = %q, want %q", got, want)
	}
}

// flaky is a Slack whose Conversations fails the first n times.
type flaky struct {
	slack.API
	fails int
}

func (f *flaky) Conversations(ctx context.Context) ([]slack.Conversation, error) {
	if f.fails > 0 {
		f.fails--
		return nil, errors.New("flaky")
	}
	return f.API.Conversations(ctx)
}

// A backfill that fails starts from the same place when run again, and
// after a reconnection, even though live messages have moved the latest
// seen on meanwhile.
func TestBackfillRetriesFromWhereItFailed(t *testing.T) {
	ctx := t.Context()
	r, f := newReceiver(t)
	fl := &flaky{API: f, fails: 1}
	r.Slack = fl
	first := post(t, f, "C1", slack.Message{User: "U1", Text: hey + "first"})
	handle(t, r, first)
	r.Disconnected()
	post(t, f, "C1", slack.Message{User: "U1", Text: "in the gap", ThreadTS: first.TS})
	connected(t, r)
	if err := r.Backfill(ctx); err == nil {
		t.Fatal("the first backfill did not fail")
	}
	handle(t, r, post(t, f, "C1", slack.Message{User: "U1", Text: "live after the gap", ThreadTS: first.TS}))
	r.Disconnected()
	connected(t, r)
	// Run keeps trying until the backfill succeeds.
	backfillUntilDone(ctx, r, time.Millisecond)
	if got, want := texts(t, r.Store, "workstation"), []string{"first", "live after the gap", "in the gap"}; !slices.Equal(got, want) {
		t.Fatalf("texts = %q, want %q", got, want)
	}
	if from, _ := r.Store.SlackState(ctx, store.BackfillFrom); from != "" {
		t.Fatalf("BackfillFrom after a finished backfill = %q, want empty", from)
	}
}

// gatedHistory is a Slack whose History of C1 waits, once entered, until
// released or its context is done.
type gatedHistory struct {
	slack.API
	entered, release chan struct{}
}

func (g *gatedHistory) History(ctx context.Context, channel, from string) ([]slack.Message, error) {
	ms, err := g.API.History(ctx, channel, from)
	if channel == "C1" {
		close(g.entered)
		select {
		case <-g.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return ms, err
}

// A backfill still running when the connection drops and comes back must
// not clear the start the new connection fixed, whether it then finishes
// or is cancelled: the new connection's backfill reads the gap.
func TestBackfillOfOldConnectionKeepsNewStart(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "old backfill finishes", true: "old backfill is cancelled"}[cancelled], func(t *testing.T) {
			ctx := t.Context()
			r, f := newReceiver(t)
			handle(t, r, post(t, f, "C1", slack.Message{User: "U1", Text: hey + "first"}))
			connected(t, r)
			gate := &gatedHistory{API: f, entered: make(chan struct{}), release: make(chan struct{})}
			r.Slack = gate
			octx, cancel := context.WithCancel(ctx)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- r.Backfill(octx) }()
			select {
			case <-gate.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("the old backfill did not get to C1's history")
			}
			// The old backfill has read C1; the connection drops, a reply
			// is posted, the connection comes back and fixes its start.
			r.Disconnected()
			post(t, f, "C1", slack.Message{User: "U1", Text: hey + "gap"})
			connected(t, r)
			if cancelled {
				cancel()
			} else {
				close(gate.release)
			}
			if err := <-done; (err == nil) == cancelled {
				t.Fatalf("the old backfill returned %v, cancelled %v", err, cancelled)
			}
			r.Slack = f
			handle(t, r, post(t, f, "C2", slack.Message{User: "U1", Text: hey + "new live"}))
			runBackfill(t, r)
			if got, want := all(t, r.Store), []string{"first", "gap", "new live"}; !slices.Equal(got, want) {
				t.Fatalf("queued = %q, want %q", got, want)
			}
			if from, _ := r.Store.SlackState(ctx, store.BackfillFrom); from != "" {
				t.Fatalf("BackfillFrom after the new backfill = %q, want empty", from)
			}
		})
	}
}

// The same across processes: the old hub's backfill finishes after the new
// hub's connection has fixed its start. The old backfill cannot clear or
// move it, since the store, not the old hub's memory, decides; the new
// hub's backfill takes in what came during the gap.
func TestBackfillOfOldProcessKeepsNewStart(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "hub.db")
	old, f := newReceiver(t)
	old.Store = openHub(t, path)
	handle(t, old, post(t, f, "C1", slack.Message{User: "U1", Text: hey + "first"}))
	connected(t, old)
	gate := &gatedHistory{API: f, entered: make(chan struct{}), release: make(chan struct{})}
	old.Slack = gate
	done := make(chan error, 1)
	go func() { done <- old.Backfill(ctx) }()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the old backfill did not get to C1's history")
	}
	// The new process comes up and fixes its start; a message is posted
	// that the old backfill has already read past.
	fresh := &Receiver{Store: openHub(t, path), Slack: f, Route: old.Route, Users: old.Users, Bot: old.Bot, Now: old.Now}
	connected(t, fresh)
	post(t, f, "C1", slack.Message{User: "U1", Text: hey + "gap"})
	close(gate.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if from, _ := fresh.Store.SlackState(ctx, store.BackfillFrom); from == "" {
		t.Fatal("the old process's backfill cleared the new process's start")
	}
	runBackfill(t, fresh)
	if got, want := all(t, fresh.Store), []string{"first", "gap"}; !slices.Equal(got, want) {
		t.Fatalf("queued = %q, want %q", got, want)
	}
	if from, _ := fresh.Store.SlackState(ctx, store.BackfillFrom); from != "" {
		t.Fatalf("BackfillFrom after the new backfill = %q, want empty", from)
	}
}

// The Receiver is the watch's view of the Slack connection.
var _ watch.SlackLink = (*Receiver)(nil)

func TestStatus(t *testing.T) {
	now := epoch
	r, _ := newReceiver(t)
	r.Now = func() time.Time { return now }
	if s := r.Status(); s.Connected || !s.Since.IsZero() || r.DownFor() != 0 {
		t.Fatalf("Status before any attempt = %+v, DownFor %v; want disconnected since zero, down for 0", s, r.DownFor())
	}
	r.Disconnected()
	now = now.Add(time.Second)
	r.Disconnected()
	if s := r.Status(); s.Connected || !s.Since.Equal(epoch) || r.DownFor() != time.Second {
		t.Fatalf("Status while connecting = %+v, DownFor %v; want disconnected since the first attempt, down for 1s", s, r.DownFor())
	}
	now = now.Add(time.Second)
	connected(t, r)
	now = now.Add(time.Second)
	connected(t, r)
	if s := r.Status(); !s.Connected || !s.Since.Equal(epoch.Add(2*time.Second)) || r.DownFor() != 0 {
		t.Fatalf("Status once connected = %+v, DownFor %v; want connected since the connection came up, down for 0", s, r.DownFor())
	}
	now = now.Add(time.Second)
	r.Disconnected()
	now = now.Add(3 * time.Second)
	if s := r.Status(); s.Connected || !s.Since.Equal(epoch.Add(4*time.Second)) || r.DownFor() != 3*time.Second {
		t.Fatalf("Status after a drop = %+v, DownFor %v; want disconnected since the drop, down for 3s", s, r.DownFor())
	}
}

func TestSlackTS(t *testing.T) {
	ts := slackTS(time.Unix(1700000000, 5000))
	if ts != "1700000000.000005" {
		t.Fatalf("slackTS = %q, want 1700000000.000005", ts)
	}
	if slack.CompareTS(ts, "1700000000.000004") <= 0 || slack.CompareTS(ts, "1700000000.000006") >= 0 {
		t.Fatalf("%q does not order between its neighbours", ts)
	}
}

// fakeTransport stands in for the Socket Mode client: the test sends
// events, and reads the acks.
type fakeTransport struct {
	events chan socketmode.Event
	acks   chan string
	ran    chan struct{}
	// payloads maps each acked envelope to the payload of its ack.
	mu       sync.Mutex
	payloads map[string]any
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{events: make(chan socketmode.Event), acks: make(chan string, 10), ran: make(chan struct{}), payloads: make(map[string]any)}
}

func (f *fakeTransport) RunContext(ctx context.Context) error {
	<-ctx.Done()
	close(f.ran)
	return ctx.Err()
}

func (f *fakeTransport) Events() <-chan socketmode.Event { return f.events }

func (f *fakeTransport) Ack(_ context.Context, envelopeID string, payload any) error {
	f.mu.Lock()
	f.payloads[envelopeID] = payload
	f.mu.Unlock()
	f.acks <- envelopeID
	return nil
}

// payload returns what the ack of envelopeID carried.
func (f *fakeTransport) payload(envelopeID string) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.payloads[envelopeID]
}

// send delivers ev to run and waits until run has taken it.
func (f *fakeTransport) send(t *testing.T, ev socketmode.Event) {
	t.Helper()
	select {
	case f.events <- ev:
	case <-time.After(5 * time.Second):
		t.Fatal("run did not take the event")
	}
}

// ack returns the next ack within d, or "".
func (f *fakeTransport) ack(d time.Duration) string {
	select {
	case id := <-f.acks:
		return id
	case <-time.After(d):
		return ""
	}
}

// messageEvent is a Socket Mode event carrying a message event, as slack-go
// parses it, in the envelope with id.
func messageEvent(t *testing.T, envelope, eventID, channel, ts, threadTS, text string, files bool) socketmode.Event {
	t.Helper()
	inner := `{"type":"message","channel":"` + channel + `","channel_type":"channel","user":"U1","ts":"` + ts + `","text":"` + text + `"`
	if threadTS != "" {
		inner += `,"thread_ts":"` + threadTS + `"`
	}
	if files {
		inner += `,"subtype":"file_share","files":[{"name":"a.txt","permalink":"https://example.invalid/a"}]`
	}
	inner += "}"
	e, err := slackevents.ParseEvent(json.RawMessage(`{"type":"event_callback","event_id":"`+eventID+`","event":`+inner+`}`), slackevents.OptionNoVerifyToken())
	if err != nil {
		t.Fatal(err)
	}
	return socketmode.Event{Type: socketmode.EventTypeEventsAPI, Data: e, Request: &socketmode.Request{Type: socketmode.RequestTypeEventsAPI, EnvelopeID: envelope}}
}

// waitFor polls cond up to a few seconds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 500; i++ {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("waited in vain for %s", what)
}

// Through the transport: an event is acked only once its message is
// recorded and queued, and not when the record fails; a redelivery is
// acked and not queued again; connecting marks the hub down, connected
// marks it up and starts a backfill that reads the gap; cancelling stops
// run.
func TestRunAcks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hub.db")
	r, f := newReceiver(t)
	r.Store = openHub(t, path)
	root := post(t, f, "C1", slack.Message{User: "U1", Text: hey + "first"})
	tr := newFakeTransport()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- run(ctx, tr, r, time.Millisecond) }()

	tr.send(t, socketmode.Event{Type: socketmode.EventTypeConnecting})
	waitFor(t, "the hub to be down while connecting", func() bool { s := r.Status(); return !s.Connected && !s.Since.IsZero() })
	tr.send(t, socketmode.Event{Type: socketmode.EventTypeConnected})
	waitFor(t, "the hub to be up after the connected event", func() bool { return r.Status().Connected })
	// A message: no ack before it is queued; acked once it is.
	tr.send(t, messageEvent(t, "env1", "Ev1", "C1", root.TS, "", hey+"first", true))
	if id := tr.ack(2 * time.Second); id != "env1" {
		t.Fatalf("ack = %q, want env1", id)
	}
	if got, want := texts(t, r.Store, "workstation"), []string{"first\nfile: a.txt https://example.invalid/a"}; !slices.Equal(got, want) {
		t.Fatalf("texts when the ack arrived = %q, want %q", got, want)
	}
	// Redelivered: acked, not queued again.
	tr.send(t, messageEvent(t, "env2", "Ev1", "C1", root.TS, "", hey+"first", true))
	if id := tr.ack(2 * time.Second); id != "env2" {
		t.Fatalf("ack of the redelivery = %q, want env2", id)
	}
	if n := len(texts(t, r.Store, "workstation")); n != 1 {
		t.Fatalf("%d queued after the redelivery, want 1", n)
	}
	// An event that is not a message is acked at once.
	tr.send(t, socketmode.Event{Type: socketmode.EventTypeEventsAPI, Data: slackevents.EventsAPIEvent{}, Request: &socketmode.Request{EnvelopeID: "env3"}})
	if id := tr.ack(2 * time.Second); id != "env3" {
		t.Fatalf("ack of a non-message event = %q, want env3", id)
	}
	// The record fails: no ack.
	r.Store.Close()
	tr.send(t, messageEvent(t, "env4", "Ev2", "C1", "1.2", root.TS, "reply", false))
	if id := tr.ack(200 * time.Millisecond); id != "" {
		t.Fatalf("acked %q although the record failed", id)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after cancel")
	}
	<-tr.ran

	// The hub comes back on the same database; a reply posted while it
	// was away is backfilled once the connection is up.
	r.Store = openHub(t, path)
	if n := len(texts(t, r.Store, "workstation")); n != 1 {
		t.Fatalf("%d queued after a failed record, want 1", n)
	}
	post(t, f, "C1", slack.Message{User: "U1", Text: "in the gap", ThreadTS: root.TS})
	tr = newFakeTransport()
	ctx, cancel = context.WithCancel(t.Context())
	go func() { done <- run(ctx, tr, r, time.Millisecond) }()
	tr.send(t, socketmode.Event{Type: socketmode.EventTypeConnecting})
	tr.send(t, socketmode.Event{Type: socketmode.EventTypeConnected})
	waitFor(t, "the gap to be backfilled", func() bool { return len(texts(t, r.Store, "workstation")) == 2 })
	if got, want := texts(t, r.Store, "workstation"), []string{"first\nfile: a.txt https://example.invalid/a", "in the gap"}; !slices.Equal(got, want) {
		t.Fatalf("texts after the backfill = %q, want %q", got, want)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run returned %v, want context.Canceled", err)
	}
}

// clickEvent is a Socket Mode event carrying a press on a card's button,
// as slack-go parses it, in the envelope with id.
func clickEvent(t *testing.T, envelope, actionID, approvalID, ts, user string, bot bool) socketmode.Event {
	t.Helper()
	var cb slackgo.InteractionCallback
	body := `{"type":"block_actions","user":{"id":"` + user + `","is_bot":` + strconv.FormatBool(bot) + `},"container":{"type":"message","message_ts":"` + ts + `","channel_id":"C9"},` +
		`"actions":[{"action_id":"` + actionID + `","block_id":"approval:` + approvalID + `","type":"button","value":"` + approvalID + `","action_ts":"1.6"}]}`
	if err := json.Unmarshal([]byte(body), &cb); err != nil {
		t.Fatal(err)
	}
	return socketmode.Event{Type: socketmode.EventTypeInteractive, Data: cb, Request: &socketmode.Request{Type: socketmode.RequestTypeInteractive, EnvelopeID: envelope}}
}

// slowUpdate is a Fake whose UpdateCard never returns.
type slowUpdate struct{ *slack.Fake }

func (slowUpdate) UpdateCard(ctx context.Context, _, _ string, _ slack.Card) error {
	<-ctx.Done()
	return ctx.Err()
}

// Through the transport: a click on a card is acked once the approval is
// decided and its outcome queued, without waiting for Slack to update the
// card; a click that does not count, or from a block that is not the
// card's, is acked too; without approvals a click is acked and dropped.
func TestRunAcksClicks(t *testing.T) {
	r, f := newReceiver(t)
	f.AddChannel("C9", "approvals")
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	// Slack takes its time updating cards; the acks must not wait for it.
	r.Approvals = &approval.Flow{Store: r.Store, Slack: slowUpdate{f}, Key: priv, Channel: "C9", Approvers: []string{"U1"}, Now: r.Now}
	id, err := r.Approvals.Request(t.Context(), "workstation", "ops-exec", "", "delete b", []byte(`{"b":1}`))
	if err != nil {
		t.Fatal(err)
	}
	tss, _ := f.Cards("C9")
	cardTS := tss[0]
	tr := newFakeTransport()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- run(ctx, tr, r, time.Millisecond) }()

	// Someone not on the approver list: acked, nothing decided.
	tr.send(t, clickEvent(t, "env1", slack.ApproveAction, id, cardTS, "U2", false))
	if ack := tr.ack(2 * time.Second); ack != "env1" {
		t.Fatalf("ack = %q, want env1", ack)
	}
	if got := queued(t, r.Store, "workstation"); len(got) != 0 {
		t.Fatalf("queued after a click that does not count = %+v, want nothing", got)
	}
	if err := r.Approvals.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if w := f.Whispers("U2"); len(w) != 1 {
		t.Fatalf("U2 was told %q, want one hint", w)
	}
	// The approver, but from a block that is not the card's: not a click.
	ev := clickEvent(t, "env1b", slack.ApproveAction, id, cardTS, "U1", false)
	cb := ev.Data.(slackgo.InteractionCallback)
	cb.ActionCallback.BlockActions[0].BlockID = "elsewhere"
	ev.Data = cb
	tr.send(t, ev)
	if ack := tr.ack(2 * time.Second); ack != "env1b" {
		t.Fatalf("ack = %q, want env1b", ack)
	}
	if got := queued(t, r.Store, "workstation"); len(got) != 0 {
		t.Fatalf("queued after a click from another block = %+v, want nothing", got)
	}
	// The approver: acked once the outcome is queued.
	tr.send(t, clickEvent(t, "env2", slack.ApproveAction, id, cardTS, "U1", false))
	if ack := tr.ack(2 * time.Second); ack != "env2" {
		t.Fatalf("ack = %q, want env2", ack)
	}
	got := queued(t, r.Store, "workstation")
	if len(got) != 1 || got[0].Type != payload.Approval || got[0].ApprovalID != id || got[0].Outcome != payload.Approved || got[0].Approver != "U1" {
		t.Fatalf("queued when the ack arrived = %+v, want the approved outcome of %s", got, id)
	}
	// An interaction that is not a button press is acked at once.
	tr.send(t, socketmode.Event{Type: socketmode.EventTypeInteractive, Data: slackgo.InteractionCallback{Type: slackgo.InteractionTypeViewSubmission}, Request: &socketmode.Request{EnvelopeID: "env3"}})
	if ack := tr.ack(2 * time.Second); ack != "env3" {
		t.Fatalf("ack of a non-button interaction = %q, want env3", ack)
	}
	// The store is gone: no ack.
	r.Store.Close()
	tr.send(t, clickEvent(t, "env4", slack.RejectAction, id, cardTS, "U1", false))
	if ack := tr.ack(200 * time.Millisecond); ack != "" {
		t.Fatalf("acked %q although the click could not be applied", ack)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run returned %v, want context.Canceled", err)
	}

	// Without approvals, a click is acked and dropped.
	r, _ = newReceiver(t)
	tr = newFakeTransport()
	ctx, cancel = context.WithCancel(t.Context())
	go func() { done <- run(ctx, tr, r, time.Millisecond) }()
	tr.send(t, clickEvent(t, "env5", slack.ApproveAction, id, cardTS, "U1", false))
	if ack := tr.ack(2 * time.Second); ack != "env5" {
		t.Fatalf("ack without approvals = %q, want env5", ack)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run returned %v, want context.Canceled", err)
	}
}

// fakeCommander records the commands and clicks it is handed and answers
// each command with the text it got.
type fakeCommander struct {
	mu       sync.Mutex
	commands []slack.Command
	clicks   []slack.Click
	// fail makes Command and Click fail.
	fail bool
}

func (f *fakeCommander) Command(_ context.Context, c slack.Command) (slack.CommandReply, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return slack.CommandReply{}, errors.New("store is gone")
	}
	f.commands = append(f.commands, c)
	return slack.CommandReply{Text: "got " + c.Text}, nil
}

func (f *fakeCommander) Click(_ context.Context, c slack.Click) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return errors.New("store is gone")
	}
	f.clicks = append(f.clicks, c)
	return nil
}

// commandEvent is a Socket Mode event carrying a slash command, in the
// envelope with id.
func commandEvent(envelope, text string) socketmode.Event {
	cmd := slackgo.SlashCommand{Command: "/fednet", Text: text, UserID: "U1", ChannelID: "D1", ResponseURL: "https://example.invalid/respond"}
	return socketmode.Event{Type: socketmode.EventTypeSlashCommand, Data: cmd, Request: &socketmode.Request{Type: socketmode.RequestTypeSlashCommands, EnvelopeID: envelope}}
}

// upgradeClickEvent is a Socket Mode event carrying a press on an upgrade
// card's button, with blockID as the button's block.
func upgradeClickEvent(t *testing.T, envelope, actionID, id, blockID string) socketmode.Event {
	t.Helper()
	var cb slackgo.InteractionCallback
	body := `{"type":"block_actions","user":{"id":"U1"},"container":{"type":"message","is_ephemeral":true,"channel_id":"D1"},"response_url":"https://example.invalid/click",` +
		`"actions":[{"action_id":"` + actionID + `","block_id":"` + blockID + `","type":"button","value":"` + id + `","action_ts":"1.6"}]}`
	if err := json.Unmarshal([]byte(body), &cb); err != nil {
		t.Fatal(err)
	}
	return socketmode.Event{Type: socketmode.EventTypeInteractive, Data: cb, Request: &socketmode.Request{Type: socketmode.RequestTypeInteractive, EnvelopeID: envelope}}
}

// Through the transport: a slash command goes to the Commander and is
// acked with its reply, as an ephemeral message; a press on an upgrade
// card's button goes to the Commander with the response URL and is
// acked; one from another block is acked and dropped; a command or click
// the Commander fails is not acked; without a Commander a command is
// answered that the hub serves none.
func TestRunAcksCommands(t *testing.T) {
	r, _ := newReceiver(t)
	fc := &fakeCommander{}
	r.Commands = fc
	tr := newFakeTransport()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- run(ctx, tr, r, time.Millisecond) }()

	tr.send(t, commandEvent("env1", "version"))
	if ack := tr.ack(2 * time.Second); ack != "env1" {
		t.Fatalf("ack = %q, want env1", ack)
	}
	p, ok := tr.payload("env1").(map[string]any)
	if !ok || p["response_type"] != "ephemeral" || p["text"] != "got version" {
		t.Fatalf("the command was acked with %#v, want the reply as an ephemeral message", tr.payload("env1"))
	}
	fc.mu.Lock()
	if len(fc.commands) != 1 || fc.commands[0].Name != "/fednet" || fc.commands[0].User != "U1" || fc.commands[0].ResponseURL == "" {
		t.Fatalf("the Commander got %+v", fc.commands)
	}
	fc.mu.Unlock()

	tr.send(t, upgradeClickEvent(t, "env2", slack.UpgradeConfirmAction, "x1", slack.UpgradeBlockID("x1")))
	if ack := tr.ack(2 * time.Second); ack != "env2" {
		t.Fatalf("ack = %q, want env2", ack)
	}
	tr.send(t, upgradeClickEvent(t, "env3", slack.UpgradeCancelAction, "x1", "elsewhere"))
	if ack := tr.ack(2 * time.Second); ack != "env3" {
		t.Fatalf("ack = %q, want env3", ack)
	}
	fc.mu.Lock()
	if len(fc.clicks) != 1 || fc.clicks[0].ID != "x1" || !fc.clicks[0].Approve || fc.clicks[0].User != "U1" || fc.clicks[0].ResponseURL != "https://example.invalid/click" {
		t.Fatalf("the Commander got clicks %+v, want the confirm from the card's block alone", fc.clicks)
	}
	fc.fail = true
	fc.mu.Unlock()
	tr.send(t, commandEvent("env4", "version"))
	if ack := tr.ack(200 * time.Millisecond); ack != "" {
		t.Fatalf("acked %q although the command failed", ack)
	}
	tr.send(t, upgradeClickEvent(t, "env5", slack.UpgradeConfirmAction, "x1", slack.UpgradeBlockID("x1")))
	if ack := tr.ack(200 * time.Millisecond); ack != "" {
		t.Fatalf("acked %q although the click failed", ack)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run returned %v, want context.Canceled", err)
	}

	r, _ = newReceiver(t)
	tr = newFakeTransport()
	ctx, cancel = context.WithCancel(t.Context())
	go func() { done <- run(ctx, tr, r, time.Millisecond) }()
	tr.send(t, commandEvent("env6", "version"))
	if ack := tr.ack(2 * time.Second); ack != "env6" {
		t.Fatalf("ack without a Commander = %q, want env6", ack)
	}
	if p, _ := tr.payload("env6").(map[string]any); p["text"] != "这个 hub 不处理命令" {
		t.Fatalf("without a Commander the command was acked with %#v", tr.payload("env6"))
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run returned %v, want context.Canceled", err)
	}
}

// Once a reply has handed a thread over, the thread's first message,
// which mentioned nobody, coming again (redelivered, or read by a
// backfill) is still chatter: not sent, not recorded.
func TestHandleChatterRootAfterReplyHandsOver(t *testing.T) {
	ctx := t.Context()
	r, f := newReceiver(t)
	root := post(t, f, "C1", slack.Message{User: "U1", Text: "chatter root"})
	handle(t, r, root)
	ask := post(t, f, "C1", slack.Message{User: "U1", Text: hey + "take this", ThreadTS: root.TS})
	handle(t, r, ask)
	handle(t, r, root)
	got := queued(t, r.Store, "workstation")
	if len(got) != 1 || got[0].TS != ask.TS {
		t.Fatalf("queued = %+v, want the mention alone", got)
	}
	fresh, err := r.Store.ReceiveSlack(ctx, store.SlackMessage{Channel: "C1", TS: root.TS}, func(*store.Hub) error { return nil })
	if err != nil || !fresh {
		t.Fatalf("the chatter root was recorded (fresh = %v, %v)", fresh, err)
	}
}

// A first mention posted while the hub was away is backfilled whether
// the thread's first message mentioned the bot (the thread is owned) or
// not (it is not, and the backfill finds the thread by its latest
// reply). A thread started before the window, with no owner, is not
// read: Slack's history does not give its first message, so the hub
// cannot find it, and the mention is neither sent nor hands the thread
// over. That is the limit the design records as a known issue; the
// person mentions the bot again.
func TestBackfillFirstMentionInThread(t *testing.T) {
	for _, tt := range []struct {
		name string
		root slack.Message
		want bool
	}{
		{"chatter root", slack.Message{User: "U1", Text: "chatter"}, true},
		{"mentioned root", slack.Message{User: "U1", Text: hey + "first"}, true},
		{"root before the window, the known limit", slack.Message{TS: slackTS(epoch.Add(-2 * DefaultWindow)), User: "U1", Text: "long ago"}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r, f := newReceiver(t)
			root := post(t, f, "C1", tt.root)
			handle(t, r, root)
			// A later message is the latest seen, so the backfill's start
			// is after the thread's first message.
			handle(t, r, post(t, f, "C1", slack.Message{User: "U1", Text: hey + "seed"}))
			r.Disconnected()
			ask := post(t, f, "C1", slack.Message{User: "U1", Text: hey + "offline ask", ThreadTS: root.TS})
			connected(t, r)
			runBackfill(t, r)
			var got []string
			for _, m := range queued(t, r.Store, "workstation") {
				got = append(got, m.TS)
			}
			if found := slices.Contains(got, ask.TS); found != tt.want {
				t.Fatalf("the mention posted while away was backfilled: %v, want %v (queued %v)", found, tt.want, got)
			}
			if _, err := r.Store.Owner(t.Context(), "C1/"+root.TS); (err == nil) != tt.want {
				t.Fatalf("Owner after the backfill: err = %v, want owned %v", err, tt.want)
			}
		})
	}
}

// infoGate is a Slack whose ChannelInfo waits, each call, until the test
// releases it: entered gets a channel to close for each call.
type infoGate struct {
	slack.API
	entered chan chan struct{}
}

func (g *infoGate) ChannelInfo(ctx context.Context, channel string) (slack.ChannelInfo, error) {
	release := make(chan struct{})
	select {
	case g.entered <- release:
	case <-ctx.Done():
		return slack.ChannelInfo{}, ctx.Err()
	}
	select {
	case <-release:
		return g.API.ChannelInfo(ctx, channel)
	case <-ctx.Done():
		return slack.ChannelInfo{}, ctx.Err()
	}
}

// Two mentions in a thread nobody handed over yet, handled at once: both
// read Slack before either is queued, but only the one queued first hands
// the thread over; the other is a reply, with no history.
func TestHandleConcurrentFirstMentions(t *testing.T) {
	r, f := newReceiver(t)
	root := post(t, f, "C1", slack.Message{User: "U1", Text: "chatter"})
	handle(t, r, root)
	first := post(t, f, "C1", slack.Message{User: "U1", Text: hey + "first", ThreadTS: root.TS})
	second := post(t, f, "C1", slack.Message{User: "U1", Text: hey + "second", ThreadTS: root.TS})
	g := &infoGate{API: f, entered: make(chan chan struct{})}
	r.Slack = g
	done := make(chan error, 2)
	go func() { done <- r.Handle(t.Context(), first) }()
	release1 := <-g.entered
	go func() { done <- r.Handle(t.Context(), second) }()
	release2 := <-g.entered
	close(release1)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	close(release2)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got := queued(t, r.Store, "workstation")
	if len(got) != 2 || got[0].TS != first.TS || got[1].TS != second.TS {
		t.Fatalf("queued = %+v, want first then second", got)
	}
	if got[0].Trigger != payload.Mention || got[0].History == nil || got[0].Context == "" {
		t.Fatalf("the first = %+v, want a mention with history and context", got[0])
	}
	if got[1].Trigger != payload.Reply || got[1].History != nil || got[1].Context != "" {
		t.Fatalf("the second = %+v, want a reply without history or context", got[1])
	}
}
