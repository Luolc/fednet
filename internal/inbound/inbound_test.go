package inbound

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/route"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
	"github.com/Luolc/fednet/internal/watch"
)

// The fake Slack's clock starts at this ts; tests set Now near it.
var epoch = time.Unix(1700000000, 0)

var users = map[string]string{"U1": "maintainer", "U2": ""}

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
	f.AddChannel("C1", "")
	f.AddChannel("C2", "")
	f.AddChannel("C3", "")
	f.AddIM("D1")
	r := &Receiver{
		Store: openHub(t, filepath.Join(t.TempDir(), "hub.db")),
		Slack: f,
		Route: route.Config{Defaults: map[string]string{"C1": "workstation", "C2": "datamachine"}, DM: "workstation"},
		Users: users,
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

// texts returns the texts of the inbound messages queued for client.
func texts(t *testing.T, h *store.Hub, client string) []string {
	t.Helper()
	var ts []string
	for _, m := range queued(t, h, client) {
		if m.Type != payload.Inbound {
			t.Fatalf("queued for %s: type %q, want %q", client, m.Type, payload.Inbound)
		}
		ts = append(ts, m.Text)
	}
	return ts
}

func handle(t *testing.T, r *Receiver, ev Event) {
	t.Helper()
	if err := r.Handle(t.Context(), ev); err != nil {
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

	// A new thread in a channel goes to the channel's default machine; a
	// reply in it goes to the owner even after the default changed.
	handle(t, r, message("Ev1", "C2", "1.1", "", "first"))
	if owner, err := h.Owner(ctx, "C2/1.1"); err != nil || owner != "datamachine" {
		t.Fatalf("Owner(C2/1.1) = %q, %v; want datamachine", owner, err)
	}
	r.Route = route.Config{Defaults: map[string]string{"C2": "workstation"}, DM: "workstation"}
	handle(t, r, message("Ev2", "C2", "1.2", "1.1", "reply"))
	got := queued(t, h, "datamachine")
	want := []payload.Message{
		{Type: payload.Inbound, Thread: "C2/1.1", Text: "first", User: "U1", TS: "1.1"},
		{Type: payload.Inbound, Thread: "C2/1.1", Text: "reply", User: "U1", TS: "1.2"},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("queued for datamachine = %+v, want %+v", got, want)
	}

	// Each message that is not a reply in a direct message conversation
	// starts a thread for the DM machine; a reply in it follows the owner.
	dm := message("Ev3", "D1", "2.1", "", "psst")
	dm.IM = true
	handle(t, r, dm)
	dm = message("Ev4", "D1", "2.2", "", "again")
	dm.IM = true
	handle(t, r, dm)
	reply := message("Ev5", "D1", "2.3", "2.1", "more")
	reply.IM = true
	handle(t, r, reply)
	got = queued(t, h, "workstation")
	want = []payload.Message{
		{Type: payload.Inbound, Thread: "D1/2.1", Text: "psst", User: "U1", TS: "2.1"},
		{Type: payload.Inbound, Thread: "D1/2.2", Text: "again", User: "U1", TS: "2.2"},
		{Type: payload.Inbound, Thread: "D1/2.1", Text: "more", User: "U1", TS: "2.3"},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("queued for workstation = %+v, want %+v", got, want)
	}

	// A reply in a thread with no owner goes nowhere.
	handle(t, r, message("Ev6", "C2", "3.2", "3.1", "orphan"))
	if n := len(queued(t, h, "workstation")) + len(queued(t, h, "datamachine")); n != 5 {
		t.Fatalf("%d queued after an orphan reply, want 5", n)
	}
}

func TestHandleFiles(t *testing.T) {
	r, _ := newReceiver(t)
	ev := message("Ev1", "C1", "1.1", "", "see these")
	ev.SubType = "file_share"
	ev.Files = []slack.File{{Name: "a.txt", URL: "https://example.invalid/a"}, {Name: "b c.png", URL: "https://example.invalid/b"}}
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
}

func TestHandleDedupsEvents(t *testing.T) {
	r, _ := newReceiver(t)
	// Slack redelivers an event it got no ack for; the same message may
	// also come under another event id, and later from history.
	handle(t, r, message("Ev1", "C1", "1.1", "", "first"))
	handle(t, r, message("Ev1", "C1", "1.1", "", "first"))
	handle(t, r, message("Ev2", "C1", "1.1", "", "first"))
	handle(t, r, message("", "C1", "1.1", "", "first"))
	handle(t, r, message("Ev3", "C1", "1.2", "1.1", "reply"))
	handle(t, r, message("Ev3", "C1", "1.2", "1.1", "reply"))
	if got := texts(t, r.Store, "workstation"); !slices.Equal(got, []string{"first", "reply"}) {
		t.Fatalf("texts = %q, want [first reply]", got)
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
	handle(t, r, message("Ev1", "C1", "1.1", "", "first"))
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
	handle(t, r, message("Ev0", "C1", "1.0", "", "first"))
	for _, tt := range []struct {
		name string
		edit func(ev *Event)
	}{
		{"not on the user list", func(ev *Event) { ev.User = "U9" }},
		{"a bot, this one included", func(ev *Event) { ev.BotID = "B1" }},
		{"an edit", func(ev *Event) { ev.SubType = "message_changed" }},
		{"a deletion", func(ev *Event) { ev.SubType = "message_deleted" }},
		{"someone joining", func(ev *Event) { ev.SubType = "channel_join" }},
	} {
		ev := message("Ev"+tt.name, "C1", "1.1", "", tt.name)
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
	handle(t, r, message("Ev1", "C1", "1.1", "", "second"))
	if got := texts(t, r.Store, "workstation"); !slices.Equal(got, []string{"first", "second"}) {
		t.Fatalf("texts = %q, want [first second]", got)
	}
}

func TestHandleNoMachineTellsThread(t *testing.T) {
	ctx := t.Context()
	r, f := newReceiver(t)
	ts, err := f.Start("C3", "U1", "anyone?")
	if err != nil {
		t.Fatal(err)
	}
	ev := message("Ev1", "C3", ts, "", "anyone?")
	handle(t, r, ev)
	handle(t, r, ev)
	ms, err := f.Replies(ctx, "C3", ts)
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 2 || ms[1].User != "fednet" || ms[1].Text != NoMachineText || f.Machine(ms[1].TS) != HubName {
		t.Fatalf("thread after a message no machine takes = %+v, want one reply from the hub saying so", ms)
	}
	if _, err := r.Store.Owner(ctx, "C3/"+ts); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Owner: err = %v, want ErrNotFound", err)
	}
	// A reply in that thread is dropped quietly.
	handle(t, r, message("Ev2", "C3", "9.9", ts, "still there?"))
	if ms, _ := f.Replies(ctx, "C3", ts); len(ms) != 2 {
		t.Fatalf("thread after a reply = %+v, want it unchanged", ms)
	}
	for _, client := range []string{"workstation", "datamachine"} {
		if got := texts(t, r.Store, client); len(got) != 0 {
			t.Fatalf("queued for %s = %q, want nothing", client, got)
		}
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
	ctx := t.Context()
	r, f := newReceiver(t)
	h := r.Store

	// Nothing is read before the hub has seen a message.
	for _, ch := range []string{"C1", "C2", "D1"} {
		post(t, f, ch, slack.Message{User: "U1", Text: "before fednet"})
	}
	if err := r.Backfill(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(texts(t, h, "workstation")) + len(texts(t, h, "datamachine")); n != 0 {
		t.Fatalf("%d queued by a backfill before any message was seen, want 0", n)
	}

	// Live: a thread in C1, a thread in C2, a DM thread.
	c1 := post(t, f, "C1", slack.Message{User: "U1", Text: "c1 first"})
	handle(t, r, c1)
	c2 := post(t, f, "C2", slack.Message{User: "U1", Text: "c2 first"})
	handle(t, r, c2)
	d1 := post(t, f, "D1", slack.Message{User: "U1", Text: "dm first"})
	handle(t, r, d1)
	// The last live message, then the connection drops: in the gap, a
	// reply in each thread, a new thread in C2, a new DM, a bot message
	// and a message from someone else, which are not for the agents.
	handle(t, r, post(t, f, "C1", slack.Message{User: "U1", Text: "c1 last live", ThreadTS: c1.TS}))
	post(t, f, "C1", slack.Message{User: "U1", Text: "c1 gap reply", ThreadTS: c1.TS})
	post(t, f, "C2", slack.Message{User: "U2", Text: "c2 gap reply", ThreadTS: c2.TS})
	post(t, f, "C2", slack.Message{User: "U1", Text: "c2 gap thread"})
	post(t, f, "D1", slack.Message{User: "U1", Text: "dm gap reply", ThreadTS: d1.TS})
	post(t, f, "D1", slack.Message{User: "U1", Text: "dm gap thread"})
	post(t, f, "C1", slack.Message{User: "U1", Text: "bot", BotID: "B1"})
	post(t, f, "C1", slack.Message{User: "U9", Text: "stranger"})
	// Reconnected: Slack redelivers the event in flight when the
	// connection dropped, and a live message arrives while the backfill
	// runs.
	handle(t, r, Event{ID: "Ev-redelivered", Channel: "C1", Message: slack.Message{TS: "1700000000.100007", ThreadTS: c1.TS, User: "U1", Text: "c1 gap reply"}})
	if err := r.Backfill(ctx); err != nil {
		t.Fatal(err)
	}
	live := post(t, f, "C1", slack.Message{User: "U1", Text: "c1 after", ThreadTS: c1.TS})
	handle(t, r, live)

	if got, want := texts(t, h, "workstation"), []string{"c1 first", "dm first", "c1 last live", "c1 gap reply", "dm gap thread", "dm gap reply", "c1 after"}; !slices.Equal(got, want) {
		t.Fatalf("texts for workstation = %q, want %q", got, want)
	}
	if got, want := texts(t, h, "datamachine"), []string{"c2 first", "c2 gap thread", "c2 gap reply"}; !slices.Equal(got, want) {
		t.Fatalf("texts for datamachine = %q, want %q", got, want)
	}
	// A second backfill, with nothing new, delivers nothing again.
	if err := r.Backfill(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(texts(t, h, "workstation")) + len(texts(t, h, "datamachine")); n != 10 {
		t.Fatalf("%d queued after a second backfill, want 10", n)
	}
}

func TestBackfillWindow(t *testing.T) {
	ctx := t.Context()
	r, f := newReceiver(t)
	r.Window = time.Hour
	// Seen a message two hours ago, then nothing live.
	old := post(t, f, "C1", slack.Message{TS: slackTS(epoch.Add(-2 * time.Hour)), User: "U1", Text: "seen"})
	handle(t, r, old)
	post(t, f, "C1", slack.Message{TS: slackTS(epoch.Add(-90 * time.Minute)), User: "U1", Text: "too old", ThreadTS: old.TS})
	post(t, f, "C1", slack.Message{TS: slackTS(epoch.Add(-80 * time.Minute)), User: "U1", Text: "too old too"})
	post(t, f, "C1", slack.Message{TS: slackTS(epoch.Add(-30 * time.Minute)), User: "U1", Text: "in the window", ThreadTS: old.TS})
	post(t, f, "C1", slack.Message{TS: slackTS(epoch.Add(-20 * time.Minute)), User: "U1", Text: "in the window too"})
	r.Now = func() time.Time { return epoch }
	if err := r.Backfill(ctx); err != nil {
		t.Fatal(err)
	}
	if got, want := texts(t, r.Store, "workstation"), []string{"seen", "in the window too", "in the window"}; !slices.Equal(got, want) {
		t.Fatalf("texts = %q, want %q", got, want)
	}
}

func TestBackfillAfterRestart(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "hub.db")
	r, f := newReceiver(t)
	r.Store = openHub(t, path)
	first := post(t, f, "C1", slack.Message{User: "U1", Text: "first"})
	handle(t, r, first)
	// The hub goes down; messages arrive; it comes back up on the same
	// database.
	r.Store.Close()
	post(t, f, "C1", slack.Message{User: "U1", Text: "while down", ThreadTS: first.TS})
	post(t, f, "C1", slack.Message{User: "U1", Text: "also while down"})
	r.Store = openHub(t, path)
	if err := r.Backfill(ctx); err != nil {
		t.Fatal(err)
	}
	if got, want := texts(t, r.Store, "workstation"), []string{"first", "also while down", "while down"}; !slices.Equal(got, want) {
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

// A backfill that fails starts from the same place when run again, even
// though a live message has moved the latest seen on meanwhile.
func TestBackfillRetriesFromWhereItFailed(t *testing.T) {
	ctx := t.Context()
	r, f := newReceiver(t)
	fl := &flaky{API: f, fails: 1}
	r.Slack = fl
	first := post(t, f, "C1", slack.Message{User: "U1", Text: "first"})
	handle(t, r, first)
	post(t, f, "C1", slack.Message{User: "U1", Text: "in the gap", ThreadTS: first.TS})
	if err := r.Backfill(ctx); err == nil {
		t.Fatal("the first backfill did not fail")
	}
	handle(t, r, post(t, f, "C1", slack.Message{User: "U1", Text: "live after the gap", ThreadTS: first.TS}))
	// Run keeps trying until the backfill succeeds.
	backfillUntilDone(ctx, r, time.Millisecond)
	if got, want := texts(t, r.Store, "workstation"), []string{"first", "live after the gap", "in the gap"}; !slices.Equal(got, want) {
		t.Fatalf("texts = %q, want %q", got, want)
	}
	if from, _ := r.Store.SlackState(ctx, store.BackfillFrom); from != "" {
		t.Fatalf("BackfillFrom after a finished backfill = %q, want empty", from)
	}
}

// The Receiver is the watch's view of the Slack connection.
var _ watch.SlackLink = (*Receiver)(nil)

func TestStatus(t *testing.T) {
	now := epoch
	r := &Receiver{Now: func() time.Time { return now }}
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
	r.Connected()
	now = now.Add(time.Second)
	r.Connected()
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
