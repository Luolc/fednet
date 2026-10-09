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

	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"

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

	// A new thread in a channel goes to the channel's default machine,
	// with the channel's purpose; a reply in it goes to the owner, without,
	// even after the default changed.
	handle(t, r, message("Ev1", "C2", "1.1", "", "first"))
	if owner, err := h.Owner(ctx, "C2/1.1"); err != nil || owner != "datamachine" {
		t.Fatalf("Owner(C2/1.1) = %q, %v; want datamachine", owner, err)
	}
	r.Route = route.Config{Defaults: map[string]string{"C2": "workstation"}, DM: "workstation"}
	handle(t, r, message("Ev2", "C2", "1.2", "1.1", "reply"))
	me := message("Ev3", "C2", "1.3", "1.1", "shrugs")
	me.SubType = "me_message"
	handle(t, r, me)
	got := queued(t, h, "datamachine")
	want := []payload.Message{
		{Type: payload.Inbound, Thread: "C2/1.1", Text: "first", User: "U1", TS: "1.1", Context: "the data channel"},
		{Type: payload.Inbound, Thread: "C2/1.1", Text: "reply", User: "U1", TS: "1.2"},
		{Type: payload.Inbound, Thread: "C2/1.1", Text: "shrugs", User: "U1", TS: "1.3"},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("queued for datamachine = %+v, want %+v", got, want)
	}

	// Each message that is not a reply in a direct message conversation
	// starts a thread for the DM machine, with no context; a reply in it
	// follows the owner.
	dm := message("Ev4", "D1", "2.1", "", "psst")
	dm.IM = true
	handle(t, r, dm)
	dm = message("Ev5", "D1", "2.2", "", "again")
	dm.IM = true
	handle(t, r, dm)
	reply := message("Ev6", "D1", "2.3", "2.1", "more")
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

	// A reply in a thread Slack does not know goes nowhere.
	handle(t, r, message("Ev7", "C2", "3.2", "3.1", "orphan"))
	if n := len(all(t, h)); n != 6 {
		t.Fatalf("%d queued after an orphan reply, want 6", n)
	}
}

// noPurpose is a Slack whose Purpose fails.
type noPurpose struct{ slack.API }

func (noPurpose) Purpose(context.Context, string) (string, error) { return "", errors.New("flaky") }

func TestHandleWithoutPurpose(t *testing.T) {
	r, f := newReceiver(t)
	r.Slack = noPurpose{f}
	handle(t, r, message("Ev1", "C1", "1.1", "", "first"))
	got := queued(t, r.Store, "workstation")
	want := []payload.Message{{Type: payload.Inbound, Thread: "C1/1.1", Text: "first", User: "U1", TS: "1.1"}}
	if !slices.Equal(got, want) {
		t.Fatalf("queued when the purpose cannot be read = %+v, want %+v", got, want)
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

// Two hubs on the same database, as during a handoff, both get the same
// event: it is queued once. Each has its own connection and its own
// memory; only the database is shared.
func TestHandleDedupsAcrossInstances(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hub.db")
	r1, _ := newReceiver(t)
	r1.Store = openHub(t, path)
	r2, _ := newReceiver(t)
	r2.Store = openHub(t, path)
	handle(t, r1, message("Ev1", "C1", "1.1", "", "first"))
	handle(t, r2, message("Ev1", "C1", "1.1", "", "first"))
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
		{"a topic change", func(ev *Event) { ev.SubType = "channel_topic" }},
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
	// The hub's answer is a post in its inbox, once, that the outbound
	// side delivers like any other post, under the hub's name.
	if got, want := posts(t, r.Store), []string{HubName + "/C3/" + ts + "/" + NoMachineText}; !slices.Equal(got, want) {
		t.Fatalf("hub inbox = %q, want %q", got, want)
	}
	if err := (&outbound.Poster{Store: r.Store, Slack: f}).Pass(ctx); err != nil {
		t.Fatal(err)
	}
	ms, err := f.Replies(ctx, "C3", ts)
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 2 || ms[1].Text != NoMachineText || f.Machine(ms[1].TS) != HubName {
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
	if got := all(t, r.Store); len(got) != 0 {
		t.Fatalf("queued = %q, want nothing", got)
	}
	// A new direct message with no DM machine is told the same way.
	r.Route.DM = ""
	dm := message("Ev3", "D1", "5.1", "", "psst")
	dm.IM = true
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
	r.Disconnected()
	gap := post(t, f, "C1", slack.Message{User: "U1", Text: "c1 gap reply", ThreadTS: c1.TS})
	post(t, f, "C2", slack.Message{User: "U2", Text: "c2 gap reply", ThreadTS: c2.TS})
	post(t, f, "C2", slack.Message{User: "U1", Text: "c2 gap thread"})
	post(t, f, "D1", slack.Message{User: "U1", Text: "dm gap reply", ThreadTS: d1.TS})
	post(t, f, "D1", slack.Message{User: "U1", Text: "dm gap thread"})
	post(t, f, "C1", slack.Message{User: "U1", Text: "bot", BotID: "B1"})
	post(t, f, "C1", slack.Message{User: "U9", Text: "stranger"})
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
	handle(t, r, post(t, f, "C1", slack.Message{User: "U1", Text: "first"}))
	r.Disconnected()
	root := post(t, f, "C1", slack.Message{User: "U1", Text: "gap root"})
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
	old := post(t, f, "C1", slack.Message{TS: slackTS(epoch.Add(-2 * DefaultWindow)), User: "U1", Text: "long ago"})
	handle(t, r, post(t, f, "C1", slack.Message{User: "U1", Text: "late reply", ThreadTS: old.TS}))
	if got := texts(t, r.Store, "workstation"); len(got) != 3 {
		t.Fatalf("texts after a reply in an old thread = %q, want the 3 before", got)
	}
}

func TestBackfillWindow(t *testing.T) {
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
	first := post(t, f, "C1", slack.Message{User: "U1", Text: "first"})
	handle(t, r, first)
	// The hub dies without a word; messages arrive; it comes back up on
	// the same database and a live message gets in before the backfill.
	r.Store.Close()
	post(t, f, "C1", slack.Message{User: "U1", Text: "while down", ThreadTS: first.TS})
	post(t, f, "C1", slack.Message{User: "U1", Text: "also while down"})
	r = &Receiver{Store: openHub(t, path), Slack: f, Route: r.Route, Users: r.Users, Now: r.Now}
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
	first := post(t, f, "C1", slack.Message{User: "U1", Text: "first"})
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
			handle(t, r, post(t, f, "C1", slack.Message{User: "U1", Text: "first"}))
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
			post(t, f, "C1", slack.Message{User: "U1", Text: "gap"})
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
			handle(t, r, post(t, f, "C2", slack.Message{User: "U1", Text: "new live"}))
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
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{events: make(chan socketmode.Event), acks: make(chan string, 10), ran: make(chan struct{})}
}

func (f *fakeTransport) RunContext(ctx context.Context) error {
	<-ctx.Done()
	close(f.ran)
	return ctx.Err()
}

func (f *fakeTransport) Events() <-chan socketmode.Event { return f.events }

func (f *fakeTransport) Ack(_ context.Context, envelopeID string) error {
	f.acks <- envelopeID
	return nil
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
	root := post(t, f, "C1", slack.Message{User: "U1", Text: "first"})
	tr := newFakeTransport()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- run(ctx, tr, r, time.Millisecond) }()

	tr.send(t, socketmode.Event{Type: socketmode.EventTypeConnecting})
	waitFor(t, "the hub to be down while connecting", func() bool { s := r.Status(); return !s.Connected && !s.Since.IsZero() })
	tr.send(t, socketmode.Event{Type: socketmode.EventTypeConnected})
	waitFor(t, "the hub to be up after the connected event", func() bool { return r.Status().Connected })
	// A message: no ack before it is queued; acked once it is.
	tr.send(t, messageEvent(t, "env1", "Ev1", "C1", root.TS, "", "first", true))
	if id := tr.ack(2 * time.Second); id != "env1" {
		t.Fatalf("ack = %q, want env1", id)
	}
	if got, want := texts(t, r.Store, "workstation"), []string{"first\nfile: a.txt https://example.invalid/a"}; !slices.Equal(got, want) {
		t.Fatalf("texts when the ack arrived = %q, want %q", got, want)
	}
	// Redelivered: acked, not queued again.
	tr.send(t, messageEvent(t, "env2", "Ev1", "C1", root.TS, "", "first", true))
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
