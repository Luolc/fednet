// Package inbound takes in the messages people post in Slack and hands them
// to the thread routing. Receiver is the logic: it takes a message event,
// records it in the hub's database and queues it for a client in one
// transaction, so that the caller may then ack the event, and it backfills
// from Slack's history what a disconnection missed. Run, in socket.go,
// feeds a Receiver from a Socket Mode connection.
package inbound

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/route"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
)

// DefaultWindow is how far back a backfill reads at most.
const DefaultWindow = 24 * time.Hour

// NoMachineText is what the hub says in a new thread no machine takes.
const NoMachineText = "没有机器接这个 channel"

// subtypes are the message subtypes taken in besides a plain message: a
// message with files uploaded, and a reply also sent to the channel. Every
// other subtype is a change to a message or something Slack did (someone
// joined, a topic changed), not something a person said.
var subtypes = []string{"", "file_share", "thread_broadcast"}

// Event is one message event, as the transport parsed it or as a backfill
// read it from history.
type Event struct {
	// ID is the id of the Slack event; empty for a message from history.
	ID string
	// Channel is the channel, or direct message conversation, the message
	// is in.
	Channel string
	// IM is set when Channel is a direct message conversation.
	IM bool
	slack.Message
}

// Status is how the hub stands with Slack.
type Status struct {
	Connected bool
	// Since is when the connection last came up or, when Connected is not
	// set, last went down; zero before the first attempt to connect.
	Since time.Time
}

// Receiver takes in message events. Its exported fields are set before use
// and not changed after.
type Receiver struct {
	Store *store.Hub
	Slack slack.API
	Route route.Config
	// Users is the user list: only messages from people on it are taken
	// in.
	Users map[string]string
	// Window is how far back a backfill reads at most; zero means
	// DefaultWindow.
	Window time.Duration
	// Now returns the current time; nil means time.Now.
	Now func() time.Time

	mu        sync.Mutex
	connected bool
	since     time.Time
}

// Handle takes in ev: if it is a message to hand on, Handle records it and
// queues it for the client the routing picks, in one transaction, and
// returns once that has committed; the caller then acks the event. A
// message recorded before is not queued again. One that passes the filter
// but that no client takes is recorded all the same, and in a new thread
// the hub says so in the thread. An error means nothing was recorded, and
// the event must not be acked.
func (r *Receiver) Handle(ctx context.Context, ev Event) error {
	if !r.wanted(ev) {
		return nil
	}
	thread := slack.ThreadKey(ev.Channel, ev.ThreadTS)
	if ev.ThreadTS == "" {
		thread = slack.ThreadKey(ev.Channel, ev.TS)
	}
	b, err := json.Marshal(payload.Message{Type: payload.Inbound, Thread: thread, Text: text(ev), User: ev.User, TS: ev.TS})
	if err != nil {
		return err
	}
	var noMachine bool
	fresh, err := r.Store.ReceiveSlack(ctx, store.SlackMessage{Channel: ev.Channel, TS: ev.TS, EventID: ev.ID}, func(tx *store.Hub) error {
		rt := route.New(tx, r.Route)
		var err error
		switch {
		case ev.ThreadTS != "":
			_, err = rt.RouteReply(ctx, thread, b)
		case ev.IM:
			_, err = rt.RouteNewDM(ctx, thread, b)
		default:
			_, err = rt.RouteNew(ctx, ev.Channel, thread, b)
		}
		switch {
		case errors.Is(err, route.ErrNoMachine):
			noMachine = true
		case errors.Is(err, route.ErrNoOwner):
			slog.Warn("inbound: reply in a thread with no owner, dropped", "thread", thread, "ts", ev.TS)
		default:
			return err
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("inbound: %s: %w", slack.ThreadKey(ev.Channel, ev.TS), err)
	}
	if fresh && noMachine {
		slog.Warn("inbound: new thread in a channel no machine takes", "thread", thread)
		if _, err := r.Slack.PostReply(ctx, ev.Channel, ev.TS, NoMachineText); err != nil {
			slog.Warn("inbound: telling the thread no machine takes it", "thread", thread, "err", err)
		}
	}
	return nil
}

// wanted reports whether ev is a message a person on the user list said.
func (r *Receiver) wanted(ev Event) bool {
	if ev.BotID != "" || !slices.Contains(subtypes, ev.SubType) {
		return false
	}
	_, ok := r.Users[ev.User]
	return ok
}

// text is the message's text with its files listed after it, one a line.
func text(ev Event) string {
	lines := make([]string, 0, 1+len(ev.Files))
	if ev.Text != "" {
		lines = append(lines, ev.Text)
	}
	for _, f := range ev.Files {
		lines = append(lines, "file: "+f.Name+" "+f.URL)
	}
	return strings.Join(lines, "\n")
}

// Backfill reads from Slack what the hub may have missed and takes it in
// as Handle does, deduplicated against what came in live: for every
// conversation the bot is in, the messages after the latest the hub has
// seen, at most Window back, and the replies in that conversation's owned
// threads after it. Nothing is read before the hub has seen any message.
// Where a backfill starts is recorded before it reads, and cleared only
// when all of it has been read, so that a backfill that failed partway
// starts from the same place next time even though live messages have
// moved the latest seen on; a hub restart starts there too.
func (r *Receiver) Backfill(ctx context.Context) error {
	st := r.Store
	from, err := st.SlackState(ctx, store.LastSeen)
	if err != nil {
		return err
	}
	pending, err := st.SlackState(ctx, store.BackfillFrom)
	if err != nil {
		return err
	}
	if pending != "" && (from == "" || slack.CompareTS(pending, from) < 0) {
		from = pending
	}
	if from == "" {
		return nil
	}
	now := r.now()
	if floor := slackTS(now.Add(-r.window())); slack.CompareTS(from, floor) < 0 {
		from = floor
	}
	if err := st.SetSlackState(ctx, store.BackfillFrom, from); err != nil {
		return err
	}
	convs, err := r.Slack.Conversations(ctx)
	if err != nil {
		return fmt.Errorf("inbound: backfill: %w", err)
	}
	for _, c := range convs {
		if err := r.backfill(ctx, c, from); err != nil {
			return fmt.Errorf("inbound: backfill %s: %w", c.ID, err)
		}
	}
	// Everything up to now has been read.
	if err := st.AdvanceLastSeen(ctx, slackTS(now)); err != nil {
		return err
	}
	return st.SetSlackState(ctx, store.BackfillFrom, "")
}

// backfill takes in what c has after from.
func (r *Receiver) backfill(ctx context.Context, c slack.Conversation, from string) error {
	ms, err := r.Slack.History(ctx, c.ID, from)
	if err != nil {
		return err
	}
	for _, m := range ms {
		if err := r.Handle(ctx, Event{Channel: c.ID, IM: c.IM, Message: m}); err != nil {
			return err
		}
	}
	threads, err := r.Store.ThreadsIn(ctx, c.ID)
	if err != nil {
		return err
	}
	for _, t := range threads {
		_, ts, _ := slack.ParseThreadKey(t)
		rs, err := r.Slack.Replies(ctx, c.ID, ts)
		if errors.Is(err, slack.ErrNotFound) {
			// The thread is gone from Slack; nothing to read.
			continue
		}
		if err != nil {
			return err
		}
		for _, m := range rs {
			if m.ThreadTS == "" || slack.CompareTS(m.TS, from) <= 0 {
				continue
			}
			if err := r.Handle(ctx, Event{Channel: c.ID, IM: c.IM, Message: m}); err != nil {
				return err
			}
		}
	}
	return nil
}

// Connected records that the connection to Slack is up.
func (r *Receiver) Connected() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.connected {
		r.connected, r.since = true, r.now()
	}
}

// Disconnected records that the connection to Slack is down, or that the
// first attempt to connect has started.
func (r *Receiver) Disconnected() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.connected || r.since.IsZero() {
		r.connected, r.since = false, r.now()
	}
}

// Status reports whether the hub is connected to Slack, and since when.
func (r *Receiver) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Status{Connected: r.connected, Since: r.since}
}

func (r *Receiver) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *Receiver) window() time.Duration {
	if r.Window != 0 {
		return r.Window
	}
	return DefaultWindow
}

// slackTS writes t as a Slack timestamp: Unix seconds, a dot, six digits
// of microseconds.
func slackTS(t time.Time) string {
	return fmt.Sprintf("%d.%06d", t.Unix(), t.Nanosecond()/1000)
}
