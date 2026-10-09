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

	"github.com/Luolc/fednet/internal/approval"
	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/route"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
)

// DefaultWindow is how far back a backfill reads at most.
const DefaultWindow = 24 * time.Hour

// NoMachineText is what the hub says in a new thread no machine takes.
const NoMachineText = "没有机器接这个 channel"

// HubName is the name the hub's own posts carry, as the client id of a
// post in the hub's inbox.
const HubName = "hub"

// subtypes are the message subtypes taken in besides a plain message: a
// message typed with /me, a message with files uploaded, and a reply also
// sent to the channel. Every other subtype is a change to a message or
// something Slack did (someone joined, a topic changed), not something a
// person said; the list of subtypes is at
// https://docs.slack.dev/reference/events/message.
var subtypes = []string{"", "me_message", "file_share", "thread_broadcast"}

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

// Commander answers slash commands and takes the clicks on the cards
// they show. An error from either means the event was not handled and
// is not acked.
type Commander interface {
	Command(ctx context.Context, c slack.Command) (slack.CommandReply, error)
	Click(ctx context.Context, c slack.Click) error
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
	// Bot is the Slack user id of the hub's bot: a message in a channel
	// hands its thread to a client only if it mentions the bot. Empty
	// means no message does.
	Bot string
	// History bounds the thread history a Mention carries.
	History Limits
	// Prefetch says which of a message's files the client fetches before
	// it runs the hook.
	Prefetch Prefetch
	// Window is how far back a backfill reads at most; zero means
	// DefaultWindow.
	Window time.Duration
	// Now returns the current time; nil means time.Now.
	Now func() time.Time
	// Stored, if set, is called each time Handle has committed a message:
	// it may have queued one for a client, or put a post from the hub in
	// the hub's inbox. It must not block.
	Stored func()
	// Approvals takes the clicks on approval cards; nil means they are
	// acked and dropped.
	Approvals *approval.Flow
	// Commands answers slash commands and takes the clicks on the cards
	// they show; nil means a command is answered that the hub serves
	// none, and the clicks are dropped.
	Commands Commander

	// mu guards the fields below, and serializes Connected's fixing of
	// the backfill's start with a backfill's clearing of it.
	mu        sync.Mutex
	connected bool
	since     time.Time
	// up is closed by the first Connected.
	up     chan struct{}
	upOnce sync.Once
	// epoch is the store's BackfillEpoch as of this Receiver's last
	// Connected: a backfill clears the start it read only if the store's
	// epoch is still this one, so that a backfill of the connection
	// before, in this process or in the one being replaced, cannot clear
	// what Connected fixed for this one.
	epoch int64
}

// Handle takes in ev: if it is a message to hand on, Handle records it and
// queues it for the client the routing picks, in one transaction, and
// returns once that has committed; the caller then acks the event. A
// message recorded before is not queued again. A reply in a thread the hub
// does not know is preceded by the thread's first message, read from
// Slack, when that is recent enough for a backfill to read.
//
// What is handed on: every message in a direct message conversation; a
// reply in a thread that has an owner; and a message in a channel that
// mentions the bot, whose thread then gets the channel's default machine
// as its owner and, when the thread had messages before, their history.
// A message in a channel that mentions nobody and whose thread has no
// owner is not the agents' business and is not even recorded. A mention
// in a channel no machine takes is recorded, with a post from the hub
// saying so put in the hub's inbox for the outbound side to deliver. An
// error means nothing was recorded, and the event must not be acked.
func (r *Receiver) Handle(ctx context.Context, ev Event) error {
	if !r.wanted(ev) {
		return nil
	}
	thread := slack.ThreadKey(ev.Channel, ev.ThreadTS)
	if ev.ThreadTS == "" {
		thread = slack.ThreadKey(ev.Channel, ev.TS)
	}
	mentioned := !ev.IM && slack.Mentions(ev.Text, r.Bot)
	if !ev.IM && ev.ThreadTS == "" && !mentioned {
		// Chatter in a channel: not the agents' business, whatever its
		// thread later becomes.
		return nil
	}
	owned, err := hasOwner(ctx, r.Store, thread)
	if err != nil {
		return err
	}
	// A reply in a thread the hub does not know: the thread is read once,
	// to take in its first message first and, on a mention, for the
	// history. A mention goes even when the thread cannot be read.
	var before []slack.Message
	if ev.ThreadTS != "" && !owned {
		ms, err := r.Slack.Replies(ctx, ev.Channel, ev.ThreadTS)
		switch {
		case errors.Is(err, slack.ErrNotFound):
		case err != nil && mentioned:
			slog.Warn("inbound: reading the thread of a mention, sending without its history", "thread", thread, "err", err)
		case err != nil:
			return fmt.Errorf("inbound: reading the thread of a reply: %w", err)
		default:
			if err := r.rootFirst(ctx, ev, ms); err != nil {
				return err
			}
			before = ms
			if owned, err = hasOwner(ctx, r.Store, thread); err != nil {
				return err
			}
		}
	}
	if !ev.IM && !owned && !mentioned {
		// A reply in a thread nobody handed over: not taken in.
		return nil
	}
	m := payload.Message{Type: payload.Inbound, Thread: thread, User: ev.User, UserName: r.Users[ev.User], TS: ev.TS, Files: r.Prefetch.mark(files(ev.Files))}
	m.Text = text(ev, r.History.messageChars())
	// What a mention carries is read outside the transaction; whether the
	// message is the one that hands the thread over is decided inside it,
	// on the ownership as of then.
	var purpose string
	var history *payload.History
	if !ev.IM && !owned {
		purpose, history = r.purpose(ctx, ev.Channel), r.history(ev, thread, before)
	}
	_, err = r.Store.ReceiveSlack(ctx, store.SlackMessage{Channel: ev.Channel, TS: ev.TS, EventID: ev.ID}, func(tx *store.Hub) error {
		rt := route.New(tx, r.Route)
		owned, err := hasOwner(ctx, tx, thread)
		if err != nil {
			return err
		}
		switch {
		case ev.IM:
			m.Trigger = payload.DM
		case owned:
			m.Trigger = payload.Reply
		default:
			m.Trigger, m.Context, m.History = payload.Mention, purpose, history
		}
		b, err := json.Marshal(m)
		if err != nil {
			return err
		}
		switch {
		case m.Trigger == payload.Mention:
			_, err = rt.RouteNew(ctx, ev.Channel, thread, b)
		case ev.IM && ev.ThreadTS == "":
			_, err = rt.RouteNewDM(ctx, thread, b)
		default:
			_, err = rt.RouteReply(ctx, thread, b)
		}
		switch {
		case errors.Is(err, route.ErrNoMachine):
			slog.Warn("inbound: new thread no machine takes", "thread", thread)
			return r.tell(ctx, tx, thread, NoMachineText)
		case errors.Is(err, route.ErrNoOwner):
			slog.Warn("inbound: reply in a thread with no owner, dropped", "thread", thread, "ts", ev.TS)
			return nil
		}
		return err
	})
	if err != nil {
		return fmt.Errorf("inbound: %s: %w", slack.ThreadKey(ev.Channel, ev.TS), err)
	}
	if r.Stored != nil {
		r.Stored()
	}
	return nil
}

// Click hands a press on an approval card's button to the approvals; an
// error means it was not applied and must not be acked.
func (r *Receiver) Click(ctx context.Context, c slack.Click) error {
	if r.Approvals == nil {
		slog.Warn("inbound: click on an approval card, but the hub runs no approvals", "approval", c.ID, "user", c.User)
		return nil
	}
	return r.Approvals.Click(ctx, c)
}

// wanted reports whether ev is a message a person on the user list said.
func (r *Receiver) wanted(ev Event) bool {
	if ev.BotID != "" || !slices.Contains(subtypes, ev.SubType) {
		return false
	}
	_, ok := r.Users[ev.User]
	return ok
}

// text is the message's text, cut to max characters, with its files
// listed after it, one a line.
func text(ev Event, max int) string {
	lines := make([]string, 0, 1+len(ev.Files))
	if ev.Text != "" {
		t, _ := clip(ev.Text, max)
		lines = append(lines, t)
	}
	for _, f := range ev.Files {
		lines = append(lines, "file: "+f.Name+" "+f.URL)
	}
	return strings.Join(lines, "\n")
}

// files is the metadata of fs for the payload.
func files(fs []slack.File) []payload.File {
	var out []payload.File
	for _, f := range fs {
		out = append(out, payload.File{ID: f.ID, Name: f.Name, Mimetype: f.Mimetype, Size: f.Size, URL: f.URL})
	}
	return out
}

// purpose returns channel's purpose, the context a new thread carries, or
// "" when Slack does not give it; the message goes without, and the agent
// can still ask for it.
func (r *Receiver) purpose(ctx context.Context, channel string) string {
	p, err := r.Slack.Purpose(ctx, channel)
	if err != nil {
		slog.Warn("inbound: reading the channel's purpose, sending without", "channel", channel, "err", err)
		return ""
	}
	return p
}

// hasOwner reports whether thread has an owner in h.
func hasOwner(ctx context.Context, h *store.Hub, thread string) (bool, error) {
	_, err := h.Owner(ctx, thread)
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

// rootFirst takes in the first message of ev's thread, ms as Slack gives
// the thread, before ev: the reply may have arrived before the message
// that started the thread, as when Slack redelivers what was in flight at
// a disconnection. The first message is taken in only if a backfill could
// read it, that is if it is within Window; an older thread is one the hub
// never had.
func (r *Receiver) rootFirst(ctx context.Context, ev Event, ms []slack.Message) error {
	if len(ms) == 0 || ms[0].TS != ev.ThreadTS || slack.CompareTS(ms[0].TS, slackTS(r.now().Add(-r.window()))) <= 0 {
		return nil
	}
	return r.Handle(ctx, Event{Channel: ev.Channel, IM: ev.IM, Message: ms[0]})
}

// tell puts a post from the hub saying text in thread into the hub's
// inbox, in tx, for the outbound side to post; the msg_id is the thread's
// key, so the same thread is told once.
func (r *Receiver) tell(ctx context.Context, tx *store.Hub, thread, text string) error {
	b, err := json.Marshal(payload.Message{Type: payload.Post, Thread: thread, Text: text})
	if err != nil {
		return err
	}
	_, err = tx.Inbox.PutFrom(ctx, HubName, store.Message{MsgID: HubName + "/" + thread, Payload: b})
	return err
}

// Connected records that the connection to Slack is up and fixes where
// the next backfill starts: at the latest message seen, or earlier where
// a backfill is still pending. It must return before any message of the
// new connection is handled, since a message handled moves the latest
// seen on.
func (r *Receiver) Connected(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.connected {
		r.connected, r.since = true, r.now()
	}
	epoch, err := r.Store.FixBackfillStart(ctx)
	if err != nil {
		return err
	}
	r.epoch = epoch
	r.upOnce.Do(func() { r.up = make(chan struct{}) })
	select {
	case <-r.up:
	default:
		close(r.up)
	}
	return nil
}

// Up returns a channel that is closed once the connection to Slack has
// come up for the first time.
func (r *Receiver) Up() <-chan struct{} {
	r.upOnce.Do(func() { r.up = make(chan struct{}) })
	return r.up
}

// Backfill reads from Slack what the hub may have missed and takes it in
// as Handle does, deduplicated against what came in live: for every
// conversation the bot is in, the messages after the point Connected
// fixed, at most Window back, and the replies in that conversation's
// owned threads after it. There is nothing to read before the hub has seen
// any message. The start is cleared only when all of it has been read, and
// only if no connection has come up meanwhile, so that a backfill that
// failed partway starts from the same place next time, and so does a hub
// that restarted, and so does the connection after one whose backfill was
// still running, in this process or in another on the same database.
func (r *Receiver) Backfill(ctx context.Context) error {
	st := r.Store
	r.mu.Lock()
	epoch := r.epoch
	from, err := st.SlackState(ctx, store.BackfillFrom)
	r.mu.Unlock()
	if err != nil || from == "" {
		return err
	}
	now := r.now()
	floor := slackTS(now.Add(-r.window()))
	if slack.CompareTS(from, floor) < 0 {
		from = floor
	}
	convs, err := r.Slack.Conversations(ctx)
	if err != nil {
		return fmt.Errorf("inbound: backfill: %w", err)
	}
	for _, c := range convs {
		if err := r.backfill(ctx, c, from, floor); err != nil {
			return fmt.Errorf("inbound: backfill %s: %w", c.ID, err)
		}
	}
	// Everything up to now has been read, unless a newer connection has
	// fixed a start of its own, which is its backfill's to clear; the
	// store decides, so that holds across processes too.
	r.mu.Lock()
	defer r.mu.Unlock()
	_, err = st.FinishBackfill(ctx, epoch, slackTS(now))
	return err
}

// backfill takes in what c has after from: the messages that are not
// replies, and the replies in the threads that may have something for the
// agents, which are the owned threads and those, started since floor,
// that got a reply after from (a mention in a thread nobody handed over
// yet is one). The history is read from floor for the latter; what it
// holds before from was seen live.
func (r *Receiver) backfill(ctx context.Context, c slack.Conversation, from, floor string) error {
	ms, err := r.Slack.History(ctx, c.ID, floor)
	if err != nil {
		return err
	}
	threads, err := r.Store.ThreadsIn(ctx, c.ID)
	if err != nil {
		return err
	}
	for _, m := range ms {
		if slack.CompareTS(m.TS, from) > 0 {
			if err := r.Handle(ctx, Event{Channel: c.ID, IM: c.IM, Message: m}); err != nil {
				return err
			}
		}
		if slack.CompareTS(m.LatestReply, from) > 0 {
			threads = append(threads, slack.ThreadKey(c.ID, m.TS))
		}
	}
	slices.Sort(threads)
	for _, t := range slices.Compact(threads) {
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

// DownFor returns how long the hub has been without a connection to
// Slack: zero while it has one, and zero before the first attempt to
// connect; from that attempt on, a hub that has never connected counts as
// down since the attempt.
func (r *Receiver) DownFor() time.Duration {
	s := r.Status()
	if s.Connected || s.Since.IsZero() {
		return 0
	}
	return r.now().Sub(s.Since)
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
