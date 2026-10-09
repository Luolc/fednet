// Package slack is the part of Slack's Web API the hub uses on behalf of the
// agents. Only the hub holds a Slack token: clients ask the hub, and the hub
// calls an API. Web calls Slack's Web API; Fake is kept in memory, for
// tests.
package slack

import (
	"cmp"
	"context"
	"errors"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrNotFound is returned for a channel, thread or message Slack does not
// know.
var ErrNotFound = errors.New("slack: not found")

// Message is one message in a channel or a thread.
type Message struct {
	// TS is the message's Slack timestamp, which identifies it in its channel.
	TS   string `json:"ts"`
	User string `json:"user"`
	Text string `json:"text"`
	// ThreadTS is the ts of the first message of the thread the message is
	// a reply in; empty for a message that is not a reply.
	ThreadTS string `json:"thread_ts,omitempty"`
	// BotID is set on a message a bot posted, this one included.
	BotID string `json:"bot_id,omitempty"`
	// SubType is Slack's subtype of the message; empty for a plain message
	// a person typed.
	SubType string `json:"subtype,omitempty"`
	// Files are the files uploaded with the message.
	Files []File `json:"files,omitempty"`
}

// File is a file uploaded with a message.
type File struct {
	Name string `json:"name"`
	// URL is the file's permalink, which opens it in Slack.
	URL string `json:"url"`
}

// Conversation is a channel or a direct message conversation.
type Conversation struct {
	ID string
	// IM is set for a direct message conversation.
	IM bool
}

// API is what the hub needs from Slack.
type API interface {
	// Replies returns the messages of the thread that starts at ts in
	// channel, the first message included, oldest first.
	Replies(ctx context.Context, channel, ts string) ([]Message, error)
	// History returns the messages posted in channel after oldest, a ts,
	// that are not replies in a thread, oldest first.
	History(ctx context.Context, channel, oldest string) ([]Message, error)
	// Conversations returns the channels and direct message conversations
	// the bot is a member of.
	Conversations(ctx context.Context) ([]Conversation, error)
	// Post posts text in channel as a new message, which starts a thread,
	// and returns its ts.
	Post(ctx context.Context, channel, text string) (string, error)
	// Purpose returns channel's purpose, the description shown with it.
	Purpose(ctx context.Context, channel string) (string, error)
	// SetPurpose replaces channel's purpose.
	SetPurpose(ctx context.Context, channel, purpose string) error
	// PostReply posts text in the thread that starts at ts in channel,
	// under a line that names machine, the machine the text comes from, and
	// returns the new message's ts.
	PostReply(ctx context.Context, channel, ts, machine, text string) (string, error)
	// Delete deletes the message at ts in channel.
	Delete(ctx context.Context, channel, ts string) error
	// DM sends text to user as a direct message, which belongs to no
	// thread.
	DM(ctx context.Context, user, text string) error
	// PostCard posts c in channel as a new message and returns its ts.
	PostCard(ctx context.Context, channel string, c Card) (string, error)
	// UpdateCard replaces the message at ts in channel with c.
	UpdateCard(ctx context.Context, channel, ts string, c Card) error
	// Whisper shows text in channel to user alone, as an ephemeral message.
	Whisper(ctx context.Context, channel, user, text string) error
}

// Card is an approval card: what an agent asks leave to do, and, once
// decided, how it ended. While Outcome is empty the card has an approve
// and a reject button, each carrying ID; a decided card has no buttons.
type Card struct {
	// ID is the approval id; a click on a button reports it.
	ID string
	// Summary says what the action does; Params is the action's
	// parameters, the JSON the agent handed in.
	Summary string
	Params  string
	// Machine and Agent are who asked: the agent on the machine.
	Machine string
	Agent   string
	// Expires is when the approval expires unless decided.
	Expires time.Time
	// Outcome is empty while the card waits, and otherwise one of the
	// payload package's outcomes.
	Outcome string
	// Approver is the Slack user id of who approved or rejected.
	Approver string
	// DecidedAt is when Outcome was reached.
	DecidedAt time.Time
}

// Click is a press on one of a card's buttons, as the interactive callback
// reports it.
type Click struct {
	// ID is the approval id the button carried.
	ID string
	// Approve is set for the approve button, clear for the reject button.
	Approve bool
	// User is the Slack user id of who clicked; Bot is set when the user is
	// a bot.
	User string
	Bot  bool
	// Channel and TS locate the card.
	Channel string
	TS      string
}

// Action ids of a card's buttons.
const (
	ApproveAction = "approve"
	RejectAction  = "reject"
)

// ThreadKey is how fednet names a thread: its channel and the ts of its
// first message, joined by a slash.
func ThreadKey(channel, ts string) string { return channel + "/" + ts }

// ParseThreadKey splits a thread key into its channel and ts.
func ParseThreadKey(key string) (channel, ts string, ok bool) {
	channel, ts, ok = strings.Cut(key, "/")
	return channel, ts, ok && channel != "" && ts != "" && !strings.Contains(ts, "/")
}

// CompareTS orders two Slack timestamps, which are "SECONDS.FRACTION"
// strings; it returns -1, 0 or 1 as a is before, at or after b. A ts that
// does not parse sorts first.
func CompareTS(a, b string) int {
	as, af := splitTS(a)
	bs, bf := splitTS(b)
	if as != bs {
		return cmp.Compare(as, bs)
	}
	return cmp.Compare(af, bf)
}

// splitTS parses ts into its seconds and its fraction scaled to
// microseconds; both are -1 for a ts that does not parse.
func splitTS(ts string) (sec, usec int64) {
	s, f, _ := strings.Cut(ts, ".")
	sec, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return -1, -1
	}
	if f == "" {
		return sec, 0
	}
	if len(f) < 6 {
		f += strings.Repeat("0", 6-len(f))
	}
	usec, err = strconv.ParseInt(f[:6], 10, 64)
	if err != nil {
		return -1, -1
	}
	return sec, usec
}

// Fake is an API kept in memory, for tests. Its zero value has no
// channels; AddChannel adds one.
type Fake struct {
	mu       sync.Mutex
	channels map[string]*fakeChannel
	dms      map[string][]string
	// machines maps the ts of each message PostReply posted to the machine
	// it named.
	machines map[string]string
	// cards maps channel and ts, as a thread key, to the card there.
	cards map[string]Card
	// whispers maps a user to the ephemeral texts shown to them.
	whispers map[string][]string
	clock    int
}

type fakeChannel struct {
	purpose string
	im      bool
	threads map[string][]Message
}

// AddChannel adds an empty channel with purpose.
func (f *Fake) AddChannel(channel, purpose string) { f.add(channel, purpose, false) }

// AddIM adds an empty direct message conversation.
func (f *Fake) AddIM(channel string) { f.add(channel, "", true) }

func (f *Fake) add(channel, purpose string, im bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.channels == nil {
		f.channels = make(map[string]*fakeChannel)
	}
	f.channels[channel] = &fakeChannel{purpose: purpose, im: im, threads: make(map[string][]Message)}
}

// Reply adds a message from user to the thread at ts in channel and returns
// the new message's ts.
func (f *Fake) Reply(channel, ts, user, text string) (string, error) {
	return f.Add(channel, Message{ThreadTS: ts, User: user, Text: text})
}

// Add adds m to channel: as a reply in the thread at m.ThreadTS when that
// is set, as a new message that starts a thread otherwise. A m.TS that is
// empty gets a ts later than any before. It returns m's ts.
func (f *Fake) Add(channel string, m Message) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.channels[channel]
	if !ok {
		return "", ErrNotFound
	}
	if m.TS == "" {
		m.TS = f.next()
	}
	if m.ThreadTS == "" {
		c.threads[m.TS] = []Message{m}
		return m.TS, nil
	}
	if c.threads[m.ThreadTS] == nil {
		return "", ErrNotFound
	}
	c.threads[m.ThreadTS] = append(c.threads[m.ThreadTS], m)
	return m.TS, nil
}

// next returns a ts later than any before; f.mu is held.
func (f *Fake) next() string {
	f.clock++
	return "1700000000." + strconv.Itoa(100000+f.clock)
}

func (f *Fake) Replies(_ context.Context, channel, ts string) ([]Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.channels[channel]
	if !ok || c.threads[ts] == nil {
		return nil, ErrNotFound
	}
	return append([]Message(nil), c.threads[ts]...), nil
}

func (f *Fake) History(_ context.Context, channel, oldest string) ([]Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.channels[channel]
	if !ok {
		return nil, ErrNotFound
	}
	var ms []Message
	for _, ts := range slices.SortedFunc(maps.Keys(c.threads), CompareTS) {
		if CompareTS(ts, oldest) > 0 {
			ms = append(ms, c.threads[ts][0])
		}
	}
	return ms, nil
}

// Conversations returns every channel and direct message conversation
// added, ordered by id.
func (f *Fake) Conversations(context.Context) ([]Conversation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var cs []Conversation
	for _, id := range slices.Sorted(maps.Keys(f.channels)) {
		cs = append(cs, Conversation{ID: id, IM: f.channels[id].im})
	}
	return cs, nil
}

// Post posts as the user "fednet".
func (f *Fake) Post(_ context.Context, channel, text string) (string, error) {
	return f.Start(channel, "fednet", text)
}

// Start posts text from user in channel as a new message, which starts a
// thread, and returns its ts.
func (f *Fake) Start(channel, user, text string) (string, error) {
	return f.Add(channel, Message{User: user, Text: text})
}

// PostReply posts as the user "fednet" and records machine, which Machine
// returns.
func (f *Fake) PostReply(_ context.Context, channel, ts, machine, text string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.channels[channel]
	if !ok || c.threads[ts] == nil {
		return "", ErrNotFound
	}
	m := Message{TS: f.next(), User: "fednet", Text: text}
	c.threads[ts] = append(c.threads[ts], m)
	if f.machines == nil {
		f.machines = make(map[string]string)
	}
	f.machines[m.TS] = machine
	return m.TS, nil
}

// Machine returns the machine PostReply named for the message at ts.
func (f *Fake) Machine(ts string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.machines[ts]
}

// Delete deletes the message at ts in channel; deleting the first message
// of a thread deletes the thread.
func (f *Fake) Delete(_ context.Context, channel, ts string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.channels[channel]
	if !ok {
		return ErrNotFound
	}
	delete(f.cards, ThreadKey(channel, ts))
	if c.threads[ts] != nil {
		delete(c.threads, ts)
		return nil
	}
	for root, ms := range c.threads {
		for i, m := range ms {
			if m.TS == ts {
				c.threads[root] = append(ms[:i:i], ms[i+1:]...)
				return nil
			}
		}
	}
	return ErrNotFound
}

func (f *Fake) Purpose(_ context.Context, channel string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.channels[channel]
	if !ok {
		return "", ErrNotFound
	}
	return c.purpose, nil
}

func (f *Fake) SetPurpose(_ context.Context, channel, purpose string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.channels[channel]
	if !ok {
		return ErrNotFound
	}
	c.purpose = purpose
	return nil
}

func (f *Fake) DM(_ context.Context, user, text string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dms == nil {
		f.dms = make(map[string][]string)
	}
	f.dms[user] = append(f.dms[user], text)
	return nil
}

// DMs returns the texts sent to user as direct messages, oldest first.
func (f *Fake) DMs(user string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.dms[user]...)
}

// PostCard posts the card as a message from the user "fednet" whose text
// is the card's summary, and keeps the card for Card.
func (f *Fake) PostCard(_ context.Context, channel string, c Card) (string, error) {
	ts, err := f.Add(channel, Message{User: "fednet", Text: c.Summary})
	if err != nil {
		return "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cards == nil {
		f.cards = make(map[string]Card)
	}
	f.cards[ThreadKey(channel, ts)] = c
	return ts, nil
}

func (f *Fake) UpdateCard(_ context.Context, channel, ts string, c Card) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.cards[ThreadKey(channel, ts)]; !ok {
		return ErrNotFound
	}
	f.cards[ThreadKey(channel, ts)] = c
	return nil
}

// Card returns the card at ts in channel as it is now, and whether there
// is one.
func (f *Fake) Card(channel, ts string) (Card, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.cards[ThreadKey(channel, ts)]
	return c, ok
}

// Cards returns the cards in channel, oldest first, each with its ts.
func (f *Fake) Cards(channel string) (ts []string, cards []Card) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, key := range slices.SortedFunc(maps.Keys(f.cards), func(a, b string) int {
		_, ats, _ := ParseThreadKey(a)
		_, bts, _ := ParseThreadKey(b)
		return CompareTS(ats, bts)
	}) {
		ch, t, _ := ParseThreadKey(key)
		if ch == channel {
			ts = append(ts, t)
			cards = append(cards, f.cards[key])
		}
	}
	return ts, cards
}

func (f *Fake) Whisper(_ context.Context, channel, user, text string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.channels[channel]; !ok {
		return ErrNotFound
	}
	if f.whispers == nil {
		f.whispers = make(map[string][]string)
	}
	f.whispers[user] = append(f.whispers[user], text)
	return nil
}

// Whispers returns the ephemeral texts shown to user, oldest first.
func (f *Fake) Whispers(user string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.whispers[user]...)
}
