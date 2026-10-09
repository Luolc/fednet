// Package slack is the part of Slack's Web API the hub uses on behalf of the
// agents. Only the hub holds a Slack token: clients ask the hub, and the hub
// calls an API. This version has only Fake; the real API comes later.
package slack

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
)

// ErrNotFound is returned for a channel or thread Slack does not know.
var ErrNotFound = errors.New("slack: not found")

// Message is one message in a thread.
type Message struct {
	// TS is the message's Slack timestamp, which identifies it in its channel.
	TS   string `json:"ts"`
	User string `json:"user"`
	Text string `json:"text"`
}

// API is what the hub needs from Slack.
type API interface {
	// Replies returns the messages of the thread that starts at ts in
	// channel, the first message included, oldest first.
	Replies(ctx context.Context, channel, ts string) ([]Message, error)
	// Post posts text in channel as a new message, which starts a thread,
	// and returns its ts.
	Post(ctx context.Context, channel, text string) (string, error)
	// Purpose returns channel's purpose, the description shown with it.
	Purpose(ctx context.Context, channel string) (string, error)
	// SetPurpose replaces channel's purpose.
	SetPurpose(ctx context.Context, channel, purpose string) error
}

// ThreadKey is how fednet names a thread: its channel and the ts of its
// first message, joined by a slash.
func ThreadKey(channel, ts string) string { return channel + "/" + ts }

// ParseThreadKey splits a thread key into its channel and ts.
func ParseThreadKey(key string) (channel, ts string, ok bool) {
	channel, ts, ok = strings.Cut(key, "/")
	return channel, ts, ok && channel != "" && ts != "" && !strings.Contains(ts, "/")
}

// Fake is an API kept in memory, for tests. Its zero value has no
// channels; AddChannel adds one.
type Fake struct {
	mu       sync.Mutex
	channels map[string]*fakeChannel
	clock    int
}

type fakeChannel struct {
	purpose string
	threads map[string][]Message
}

// AddChannel adds an empty channel with purpose.
func (f *Fake) AddChannel(channel, purpose string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.channels == nil {
		f.channels = make(map[string]*fakeChannel)
	}
	f.channels[channel] = &fakeChannel{purpose: purpose, threads: make(map[string][]Message)}
}

// Reply adds a message from user to the thread at ts in channel and returns
// the new message's ts.
func (f *Fake) Reply(channel, ts, user, text string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.channels[channel]
	if !ok || c.threads[ts] == nil {
		return "", ErrNotFound
	}
	m := Message{TS: f.next(), User: user, Text: text}
	c.threads[ts] = append(c.threads[ts], m)
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

// Post posts as the user "fednet".
func (f *Fake) Post(_ context.Context, channel, text string) (string, error) {
	return f.Start(channel, "fednet", text)
}

// Start posts text from user in channel as a new message, which starts a
// thread, and returns its ts.
func (f *Fake) Start(channel, user, text string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.channels[channel]
	if !ok {
		return "", ErrNotFound
	}
	ts := f.next()
	c.threads[ts] = []Message{{TS: ts, User: user, Text: text}}
	return ts, nil
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
