// Package outbound posts to Slack what the clients posted: the hub's inbox
// holds each post until it is in Slack. Posts go out in the order they
// reached the hub, each in its thread. A post longer than MaxChars is
// split into consecutive messages in the same thread. A post may mention
// people on the user list at its start. The Slack ts of every message a
// post goes out as is recorded, so that the client that posted it can
// delete it.
//
// A post Slack does not take is left in the inbox and tried again later,
// after the Slack API's own waits on rate limits; nothing behind it goes
// out first, so a thread's messages stay in order. A post that can never
// go out (its payload is not a post, its thread is not in Slack) is
// alerted, logged and marked delivered, so it does not hold up the rest.
//
// A post may also be a footer, one line of small grey text that ends with
// the name of the machine it came from, or a progress card: one card open
// at a time in each thread, which each progress replaces whole, kept in
// the store so that it outlives the process. Whatever goes into a
// thread, uploads included, first closes its open card, as a progress
// with Close slack.Done does.
//
// The inbox also holds the alerts clients raise, which only the hub can
// send, since only the hub has the webhook: each goes to the webhook as
// from the client that raised it, and one that fails stays in the inbox
// for the next pass without holding up the posts.
package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Luolc/fednet/internal/alert"
	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
)

// Defaults for the zero fields of Poster.
const (
	// DefaultMaxChars is about the longest message Slack recommends.
	DefaultMaxChars = 4000
	DefaultInterval = 5 * time.Second
	// DefaultUploadWait is well within the time a client gives an upload.
	DefaultUploadWait = 30 * time.Second
)

// uploadPoll is how often BeforeUpload looks at the inbox; tests shorten
// it.
var uploadPoll = 100 * time.Millisecond

// Poster posts the hub's inbox to Slack. Run serves until its context is
// done; Nudge tells it a post has arrived.
type Poster struct {
	Store *store.Hub
	Slack slack.API
	// Alert, if not nil, gets an alert for each post that can never go
	// out, and the alerts clients raise.
	Alert *alert.Webhook
	// MaxChars is the longest message, in characters, a post goes out
	// as. Zero means DefaultMaxChars.
	MaxChars int
	// Interval is how often Run looks at the inbox when not nudged, and
	// how long it waits after a failure. Zero means DefaultInterval.
	Interval time.Duration
	// UploadWait bounds how long BeforeUpload waits for a client's
	// earlier posts. Zero means DefaultUploadWait.
	UploadWait time.Duration
	// Users is the user list: a post mentions only the people on it.
	Users map[string]string

	// cards is held over each change to a progress card, which Run and
	// BeforeUpload make.
	cards sync.Mutex

	nudge     chan struct{}
	nudgeOnce sync.Once
	// stop is closed by Stop.
	stop     chan struct{}
	stopOnce sync.Once
}

func (p *Poster) maxChars() int {
	if p.MaxChars == 0 {
		return DefaultMaxChars
	}
	return p.MaxChars
}

func (p *Poster) interval() time.Duration {
	if p.Interval == 0 {
		return DefaultInterval
	}
	return p.Interval
}

func (p *Poster) nudgeCh() chan struct{} {
	p.nudgeOnce.Do(func() { p.nudge = make(chan struct{}, 1) })
	return p.nudge
}

// Nudge tells Run that the inbox has a new post. It never blocks.
func (p *Poster) Nudge() {
	select {
	case p.nudgeCh() <- struct{}{}:
	default:
	}
}

// Run posts the inbox until ctx is done. It must be called once.
func (p *Poster) Run(ctx context.Context) {
	for ctx.Err() == nil && !p.stopping() {
		if err := p.Pass(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("outbound: post", "err", err)
		}
		t := time.NewTimer(p.interval())
		select {
		case <-p.nudgeCh():
		case <-t.C:
		case <-ctx.Done():
		case <-p.stopCh():
		}
		t.Stop()
	}
}

func (p *Poster) stopCh() chan struct{} {
	p.stopOnce.Do(func() { p.stop = make(chan struct{}) })
	return p.stop
}

// Stop makes Run return once the post in hand, if any, is out as far as
// Slack takes it and that is recorded, taking no further one; unlike ctx
// it interrupts nothing. It is for handing the inbox to another process,
// which goes on from what is recorded. It may be called more than once.
func (p *Poster) Stop() {
	p.stopOnce.Do(func() { p.stop = make(chan struct{}) })
	select {
	case <-p.stop:
	default:
		close(p.stop)
	}
}

func (p *Poster) stopping() bool {
	select {
	case <-p.stopCh():
		return true
	default:
		return false
	}
}

// errPermanent marks a post that can never go out.
var errPermanent = errors.New("cannot be posted")

// Pass posts every undelivered post, oldest first, and stops at the first
// that Slack does not take, which stays in the inbox.
func (p *Poster) Pass(ctx context.Context) error {
	us, err := p.Store.Inbox.UndeliveredFrom(ctx)
	if err != nil {
		return err
	}
	for _, u := range us {
		if p.stopping() {
			return nil
		}
		if text, ok := alertText(u); ok {
			if err := p.relay(ctx, u.Client, text); err != nil {
				// Left in the inbox for the next pass; an alert has no
				// place in a thread's order, so posts behind it go on.
				slog.Warn("outbound: alert from a client", "msg_id", u.MsgID, "client", u.Client, "err", err)
				continue
			}
			if err := p.record(ctx, func() error { return p.Store.Inbox.MarkDelivered(ctx, u.MsgID) }); err != nil {
				return err
			}
			continue
		}
		err := p.post(ctx, u)
		if errors.Is(err, errPermanent) {
			slog.Error("outbound: dropped", "msg_id", u.MsgID, "client", u.Client, "err", err)
			if p.Alert != nil {
				if aerr := p.Alert.Send(ctx, fmt.Sprintf("post from %s dropped: msg_id %s: %v", u.Client, u.MsgID, err)); aerr != nil {
					slog.Warn("outbound: alert", "msg_id", u.MsgID, "err", aerr)
				}
			}
		} else if err != nil {
			return fmt.Errorf("msg_id %s: %w", u.MsgID, err)
		}
		if err := p.record(ctx, func() error { return p.Store.Inbox.MarkDelivered(ctx, u.MsgID) }); err != nil {
			return err
		}
	}
	return nil
}

// record runs write, a store write about something already in Slack,
// again every Interval until it succeeds or ctx is done: what is in Slack
// must be recorded before anything else is posted, or Run returns, so
// that no process posts it again. It returns ctx's error when it gives up.
func (p *Poster) record(ctx context.Context, write func() error) error {
	for {
		err := write()
		if err == nil {
			return nil
		}
		slog.Warn("outbound: record, retrying", "err", err)
		t := time.NewTimer(p.interval())
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		}
	}
}

// alertText returns the text of u if it is an alert a client raised.
func alertText(u store.Uplink) (string, bool) {
	var m payload.Message
	if json.Unmarshal(u.Payload, &m) != nil || m.Type != payload.Alert {
		return "", false
	}
	return m.Text, true
}

// relay sends an alert client raised to the webhook, as from client. With
// no webhook, it is only logged.
func (p *Poster) relay(ctx context.Context, client, text string) error {
	if p.Alert == nil {
		slog.Error("outbound: alert from a client, no webhook to send it to", "client", client, "text", text)
		return nil
	}
	w := *p.Alert
	w.From = client
	return w.Send(ctx, text)
}

// post posts u's parts that are not in Slack yet, recording each as it
// goes. An error wrapping errPermanent means u can never go out.
func (p *Poster) post(ctx context.Context, u store.Uplink) error {
	var m payload.Message
	if err := json.Unmarshal(u.Payload, &m); err != nil {
		return fmt.Errorf("%w: payload is not JSON: %v", errPermanent, err)
	}
	if m.Type != payload.Post && m.Type != payload.Progress {
		return fmt.Errorf("%w: payload type %q is not %q or %q", errPermanent, m.Type, payload.Post, payload.Progress)
	}
	channel, ts, ok := slack.ParseThreadKey(m.Thread)
	if !ok {
		return fmt.Errorf("%w: %q is not a thread key", errPermanent, m.Thread)
	}
	if m.Type == payload.Progress {
		return p.progress(ctx, channel, ts, m)
	}
	if strings.TrimSpace(m.Text) == "" {
		return fmt.Errorf("%w: empty text", errPermanent)
	}
	parts := Split(m.Text, p.maxChars())
	if m.Footer {
		footer := slack.MachineFooter(m.Text, u.Client)
		if !slack.FooterFits(footer) {
			return fmt.Errorf("%w: footer over %d characters", errPermanent, slack.MaxFooterChars)
		}
		parts = []string{footer}
	}
	mentions := p.mentions(u.MsgID, m)
	if err := p.close(ctx, m.Thread, slack.Done, "", nil); err != nil {
		return err
	}
	for i := u.PartsSent; i < len(parts); i++ {
		var posted string
		var err error
		switch {
		case m.Footer:
			posted, err = p.Slack.PostFooter(ctx, channel, ts, parts[i])
		case i == 0 && len(mentions) > 0:
			posted, err = p.Slack.PostReplyMentioning(ctx, channel, ts, parts[i], mentions)
		default:
			posted, err = p.Slack.PostReply(ctx, channel, ts, parts[i])
		}
		if errors.Is(err, slack.ErrNotFound) {
			return fmt.Errorf("%w: %v", errPermanent, err)
		}
		if err != nil {
			return err
		}
		if err := p.record(ctx, func() error { return p.Store.PartSent(ctx, u.MsgID, i, posted) }); err != nil {
			return err
		}
	}
	return nil
}

// mentions returns the people m mentions who are on the user list. The
// client checked them with the hub before it queued m; one who has left
// the list since is not mentioned, and m goes out all the same.
func (p *Poster) mentions(msgID string, m payload.Message) []string {
	if m.Footer {
		return nil
	}
	var users []string
	for _, u := range m.Mentions {
		if _, ok := p.Users[u]; !ok {
			slog.Warn("outbound: not mentioning a user who is not on the user list", "msg_id", msgID, "user", u)
			continue
		}
		users = append(users, u)
	}
	return users
}

// Split cuts text into parts of at most max characters, in order. A part
// ends at the last line break that leaves it at least half full, and
// otherwise at max characters.
func Split(text string, max int) []string {
	var parts []string
	for utf8.RuneCountInString(text) > max {
		// cut is the byte offset just past max characters.
		cut := len(text)
		n := 0
		for i := range text {
			if n == max {
				cut = i
				break
			}
			n++
		}
		if nl := strings.LastIndexByte(text[:cut], '\n'); nl >= 0 && utf8.RuneCountInString(text[:nl]) >= max/2 {
			cut = nl + 1
		}
		parts = append(parts, text[:cut])
		text = text[cut:]
	}
	return append(parts, text)
}

// progress sets the thread's progress card to m's, or closes it. A card
// to set replaces the open one, or is posted as a new one when none is
// open or the open one is gone from Slack.
func (p *Poster) progress(ctx context.Context, channel, ts string, m payload.Message) error {
	if m.Close != "" && m.Close != slack.Done && m.Close != slack.Failed {
		return fmt.Errorf("%w: close %q is not %q or %q", errPermanent, m.Close, slack.Done, slack.Failed)
	}
	if err := slack.CheckProgress(m.Title, m.Items, m.Close != ""); err != nil {
		return fmt.Errorf("%w: %v", errPermanent, err)
	}
	if m.Close != "" {
		return p.close(ctx, m.Thread, m.Close, m.Title, m.Items)
	}
	card := slack.Progress{Title: m.Title, Items: m.Items}
	b, err := json.Marshal(card)
	if err != nil {
		return err
	}
	p.cards.Lock()
	defer p.cards.Unlock()
	open, _, err := p.Store.OpenProgress(ctx, m.Thread)
	switch {
	case err == nil:
		err = p.Slack.UpdateProgress(ctx, channel, open, card)
		if err == nil {
			return p.record(ctx, func() error { return p.Store.SetProgress(ctx, m.Thread, open, b) })
		}
		if !errors.Is(err, slack.ErrNotFound) {
			return err
		}
	case !errors.Is(err, store.ErrNotFound):
		return err
	}
	posted, err := p.Slack.PostProgress(ctx, channel, ts, card)
	if errors.Is(err, slack.ErrNotFound) {
		return fmt.Errorf("%w: %v", errPermanent, err)
	}
	if err != nil {
		return err
	}
	return p.record(ctx, func() error { return p.Store.SetProgress(ctx, m.Thread, posted, b) })
}

// close closes the card open in thread, if any, with title and items in
// place of its own when given: the items still Doing become state,
// slack.Done or slack.Failed.
func (p *Poster) close(ctx context.Context, thread, state, title string, items []slack.ProgressItem) error {
	p.cards.Lock()
	defer p.cards.Unlock()
	ts, b, err := p.Store.OpenProgress(ctx, thread)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var card slack.Progress
	if err := json.Unmarshal(b, &card); err != nil {
		return err
	}
	if title != "" {
		card.Title = title
	}
	if items != nil {
		card.Items = items
	}
	card.Items = slices.Clone(card.Items)
	for i := range card.Items {
		if card.Items[i].State == slack.Doing {
			card.Items[i].State = state
		}
	}
	channel, _, _ := slack.ParseThreadKey(thread)
	if err := p.Slack.UpdateProgress(ctx, channel, ts, card); err != nil && !errors.Is(err, slack.ErrNotFound) {
		return err
	}
	return p.record(ctx, func() error { return p.Store.CloseProgress(ctx, thread) })
}

// BeforeUpload readies thread for an upload from client: it waits, at
// most UploadWait, until what client posted before is in Slack, then
// closes the thread's progress card.
func (p *Poster) BeforeUpload(ctx context.Context, client, thread string) error {
	wait := p.UploadWait
	if wait == 0 {
		wait = DefaultUploadWait
	}
	deadline := time.Now().Add(wait)
	upTo := int64(math.MaxInt64)
	for {
		last, err := p.Store.Inbox.LastUndelivered(ctx, client, upTo)
		if err != nil {
			return err
		}
		if last == 0 {
			break
		}
		upTo = last
		if time.Now().After(deadline) {
			return fmt.Errorf("what this machine posted before the upload is not in Slack after %v", wait)
		}
		t := time.NewTimer(uploadPoll)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		}
	}
	return p.close(ctx, thread, slack.Done, "", nil)
}
