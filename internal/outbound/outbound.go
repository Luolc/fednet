// Package outbound posts to Slack what the clients posted: the hub's inbox
// holds each post until it is in Slack. Posts go out in the order they
// reached the hub, each in its thread, under a line naming the machine it
// came from. A post longer than MaxChars is split into consecutive
// messages in the same thread.
//
// A post Slack does not take is left in the inbox and tried again later,
// after the Slack API's own waits on rate limits; nothing behind it goes
// out first, so a thread's messages stay in order. A post that can never
// go out (its payload is not a post, its thread is not in Slack) is
// alerted, logged and marked delivered, so it does not hold up the rest.
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
)

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

	nudge     chan struct{}
	nudgeOnce sync.Once
	// sent counts the parts of a split post already in Slack, by msg_id,
	// so a retry does not post them again. It lives in memory: a hub that
	// restarts in the middle of a post posts its first parts again.
	sent map[string]int
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
	for ctx.Err() == nil {
		if err := p.Pass(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("outbound: post", "err", err)
		}
		t := time.NewTimer(p.interval())
		select {
		case <-p.nudgeCh():
		case <-t.C:
		case <-ctx.Done():
		}
		t.Stop()
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
		if text, ok := alertText(u); ok {
			if err := p.relay(ctx, u.Client, text); err != nil {
				// Left in the inbox for the next pass; an alert has no
				// place in a thread's order, so posts behind it go on.
				slog.Warn("outbound: alert from a client", "msg_id", u.MsgID, "client", u.Client, "err", err)
				continue
			}
			if err := p.Store.Inbox.MarkDelivered(ctx, u.MsgID); err != nil {
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
		if err := p.Store.Inbox.MarkDelivered(ctx, u.MsgID); err != nil {
			return err
		}
		delete(p.sent, u.MsgID)
	}
	return nil
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

// post posts u's parts that are not in Slack yet. An error wrapping
// errPermanent means u can never go out.
func (p *Poster) post(ctx context.Context, u store.Uplink) error {
	var m payload.Message
	if err := json.Unmarshal(u.Payload, &m); err != nil {
		return fmt.Errorf("%w: payload is not JSON: %v", errPermanent, err)
	}
	if m.Type != payload.Post {
		return fmt.Errorf("%w: payload type %q is not %q", errPermanent, m.Type, payload.Post)
	}
	channel, ts, ok := slack.ParseThreadKey(m.Thread)
	if !ok {
		return fmt.Errorf("%w: %q is not a thread key", errPermanent, m.Thread)
	}
	if strings.TrimSpace(m.Text) == "" {
		return fmt.Errorf("%w: empty text", errPermanent)
	}
	if p.sent == nil {
		p.sent = make(map[string]int)
	}
	parts := Split(m.Text, p.maxChars())
	for i := p.sent[u.MsgID]; i < len(parts); i++ {
		_, err := p.Slack.PostReply(ctx, channel, ts, u.Client, parts[i])
		if errors.Is(err, slack.ErrNotFound) {
			return fmt.Errorf("%w: %v", errPermanent, err)
		}
		if err != nil {
			return err
		}
		p.sent[u.MsgID] = i + 1
	}
	return nil
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
