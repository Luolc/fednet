package slack

import (
	"context"
	"errors"
	"fmt"
	"time"

	slackgo "github.com/slack-go/slack"
)

// maxRetries is how many times a call that Slack rate-limits is retried,
// each after the wait Slack asks for in Retry-After, before it fails.
const maxRetries = 3

// maxWait is the longest Retry-After a call waits out; a longer one fails
// the call at once, so a call cannot wait without bound even when its
// context has no deadline.
const maxWait = time.Minute

// Web is the API backed by Slack's Web API. It calls Slack with the bot
// token it was made with, and writes no logs.
type Web struct {
	c *slackgo.Client
	// sleep waits for d or until ctx is done; tests replace it.
	sleep func(ctx context.Context, d time.Duration) error
}

// New returns a Web that calls Slack with token, a bot token.
func New(token string) *Web {
	return &Web{c: slackgo.New(token), sleep: sleep}
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// call runs f, the Web API method named method, again while Slack
// rate-limits it, at most maxRetries times and each time for at most
// maxWait. A request Slack rate-limits is
// not carried out, so running f again does not post twice.
func (w *Web) call(ctx context.Context, method string, f func() error) error {
	for retries := 0; ; retries++ {
		err := f()
		var limited *slackgo.RateLimitedError
		if !errors.As(err, &limited) || retries == maxRetries || limited.RetryAfter > maxWait {
			return wrap(method, err)
		}
		if err := w.sleep(ctx, limited.RetryAfter); err != nil {
			return fmt.Errorf("slack: %s: waiting out a rate limit: %w", method, err)
		}
	}
}

// wrap names method in err, and turns Slack's errors for an unknown
// channel or thread into ErrNotFound.
func wrap(method string, err error) error {
	var slackErr slackgo.SlackErrorResponse
	switch {
	case err == nil:
		return nil
	case errors.As(err, &slackErr) && (slackErr.Err == "channel_not_found" || slackErr.Err == "thread_not_found"):
		return fmt.Errorf("%w: %s: %s", ErrNotFound, method, slackErr.Err)
	default:
		return fmt.Errorf("slack: %s: %w", method, err)
	}
}

func (w *Web) Replies(ctx context.Context, channel, ts string) ([]Message, error) {
	p := &slackgo.GetConversationRepliesParameters{ChannelID: channel, Timestamp: ts}
	var ms []Message
	for {
		var page []slackgo.Message
		var next string
		err := w.call(ctx, "conversations.replies", func() (err error) {
			page, _, next, err = w.c.GetConversationRepliesContext(ctx, p)
			return err
		})
		if err != nil {
			return nil, err
		}
		for _, m := range page {
			ms = append(ms, Message{TS: m.Timestamp, User: m.User, Text: m.Text})
		}
		if next == "" {
			return ms, nil
		}
		p.Cursor = next
	}
}

func (w *Web) Post(ctx context.Context, channel, text string) (string, error) {
	var ts string
	err := w.call(ctx, "chat.postMessage", func() (err error) {
		_, ts, err = w.c.PostMessageContext(ctx, channel, slackgo.MsgOptionText(text, false))
		return err
	})
	return ts, err
}

func (w *Web) Purpose(ctx context.Context, channel string) (string, error) {
	var c *slackgo.Channel
	err := w.call(ctx, "conversations.info", func() (err error) {
		c, err = w.c.GetConversationInfoContext(ctx, &slackgo.GetConversationInfoInput{ChannelID: channel})
		return err
	})
	if err != nil {
		return "", err
	}
	return c.Purpose.Value, nil
}

func (w *Web) SetPurpose(ctx context.Context, channel, purpose string) error {
	return w.call(ctx, "conversations.setPurpose", func() error {
		_, err := w.c.SetPurposeOfConversationContext(ctx, channel, purpose)
		return err
	})
}

// DM opens the direct message conversation with user, or finds the one
// already open, and posts text in it.
func (w *Web) DM(ctx context.Context, user, text string) error {
	var c *slackgo.Channel
	err := w.call(ctx, "conversations.open", func() (err error) {
		c, _, _, err = w.c.OpenConversationContext(ctx, &slackgo.OpenConversationParameters{Users: []string{user}})
		return err
	})
	if err != nil {
		return err
	}
	_, err = w.Post(ctx, c.ID, text)
	return err
}
