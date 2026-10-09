package slack

import (
	"context"
	"errors"
	"fmt"
	"slices"
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

// notFound is the Slack errors that mean ErrNotFound.
var notFound = map[string]bool{"channel_not_found": true, "thread_not_found": true, "message_not_found": true}

// wrap names method in err, and turns Slack's errors for an unknown
// channel, thread or message into ErrNotFound.
func wrap(method string, err error) error {
	var slackErr slackgo.SlackErrorResponse
	switch {
	case err == nil:
		return nil
	case errors.As(err, &slackErr) && notFound[slackErr.Err]:
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
			ms = append(ms, message(m))
		}
		if next == "" {
			return ms, nil
		}
		p.Cursor = next
	}
}

// message converts a message as Slack returns it. A thread's first message
// carries its own ts as thread_ts, which here means "not a reply".
func message(m slackgo.Message) Message {
	out := Message{TS: m.Timestamp, User: m.User, Text: m.Text, BotID: m.BotID, SubType: m.SubType}
	if m.ThreadTimestamp != m.Timestamp {
		out.ThreadTS = m.ThreadTimestamp
	}
	for _, f := range m.Files {
		out.Files = append(out.Files, File{Name: f.Name, URL: f.Permalink})
	}
	return out
}

// History pages through conversations.history, which Slack returns newest
// first, and puts the result oldest first.
func (w *Web) History(ctx context.Context, channel, oldest string) ([]Message, error) {
	p := &slackgo.GetConversationHistoryParameters{ChannelID: channel, Oldest: oldest}
	var ms []Message
	for {
		var res *slackgo.GetConversationHistoryResponse
		err := w.call(ctx, "conversations.history", func() (err error) {
			res, err = w.c.GetConversationHistoryContext(ctx, p)
			return err
		})
		if err != nil {
			return nil, err
		}
		for _, m := range res.Messages {
			ms = append(ms, message(m))
		}
		if !res.HasMore || res.ResponseMetaData.NextCursor == "" {
			slices.Reverse(ms)
			return ms, nil
		}
		p.Cursor = res.ResponseMetaData.NextCursor
	}
}

// Conversations pages through users.conversations for the bot's own
// channels, private channels and direct messages.
func (w *Web) Conversations(ctx context.Context) ([]Conversation, error) {
	p := &slackgo.GetConversationsForUserParameters{Types: []string{"public_channel", "private_channel", "im"}}
	var cs []Conversation
	for {
		var page []slackgo.Channel
		var next string
		err := w.call(ctx, "users.conversations", func() (err error) {
			page, next, err = w.c.GetConversationsForUserContext(ctx, p)
			return err
		})
		if err != nil {
			return nil, err
		}
		for _, c := range page {
			cs = append(cs, Conversation{ID: c.ID, IM: c.IsIM})
		}
		if next == "" {
			return cs, nil
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

// PostReply puts machine in a context block, the small grey line Slack
// shows above the text, and text in a markdown block. text is also the
// message's plain text, which notifications and conversations.replies
// show.
func (w *Web) PostReply(ctx context.Context, channel, ts, machine, text string) (string, error) {
	blocks := slackgo.MsgOptionBlocks(
		slackgo.NewContextBlock("", slackgo.NewTextBlockObject(slackgo.PlainTextType, machine, false, false)),
		slackgo.NewMarkdownBlock("", text),
	)
	var posted string
	err := w.call(ctx, "chat.postMessage", func() (err error) {
		_, posted, err = w.c.PostMessageContext(ctx, channel, slackgo.MsgOptionTS(ts), slackgo.MsgOptionText(text, false), blocks)
		return err
	})
	return posted, err
}

func (w *Web) Delete(ctx context.Context, channel, ts string) error {
	return w.call(ctx, "chat.delete", func() error {
		_, _, err := w.c.DeleteMessageContext(ctx, channel, ts)
		return err
	})
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
