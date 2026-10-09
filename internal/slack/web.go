package slack

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	slackgo "github.com/slack-go/slack"

	"github.com/Luolc/fednet/internal/alert"
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
	// http posts to response URLs, which take no token.
	http *http.Client
	// sleep waits for d or until ctx is done; tests replace it.
	sleep func(ctx context.Context, d time.Duration) error
}

// New returns a Web that calls Slack with token, a bot token.
func New(token string) *Web {
	return &Web{c: slackgo.New(token), http: &http.Client{Timeout: responseTimeout}, sleep: sleep}
}

// responseTimeout bounds one post to a response URL.
const responseTimeout = 10 * time.Second

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
// carries its own ts as thread_ts, which here means "not a reply". A
// message a machine posted (see PostReply) has the machine's name in a
// context block before the text; that is read back as Machine.
func message(m slackgo.Message) Message {
	out := Message{TS: m.Timestamp, User: m.User, Text: m.Text, BotID: m.BotID, SubType: m.SubType, Machine: machine(m)}
	if m.ThreadTimestamp != m.Timestamp {
		out.ThreadTS = m.ThreadTimestamp
	}
	for _, f := range m.Files {
		out.Files = append(out.Files, File{Name: f.Name, Mimetype: f.Mimetype, Size: f.Size, URL: f.Permalink})
	}
	return out
}

// machine returns the machine named in m's first block when that is a
// context block holding one plain text, the layout PostReply posts with,
// and "" otherwise.
func machine(m slackgo.Message) string {
	if len(m.Blocks.BlockSet) < 2 {
		return ""
	}
	c, ok := m.Blocks.BlockSet[0].(*slackgo.ContextBlock)
	if !ok || len(c.ContextElements.Elements) != 1 {
		return ""
	}
	if t, ok := c.ContextElements.Elements[0].(*slackgo.TextBlockObject); ok && t.Type == slackgo.PlainTextType {
		return t.Text
	}
	return ""
}

// Self asks auth.test which user the token belongs to.
func (w *Web) Self(ctx context.Context) (string, error) {
	var res *slackgo.AuthTestResponse
	err := w.call(ctx, "auth.test", func() (err error) {
		res, err = w.c.AuthTestContext(ctx)
		return err
	})
	if err != nil {
		return "", err
	}
	return res.UserID, nil
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

// PostCard posts the card's blocks; the summary is the message's plain
// text, for notifications.
func (w *Web) PostCard(ctx context.Context, channel string, c Card) (string, error) {
	var ts string
	err := w.call(ctx, "chat.postMessage", func() (err error) {
		_, ts, err = w.c.PostMessageContext(ctx, channel, slackgo.MsgOptionText(c.Summary, false), slackgo.MsgOptionBlocks(blocks(c)...))
		return err
	})
	return ts, err
}

func (w *Web) UpdateCard(ctx context.Context, channel, ts string, c Card) error {
	return w.call(ctx, "chat.update", func() error {
		_, _, _, err := w.c.UpdateMessageContext(ctx, channel, ts, slackgo.MsgOptionText(c.Summary, false), slackgo.MsgOptionBlocks(blocks(c)...))
		return err
	})
}

func (w *Web) Whisper(ctx context.Context, channel, user, text string) error {
	return w.call(ctx, "chat.postEphemeral", func() error {
		_, err := w.c.PostEphemeralContext(ctx, channel, user, slackgo.MsgOptionText(text, false))
		return err
	})
}

// Respond posts text through responseURL, replacing the message there,
// as an ephemeral message.
// The URL lets whoever has it post in the sender's place for a while,
// so the error never carries it.
func (w *Web) Respond(ctx context.Context, responseURL, text string) error {
	err := w.call(ctx, "response_url", func() error {
		return slackgo.PostWebhookCustomHTTPContext(ctx, responseURL, w.http, &slackgo.WebhookMessage{
			Text: text, ResponseType: "ephemeral", ReplaceOriginal: true,
		})
	})
	if err != nil {
		return errors.New(alert.Redact(responseURL, err.Error()))
	}
	return nil
}

// CommandAck is the payload a slash command is acked with: r, as an
// ephemeral message.
func CommandAck(r CommandReply) map[string]any {
	ack := map[string]any{"response_type": "ephemeral", "text": r.Text}
	if r.Card != nil {
		ack["text"] = "从 " + r.Card.From + " 升到 " + r.Card.To + "？"
		ack["blocks"] = upgradeBlocks(*r.Card)
	}
	return ack
}

// upgradeBlocks lays the upgrade card out: the question, the clients,
// then the two buttons.
func upgradeBlocks(c UpgradeCard) []slackgo.Block {
	plain := func(s string) *slackgo.TextBlockObject {
		return slackgo.NewTextBlockObject(slackgo.PlainTextType, s, false, false)
	}
	bs := []slackgo.Block{
		slackgo.NewSectionBlock(plain("从 "+c.From+" 升到 "+c.To+"？先升在线的 client，都升好了再升 hub。"), nil, nil),
	}
	if len(c.Clients) > 0 {
		bs = append(bs, slackgo.NewSectionBlock(plain(strings.Join(c.Clients, "\n")), nil, nil))
	}
	return append(bs, slackgo.NewActionBlock(UpgradeBlockID(c.ID),
		slackgo.NewButtonBlockElement(UpgradeConfirmAction, c.ID, plain("升级")).WithStyle(slackgo.StylePrimary),
		slackgo.NewButtonBlockElement(UpgradeCancelAction, c.ID, plain("取消")),
	))
}

// blocks lays the card out: a header, the summary, the parameters in
// preformatted blocks (one each chunk of ParamBlocks, which the caller
// has checked fit), who asks and when it expires, then the buttons while
// it waits or how it ended once decided. What the agent wrote (summary,
// parameters, agent, requester) goes in plain text and preformatted
// blocks, which Slack shows as they are; only the card's own labels, the
// approver (from the hub's list) and the dates use Slack's markup. Times
// are Slack date tokens, which each reader sees in their own time zone.
func blocks(c Card) []slackgo.Block {
	plain := func(s string) *slackgo.TextBlockObject {
		return slackgo.NewTextBlockObject(slackgo.PlainTextType, s, false, false)
	}
	mrkdwn := func(s string) *slackgo.TextBlockObject {
		return slackgo.NewTextBlockObject(slackgo.MarkdownType, s, false, false)
	}
	bs := []slackgo.Block{
		slackgo.NewHeaderBlock(plain("审批请求")),
		slackgo.NewSectionBlock(mrkdwn("*做什么*"), nil, nil),
		slackgo.NewSectionBlock(plain(c.Summary), nil, nil),
		slackgo.NewSectionBlock(mrkdwn("*参数*"), nil, nil),
	}
	chunks, _ := ParamBlocks(c.Params)
	for i, chunk := range chunks {
		bs = append(bs, slackgo.NewRichTextBlock(fmt.Sprintf("params:%d", i),
			&slackgo.RichTextPreformatted{Type: slackgo.RTEPreformatted, Elements: []slackgo.RichTextSectionElement{slackgo.NewRichTextSectionTextElement(chunk, nil)}}))
	}
	who := []*slackgo.TextBlockObject{mrkdwn("*机器* (hub 认证)"), plain(c.Machine), mrkdwn("*agent* (自报)"), plain(c.Agent)}
	if c.Requester != "" {
		who = append(who, mrkdwn("*代谁* (agent 自报，不参与校验)"), plain(c.Requester))
	}
	bs = append(bs,
		slackgo.NewSectionBlock(nil, who, nil),
		slackgo.NewSectionBlock(nil, []*slackgo.TextBlockObject{mrkdwn("*过期*\n" + date(c.Expires)), mrkdwn("*编号*\n" + c.ID)}, nil),
	)
	if c.Outcome == "" {
		return append(bs, slackgo.NewActionBlock(CardBlockID(c.ID),
			slackgo.NewButtonBlockElement(ApproveAction, c.ID, plain("批准")).WithStyle(slackgo.StylePrimary),
			slackgo.NewButtonBlockElement(RejectAction, c.ID, plain("拒绝")).WithStyle(slackgo.StyleDanger),
		))
	}
	return append(bs, slackgo.NewSectionBlock(mrkdwn(outcomeText(c)), nil, nil))
}

// outcomeText says how a decided card ended, in one line.
func outcomeText(c Card) string {
	switch c.Outcome {
	case "approved":
		return "✅ *已批准* <@" + c.Approver + "> " + date(c.DecidedAt)
	case "rejected":
		return "⛔ *已拒绝* <@" + c.Approver + "> " + date(c.DecidedAt)
	default:
		return "⌛ *已过期* " + date(c.DecidedAt)
	}
}

// date writes t as a Slack date token, with an RFC 3339 fallback.
func date(t time.Time) string {
	return fmt.Sprintf("<!date^%d^{date_short_pretty} {time}|%s>", t.Unix(), t.UTC().Format(time.RFC3339))
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
