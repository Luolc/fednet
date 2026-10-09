package inbound

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"time"

	slackgo "github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"

	"github.com/Luolc/fednet/internal/slack"
)

// DefaultRetry is how long Run waits before trying a failed backfill, or a
// failed Connected, again.
const DefaultRetry = time.Minute

// Run keeps a Socket Mode connection to Slack up until ctx is done, hands
// every message event it brings to r.Handle, every click on an approval
// card to r.Click, every click on an upgrade card and every slash command
// to r.Commands, and acks each once that has returned without error, a
// slash command with its reply; other events and interactions are acked
// at once. Each time the
// connection comes up, r.Connected fixes where the backfill starts before
// any event of the connection is handled, and the backfill runs in the
// background, again after retry (DefaultRetry when zero) while it fails.
// appToken is the app-level token Socket Mode connects with. Run returns
// ctx.Err() when ctx is done, and earlier only when Slack rejects the
// token, which no retry fixes.
func Run(ctx context.Context, appToken string, r *Receiver, retry time.Duration) error {
	smc := socketmode.New(slackgo.New("", slackgo.OptionAppLevelToken(appToken)))
	return run(ctx, socketClient{smc}, r, retry)
}

// transport is what run needs of a socketmode.Client.
type transport interface {
	// RunContext keeps the connection up until ctx is done, sending what
	// happens to the Events channel.
	RunContext(ctx context.Context) error
	Events() <-chan socketmode.Event
	// Ack acks the request with envelopeID, with payload as the response
	// when that is not nil.
	Ack(ctx context.Context, envelopeID string, payload any) error
}

type socketClient struct{ *socketmode.Client }

func (c socketClient) Events() <-chan socketmode.Event { return c.Client.Events }

func (c socketClient) Ack(ctx context.Context, envelopeID string, payload any) error {
	return c.AckCtx(ctx, envelopeID, payload)
}

func run(ctx context.Context, t transport, r *Receiver, retry time.Duration) error {
	if retry == 0 {
		retry = DefaultRetry
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	r.Disconnected()
	ran := make(chan error, 1)
	go func() { ran <- t.RunContext(ctx) }()
	var backfill backfill
	defer backfill.stop()
	for {
		select {
		case err := <-ran:
			return err
		case ev := <-t.Events():
			switch ev.Type {
			case socketmode.EventTypeConnected:
				// The backfill of the connection before has no more to
				// do: what it had not read, this one's reads. Connected
				// must have fixed the backfill's start before the
				// connection's first message is handled; without the
				// database nothing can be handled anyway.
				backfill.stop()
				for r.Connected(ctx) != nil {
					if err := wait(ctx, retry); err != nil {
						return err
					}
				}
				backfill.start(ctx, r, retry)
			case socketmode.EventTypeConnecting, socketmode.EventTypeConnectionError:
				r.Disconnected()
			case socketmode.EventTypeInvalidAuth, socketmode.EventTypeIncomingError, socketmode.EventTypeErrorBadMessage, socketmode.EventTypeErrorWriteFailed:
				slog.Warn("inbound: socket mode", "event", ev.Type, "data", ev.Data)
			case socketmode.EventTypeEventsAPI:
				if ev.Request == nil {
					continue
				}
				if err := r.handleEventsAPI(ctx, ev.Data); err != nil {
					slog.Warn("inbound: event not acked", "err", err)
					continue
				}
				if err := t.Ack(ctx, ev.Request.EnvelopeID, nil); err != nil {
					slog.Warn("inbound: ack", "err", err)
				}
			case socketmode.EventTypeInteractive:
				if ev.Request == nil {
					continue
				}
				if err := r.handleInteractive(ctx, ev.Data); err != nil {
					slog.Warn("inbound: interaction not acked", "err", err)
					continue
				}
				if err := t.Ack(ctx, ev.Request.EnvelopeID, nil); err != nil {
					slog.Warn("inbound: ack", "err", err)
				}
			case socketmode.EventTypeSlashCommand:
				if ev.Request == nil {
					continue
				}
				reply, err := r.handleCommand(ctx, ev.Data)
				if err != nil {
					slog.Warn("inbound: command not acked", "err", err)
					continue
				}
				if err := t.Ack(ctx, ev.Request.EnvelopeID, slack.CommandAck(reply)); err != nil {
					slog.Warn("inbound: ack", "err", err)
				}
			}
		}
	}
}

// handleEventsAPI hands a message event to r.Handle; any other Events API
// event is not the hub's business.
func (r *Receiver) handleEventsAPI(ctx context.Context, data any) error {
	e, ok := data.(slackevents.EventsAPIEvent)
	if !ok {
		return nil
	}
	m, ok := e.InnerEvent.Data.(*slackevents.MessageEvent)
	if !ok {
		return nil
	}
	cb, ok := e.Data.(*slackevents.EventsAPICallbackEvent)
	if !ok || cb.EventID == "" {
		return fmt.Errorf("inbound: message event %s/%s without an event id", m.Channel, m.TimeStamp)
	}
	ev := Event{ID: cb.EventID, Channel: m.Channel, IM: m.IsIM()}
	ev.TS, ev.User, ev.Text, ev.ThreadTS = m.TimeStamp, m.User, m.Text, m.ThreadTimeStamp
	ev.BotID, ev.SubType = m.BotID, m.SubType
	if m.Message != nil {
		for _, f := range m.Message.Files {
			ev.Files = append(ev.Files, slack.File{ID: f.ID, Name: f.Name, Mimetype: f.Mimetype, Size: f.Size, URL: f.Permalink})
		}
	}
	return r.Handle(ctx, ev)
}

// handleInteractive hands each press on an approval card's button, told
// by its action id and the block id the card gives its buttons, to
// r.Click, and each press on an upgrade card's button to r.Commands; any
// other interaction is not the hub's business.
func (r *Receiver) handleInteractive(ctx context.Context, data any) error {
	cb, ok := data.(slackgo.InteractionCallback)
	if !ok || cb.Type != slackgo.InteractionTypeBlockActions {
		return nil
	}
	// The container names the message the buttons are on; older payloads
	// name it at the top level instead.
	channel, ts := cmp.Or(cb.Container.ChannelID, cb.Channel.ID), cmp.Or(cb.Container.MessageTs, cb.Message.Timestamp)
	for _, a := range cb.ActionCallback.BlockActions {
		c := slack.Click{ID: a.Value, User: cb.User.ID, Bot: cb.User.IsBot, Channel: channel, TS: ts, ResponseURL: cb.ResponseURL}
		var err error
		switch {
		case (a.ActionID == slack.ApproveAction || a.ActionID == slack.RejectAction) && a.BlockID == slack.CardBlockID(a.Value):
			c.Approve = a.ActionID == slack.ApproveAction
			err = r.Click(ctx, c)
		case (a.ActionID == slack.UpgradeConfirmAction || a.ActionID == slack.UpgradeCancelAction) && a.BlockID == slack.UpgradeBlockID(a.Value):
			c.Approve = a.ActionID == slack.UpgradeConfirmAction
			err = r.commander().Click(ctx, c)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// handleCommand answers a slash command through r.Commands; anything
// else is acked with no reply.
func (r *Receiver) handleCommand(ctx context.Context, data any) (slack.CommandReply, error) {
	cmd, ok := data.(slackgo.SlashCommand)
	if !ok {
		return slack.CommandReply{}, nil
	}
	return r.commander().Command(ctx, slack.Command{Name: cmd.Command, Text: cmd.Text, User: cmd.UserID, Channel: cmd.ChannelID, ResponseURL: cmd.ResponseURL})
}

// commander returns r.Commands, or, when there is none, one that says so.
func (r *Receiver) commander() Commander {
	if r.Commands == nil {
		return noCommands{}
	}
	return r.Commands
}

// noCommands is the Commander of a hub that serves no commands: it says
// so to whoever sends one, and drops the clicks.
type noCommands struct{}

func (noCommands) Command(context.Context, slack.Command) (slack.CommandReply, error) {
	return slack.CommandReply{Text: "这个 hub 不处理命令"}, nil
}

func (noCommands) Click(_ context.Context, c slack.Click) error {
	slog.Warn("inbound: click on an upgrade card, but the hub serves no commands", "id", c.ID, "user", c.User)
	return nil
}

// backfill is the backfill goroutine run keeps: one at a time, the last
// one stopped when a new one starts.
type backfill struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func (b *backfill) start(ctx context.Context, r *Receiver, retry time.Duration) {
	b.stop()
	ctx, b.cancel = context.WithCancel(ctx)
	b.done = make(chan struct{})
	go func() {
		defer close(b.done)
		backfillUntilDone(ctx, r, retry)
	}()
}

func (b *backfill) stop() {
	if b.cancel != nil {
		b.cancel()
		<-b.done
	}
}

// backfillUntilDone runs r.Backfill until it succeeds or ctx is done,
// waiting retry between failures.
func backfillUntilDone(ctx context.Context, r *Receiver, retry time.Duration) {
	for {
		err := r.Backfill(ctx)
		if err == nil || ctx.Err() != nil {
			return
		}
		slog.Warn("inbound: backfill failed, retrying", "retry_in", retry, "err", err)
		if wait(ctx, retry) != nil {
			return
		}
	}
}

// wait sleeps for d, or until ctx is done.
func wait(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
