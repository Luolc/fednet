package inbound

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	slackgo "github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"

	"github.com/Luolc/fednet/internal/slack"
)

// DefaultRetry is how long Run waits before trying a failed backfill again.
const DefaultRetry = time.Minute

// Run keeps a Socket Mode connection to Slack up until ctx is done, hands
// every message event it brings to r.Handle and acks it once that has
// returned without error; other events are acked at once. Each time the
// connection comes up, a backfill runs in the background, again after
// retry (DefaultRetry when zero) while it fails. appToken is the app-level
// token Socket Mode connects with. Run returns ctx.Err() when ctx is done,
// and earlier only when Slack rejects the token, which no retry fixes.
func Run(ctx context.Context, appToken string, r *Receiver, retry time.Duration) error {
	if retry == 0 {
		retry = DefaultRetry
	}
	smc := socketmode.New(slackgo.New("", slackgo.OptionAppLevelToken(appToken)))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	r.Disconnected()
	ran := make(chan error, 1)
	go func() { ran <- smc.RunContext(ctx) }()
	var backfill backfill
	defer backfill.stop()
	for {
		select {
		case err := <-ran:
			return err
		case ev := <-smc.Events:
			switch ev.Type {
			case socketmode.EventTypeConnected:
				r.Connected()
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
				if err := smc.AckCtx(ctx, ev.Request.EnvelopeID, nil); err != nil {
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
			ev.Files = append(ev.Files, slack.File{Name: f.Name, URL: f.Permalink})
		}
	}
	return r.Handle(ctx, ev)
}

// backfill is the backfill goroutine Run keeps: one at a time, the last
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
		t := time.NewTimer(retry)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return
		}
	}
}
