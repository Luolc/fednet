package watch

import (
	"context"
	"log/slog"
	"sync"

	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
)

// Hourglass is the reaction the hub puts on a message a person posted
// while the message waits for an offline client.
const Hourglass = "hourglass_flowing_sand"

// backlog bounds the reactions waiting to be put on or taken off; past it
// they are skipped, with a warning.
const backlog = 1024

// Marks puts the hourglass on the messages people post for an offline
// client and takes it off once the client has them or they are dropped.
// The reactions are a hint: one that fails is logged and the message goes
// on as it would without.
type Marks struct {
	Store *store.Hub
	Slack slack.API
	// Online reports whether a client is online.
	Online func(client string) bool

	once sync.Once
	jobs chan func(context.Context)
}

func (m *Marks) init() { m.once.Do(func() { m.jobs = make(chan func(context.Context), backlog) }) }

// Run puts on and takes off the reactions, one at a time and in the order
// asked, until ctx is done. It must be called once.
func (m *Marks) Run(ctx context.Context) {
	m.init()
	for {
		select {
		case job := <-m.jobs:
			job(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (m *Marks) do(job func(context.Context)) {
	m.init()
	select {
	case m.jobs <- job:
	default:
		slog.Warn("watch: too many reactions waiting, one skipped")
	}
}

// Queued is inbound.Receiver.Queued: if client is offline, it puts the
// hourglass on the message at ts in channel, which is queued as msgID,
// and records that with the queued message. A message the client took
// meanwhile loses the hourglass again.
func (m *Marks) Queued(client, msgID, channel, ts string) {
	if m.Online(client) {
		return
	}
	m.do(func(ctx context.Context) {
		if err := m.Slack.React(ctx, channel, ts, Hourglass); err != nil {
			slog.Warn("watch: putting the hourglass on a queued message", "client", client, "channel", channel, "ts", ts, "err", err)
			return
		}
		mark := slack.ThreadKey(channel, ts)
		queued, err := m.Store.Outbox.Mark(ctx, msgID, mark)
		if err != nil {
			slog.Warn("watch: recording the hourglass on a queued message", "msg_id", msgID, "err", err)
		}
		if err != nil || !queued {
			m.unreact(ctx, mark)
		}
	})
}

// Done is link.Hub.Acked: it takes the hourglass off the messages marks
// name.
func (m *Marks) Done(marks []string) {
	for _, mark := range marks {
		m.do(func(ctx context.Context) { m.unreact(ctx, mark) })
	}
}

func (m *Marks) unreact(ctx context.Context, mark string) {
	channel, ts, ok := slack.ParseThreadKey(mark)
	if !ok {
		return
	}
	if err := m.Slack.Unreact(ctx, channel, ts, Hourglass); err != nil {
		slog.Warn("watch: taking the hourglass off a message", "channel", channel, "ts", ts, "err", err)
	}
}
