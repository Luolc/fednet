// Package watch raises the hub's alerts: when the hub has been cut off from
// Slack too long, and when a machine is offline while messages for it have
// waited too long. An alert goes to the alerts webhook once for each
// occurrence; only after the trouble has cleared does it alert again.
package watch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Luolc/fednet/internal/alert"
	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
)

// SlackLink is the hub's connection to Slack, as far as the watch needs it.
type SlackLink interface {
	// DownFor returns how long the hub has been without a connection to
	// Slack, zero while it has one.
	DownFor() time.Duration
}

// Defaults for the zero fields of Watch.
const (
	DefaultSlackDown = 5 * time.Minute
	DefaultOffline   = 10 * time.Minute
	DefaultInterval  = 30 * time.Second
)

// Watch checks for the hub's troubles. Run checks until its context is
// done.
type Watch struct {
	Store *store.Hub
	// Slack is where a machine's affected threads are told it is offline.
	Slack slack.API
	// Link is the hub's connection to Slack.
	Link SlackLink
	// Online reports whether a client is online.
	Online func(client string) bool
	// Alert gets the alerts; it must be set.
	Alert *alert.Webhook
	// SlackDown is how long the hub may be cut off from Slack before an
	// alert. Zero means DefaultSlackDown.
	SlackDown time.Duration
	// Offline is how long a message may wait for an offline client before
	// an alert. Zero means DefaultOffline.
	Offline time.Duration
	// Interval is how often Run checks. Zero means DefaultInterval.
	Interval time.Duration

	// slackAlerted is set once the current loss of Slack is alerted.
	slackAlerted bool
	// offline holds the clients whose current absence is alerted, each with
	// the threads already told.
	offline map[string]map[string]bool
}

func or(d, def time.Duration) time.Duration {
	if d == 0 {
		return def
	}
	return d
}

// Run checks every Interval until ctx is done. It must be called once.
func (w *Watch) Run(ctx context.Context) {
	t := time.NewTicker(or(w.Interval, DefaultInterval))
	defer t.Stop()
	for {
		if err := w.Check(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("watch: check", "err", err)
		}
		select {
		case <-t.C:
		case <-ctx.Done():
			return
		}
	}
}

// Check looks for the troubles once and raises the alerts not raised yet.
// An alert that fails to go out is tried again on the next Check.
func (w *Watch) Check(ctx context.Context) error {
	return errors.Join(w.checkSlack(ctx), w.checkClients(ctx))
}

func (w *Watch) checkSlack(ctx context.Context) error {
	limit := or(w.SlackDown, DefaultSlackDown)
	down := w.Link.DownFor()
	if down < limit {
		w.slackAlerted = false
		return nil
	}
	if w.slackAlerted {
		return nil
	}
	if err := w.Alert.Send(ctx, fmt.Sprintf("the hub has been cut off from Slack for %v", down.Round(time.Second))); err != nil {
		return err
	}
	w.slackAlerted = true
	return nil
}

func (w *Watch) checkClients(ctx context.Context) error {
	limit := or(w.Offline, DefaultOffline)
	clients, err := w.Store.Clients(ctx)
	if err != nil {
		return err
	}
	if w.offline == nil {
		w.offline = make(map[string]map[string]bool)
	}
	var errs []error
	for _, c := range clients {
		waiting := false
		if !w.Online(c) {
			if waiting, err = w.Store.Outbox.QueuedFor(ctx, c, limit); err != nil {
				errs = append(errs, err)
				continue
			}
		}
		if !waiting {
			delete(w.offline, c)
			continue
		}
		if w.offline[c] == nil {
			if err := w.Alert.Send(ctx, fmt.Sprintf("%s is offline and messages for it have waited over %v", c, limit)); err != nil {
				errs = append(errs, err)
				continue
			}
			w.offline[c] = make(map[string]bool)
		}
		errs = append(errs, w.tellThreads(ctx, c, limit))
	}
	return errors.Join(errs...)
}

// tellThreads says in each thread with a message queued for client, once,
// that client is offline. A thread it fails to tell is tried again on the
// next Check.
func (w *Watch) tellThreads(ctx context.Context, client string, limit time.Duration) error {
	ds, err := w.Store.Outbox.After(ctx, client, 0)
	if err != nil {
		return err
	}
	told := w.offline[client]
	var errs []error
	for _, d := range ds {
		var m payload.Message
		if json.Unmarshal(d.Payload, &m) != nil || told[m.Thread] {
			continue
		}
		channel, ts, ok := slack.ParseThreadKey(m.Thread)
		if !ok {
			continue
		}
		text := fmt.Sprintf("%s is offline; messages for it have waited over %v and will reach it when it is back.", client, limit)
		if _, err := w.Slack.PostReply(ctx, channel, ts, "fednet", text); err != nil && !errors.Is(err, slack.ErrNotFound) {
			errs = append(errs, fmt.Errorf("tell thread %s: %w", m.Thread, err))
			continue
		}
		told[m.Thread] = true
	}
	return errors.Join(errs...)
}
