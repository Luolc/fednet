// Package route decides which client gets a message a person posted in a
// Slack thread, and queues it there.
package route

import (
	"context"
	"errors"

	"github.com/Luolc/fednet/internal/store"
)

// ErrNoMachine is returned for a new thread in a channel that has no
// default machine. The message is not queued anywhere.
var ErrNoMachine = errors.New("route: no machine takes this channel")

// ErrNoOwner is returned for a reply in a thread that has no owner, such as
// one started before fednet. The message is not queued anywhere.
var ErrNoOwner = errors.New("route: thread has no owner")

// Config is the routing part of the hub's configuration.
type Config struct {
	// Defaults maps a channel to the client that takes its new threads.
	Defaults map[string]string
	// DM is the client that takes new threads in direct messages, where
	// every message that is not a reply starts a thread; empty when none
	// does.
	DM string
}

// Router queues the messages people post in threads. A message is queued
// whether or not its client is connected; it waits in the outbox until the
// client takes it.
type Router struct {
	hub *store.Hub
	cfg Config
}

// New returns a Router that queues into hub's outbox. hub may be bound to
// a transaction (see store.Hub.ReceiveSlack); the Router then queues in it.
func New(hub *store.Hub, cfg Config) *Router {
	return &Router{hub: hub, cfg: cfg}
}

// RouteNew queues payload, the message that starts thread in channel, for
// the channel's default machine, which becomes the thread's owner, with
// channelName, the channel's name, recorded along. If thread already has an
// owner, payload goes to that owner instead. It returns the client the
// message was queued for.
func (r *Router) RouteNew(ctx context.Context, channel, channelName, thread string, payload []byte) (string, error) {
	return r.routeNew(ctx, r.cfg.Defaults[channel], channelName, thread, payload)
}

// RouteNewDM is RouteNew for a thread in a direct message conversation,
// which the DM client takes; it has no channel name.
func (r *Router) RouteNewDM(ctx context.Context, thread string, payload []byte) (string, error) {
	return r.routeNew(ctx, r.cfg.DM, "", thread, payload)
}

func (r *Router) routeNew(ctx context.Context, def, channelName, thread string, payload []byte) (string, error) {
	if def == "" {
		client, err := r.RouteReply(ctx, thread, payload)
		if errors.Is(err, ErrNoOwner) {
			return "", ErrNoMachine
		}
		return client, err
	}
	client, _, err := r.hub.ClaimAndEnqueue(ctx, thread, def, channelName, payload)
	return client, err
}

// RouteReply queues payload, a reply in thread, for the thread's owner and
// returns the owner.
func (r *Router) RouteReply(ctx context.Context, thread string, payload []byte) (string, error) {
	client, err := r.hub.Owner(ctx, thread)
	if errors.Is(err, store.ErrNotFound) {
		return "", ErrNoOwner
	}
	if err != nil {
		return "", err
	}
	if _, err := r.hub.Outbox.Enqueue(ctx, client, payload); err != nil {
		return "", err
	}
	return client, nil
}
