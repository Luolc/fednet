// Package route decides which client gets a message a person posted in a
// Slack thread, and queues it there.
package route

import (
	"context"
	"errors"

	"github.com/Luolc/fednet/internal/store"
)

// ErrNoMachine is returned for a thread with no owner in a channel that has
// no default machine. The message is not queued anywhere.
var ErrNoMachine = errors.New("route: no machine takes this channel")

// Config is the routing part of the hub's configuration.
type Config struct {
	// Defaults maps a channel to the client that takes its new threads.
	Defaults map[string]string
}

// Router queues the messages people post in threads.
type Router struct {
	hub *store.Hub
	cfg Config
}

// New returns a Router that queues into hub's outbox.
func New(hub *store.Hub, cfg Config) *Router {
	return &Router{hub: hub, cfg: cfg}
}

// Route queues payload, posted in thread of channel, for the client that owns
// the thread and returns that client. A thread with no owner goes to the
// channel's default machine, which becomes its owner. The message is queued
// whether or not the client is connected; it waits in the outbox until the
// client takes it.
func (r *Router) Route(ctx context.Context, channel, thread string, payload []byte) (string, error) {
	client, err := r.hub.Owner(ctx, thread)
	if errors.Is(err, store.ErrNotFound) {
		def, ok := r.cfg.Defaults[channel]
		if !ok {
			return "", ErrNoMachine
		}
		client, err = r.hub.ClaimOwner(ctx, thread, def)
	}
	if err != nil {
		return "", err
	}
	if _, err := r.hub.Outbox.Enqueue(ctx, client, payload); err != nil {
		return "", err
	}
	return client, nil
}
