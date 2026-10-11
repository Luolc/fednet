// Package route decides which client gets a message a person posted in a
// Slack thread, and queues it there.
package route

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/store"
)

// ErrNoMachine is returned for a new thread in a channel that has no
// default machine. The message is not queued anywhere.
var ErrNoMachine = errors.New("route: no machine takes this channel")

// ErrNoOwner is returned for a reply in a thread that has no owner, such as
// one started before fednet. The message is not queued anywhere.
var ErrNoOwner = errors.New("route: thread has no owner")

// Router queues the messages people post in threads, by the routes in the
// hub's database as they are when each message comes. A message is queued
// whether or not its client is connected; it waits in the outbox until the
// client takes it.
type Router struct {
	hub *store.Hub
}

// New returns a Router that reads the routes from hub and queues into its
// outbox. hub may be bound to a transaction (see store.Hub.ReceiveSlack);
// the Router then reads and queues in it.
func New(hub *store.Hub) *Router {
	return &Router{hub: hub}
}

// RouteNew queues m, the message that starts thread in channel, for the
// machine of the channel's route, which becomes the thread's owner, with
// channelName, the channel's name, and the route's scope recorded along;
// m carries that scope. If thread already has an owner, m goes to that
// owner instead. It returns the client the message was queued for.
func (r *Router) RouteNew(ctx context.Context, channel, channelName, thread string, m payload.Message) (string, error) {
	rt, err := r.hub.Route(ctx, channel)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return "", err
	}
	if rt.Machine == "" {
		client, err := r.RouteReply(ctx, thread, m)
		if errors.Is(err, ErrNoOwner) {
			return "", ErrNoMachine
		}
		return client, err
	}
	m.Scope = rt.Scope
	b, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	client, _, err := r.hub.ClaimAndEnqueue(ctx, thread, rt.Machine, channelName, rt.Scope, b)
	return client, err
}

// RouteNewDM is RouteNew for a thread in a direct message conversation,
// which the route of direct messages takes; it has no channel name.
func (r *Router) RouteNewDM(ctx context.Context, thread string, m payload.Message) (string, error) {
	return r.RouteNew(ctx, store.DMRoute, "", thread, m)
}

// RouteReply queues m, a reply in thread, for the thread's owner, with the
// scope recorded with the owner, and returns the owner.
func (r *Router) RouteReply(ctx context.Context, thread string, m payload.Message) (string, error) {
	client, err := r.hub.Owner(ctx, thread)
	if errors.Is(err, store.ErrNotFound) {
		return "", ErrNoOwner
	}
	if err != nil {
		return "", err
	}
	if m.Scope, err = r.hub.ThreadScope(ctx, thread); err != nil {
		return "", err
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	if _, err := r.hub.Outbox.Enqueue(ctx, client, b); err != nil {
		return "", err
	}
	return client, nil
}
