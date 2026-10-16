package slack

import (
	"context"
	"slices"

	slackgo "github.com/slack-go/slack"
)

func (w *Web) React(ctx context.Context, channel, ts, name string) error {
	err := w.call(ctx, "reactions.add", func() error {
		return w.c.AddReactionContext(ctx, name, slackgo.NewRefToMessage(channel, ts))
	})
	if Code(err) == "already_reacted" {
		return nil
	}
	return err
}

func (w *Web) Unreact(ctx context.Context, channel, ts, name string) error {
	err := w.call(ctx, "reactions.remove", func() error {
		return w.c.RemoveReactionContext(ctx, name, slackgo.NewRefToMessage(channel, ts))
	})
	if Code(err) == "no_reaction" {
		return nil
	}
	return err
}

func (f *Fake) React(_ context.Context, channel, ts, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.has(channel, ts) {
		return ErrNotFound
	}
	if f.reactions == nil {
		f.reactions = make(map[string][]string)
	}
	key := ThreadKey(channel, ts)
	if !slices.Contains(f.reactions[key], name) {
		f.reactions[key] = append(f.reactions[key], name)
	}
	return nil
}

func (f *Fake) Unreact(_ context.Context, channel, ts, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.has(channel, ts) {
		return ErrNotFound
	}
	key := ThreadKey(channel, ts)
	f.reactions[key] = slices.DeleteFunc(f.reactions[key], func(n string) bool { return n == name })
	return nil
}

// Reactions returns the names of the bot's reactions on the message at
// ts in channel, in the order they were added.
func (f *Fake) Reactions(channel, ts string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.reactions[ThreadKey(channel, ts)])
}

// has reports whether channel has a message at ts; f.mu is held.
func (f *Fake) has(channel, ts string) bool {
	c, ok := f.channels[channel]
	if !ok {
		return false
	}
	for _, ms := range c.threads {
		if slices.ContainsFunc(ms, func(m Message) bool { return m.TS == ts }) {
			return true
		}
	}
	return false
}
