// Package hubapi is the requests a client sends the hub on the link's
// request channel and the hub's answers: their JSON format, which both ends
// share, and Server, which answers them on the hub. Requests that need Slack
// are answered through the hub's Slack API, so a client never needs a Slack
// token.
package hubapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/Luolc/fednet/internal/link"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
)

// Commands a Request can carry.
const (
	// ReadThread returns the messages of Thread.
	ReadThread = "read-thread"
	// OpenThread posts Text in Channel as a new thread, makes the
	// requesting client its owner, and returns its key.
	OpenThread = "open-thread"
	// Threads returns the threads the requesting client owns.
	Threads = "threads"
	// Adopt makes the requesting client the owner of Thread, which must
	// already have an owner.
	Adopt = "adopt"
	// GetChannelContext returns the description of Channel, which is its
	// Slack purpose.
	GetChannelContext = "channel-context-get"
	// SetChannelContext replaces the description of Channel with Text.
	SetChannelContext = "channel-context-set"
	// Users returns the user list.
	Users = "users"
	// DM sends Text to User, who must be on the user list, as a direct
	// message.
	DM = "dm"
)

// Request is one request. Cmd selects the command; the other fields are its
// arguments.
type Request struct {
	Cmd     string `json:"cmd"`
	Thread  string `json:"thread,omitempty"`
	Channel string `json:"channel,omitempty"`
	Text    string `json:"text,omitempty"`
	User    string `json:"user,omitempty"`
}

// User is one person on the user list.
type User struct {
	// ID is the person's Slack user id.
	ID string `json:"id"`
	// Name is what the agents call them; it may be empty.
	Name string `json:"name,omitempty"`
}

// Reply is the hub's answer. Which fields are set depends on the command.
type Reply struct {
	Messages []slack.Message `json:"messages,omitempty"`
	Text     string          `json:"text,omitempty"`
	Thread   string          `json:"thread,omitempty"`
	Threads  []string        `json:"threads,omitempty"`
	Users    []User          `json:"users,omitempty"`
}

// Server answers requests on the hub. Its Answer is meant for
// link.Hub.Answer.
type Server struct {
	Store *store.Hub
	// Slack is nil until the hub is connected to Slack; requests that need
	// it fail until then.
	Slack slack.API
	// OpenThread maps a channel to the clients that may open threads in
	// it. A client may open threads only in the channels that list it.
	OpenThread map[string][]string
	// Users is the user list: the Slack user id of each person fednet
	// serves, mapped to a name for the agents, which may be empty. Only
	// people on it get direct messages.
	Users map[string]string
}

// errNoSlack is returned for a request that needs Slack when Server.Slack
// is nil.
var errNoSlack = errors.New("hubapi: the hub is not connected to Slack")

// Answer answers req, a JSON Request from client, with a JSON Reply.
func (s *Server) Answer(ctx context.Context, client string, req []byte) ([]byte, error) {
	var r Request
	if err := json.Unmarshal(req, &r); err != nil {
		return nil, link.Refuse(link.ErrBadRequest, "unreadable request: %v", err)
	}
	var reply Reply
	var err error
	switch r.Cmd {
	case ReadThread:
		reply, err = s.readThread(ctx, r)
	case OpenThread:
		reply, err = s.openThread(ctx, client, r)
	case Threads:
		reply.Threads, err = s.Store.Threads(ctx, client)
	case Adopt:
		err = s.adopt(ctx, client, r)
	case GetChannelContext:
		reply, err = s.channelContext(ctx, r)
	case SetChannelContext:
		err = s.setChannelContext(ctx, r)
	case Users:
		reply.Users = s.users()
	case DM:
		err = s.dm(ctx, r)
	default:
		err = link.Refuse(link.ErrBadRequest, "unknown command %q", r.Cmd)
	}
	if err != nil {
		return nil, err
	}
	return json.Marshal(reply)
}

// thread checks that r names a thread and returns its channel and ts.
func thread(r Request) (channel, ts string, err error) {
	channel, ts, ok := slack.ParseThreadKey(r.Thread)
	if !ok {
		return "", "", link.Refuse(link.ErrBadRequest, "%q is not a thread key (CHANNEL/TS)", r.Thread)
	}
	return channel, ts, nil
}

func (s *Server) readThread(ctx context.Context, r Request) (Reply, error) {
	channel, ts, err := thread(r)
	if err != nil {
		return Reply{}, err
	}
	if s.Slack == nil {
		return Reply{}, errNoSlack
	}
	ms, err := s.Slack.Replies(ctx, channel, ts)
	if errors.Is(err, slack.ErrNotFound) {
		return Reply{}, link.Refuse(link.ErrNotFound, "no thread %s", r.Thread)
	}
	return Reply{Messages: ms}, err
}

// openThread posts the thread, then records its owner. If recording fails
// it deletes the message it posted, so no thread is left in Slack without
// an owner, and fails.
func (s *Server) openThread(ctx context.Context, client string, r Request) (Reply, error) {
	if r.Channel == "" || r.Text == "" {
		return Reply{}, link.Refuse(link.ErrBadRequest, "needs a channel and a text")
	}
	if !slices.Contains(s.OpenThread[r.Channel], client) {
		return Reply{}, link.Refuse(link.ErrDenied, "client %s may not open threads in %s", client, r.Channel)
	}
	if s.Slack == nil {
		return Reply{}, errNoSlack
	}
	ts, err := s.Slack.Post(ctx, r.Channel, r.Text)
	if errors.Is(err, slack.ErrNotFound) {
		return Reply{}, link.Refuse(link.ErrNotFound, "no channel %s", r.Channel)
	}
	if err != nil {
		return Reply{}, err
	}
	key := slack.ThreadKey(r.Channel, ts)
	if err := s.Store.Claim(ctx, key, client); err != nil {
		// The caller may have given up already; the message goes anyway.
		if derr := s.Slack.Delete(context.WithoutCancel(ctx), r.Channel, ts); derr != nil {
			return Reply{}, fmt.Errorf("recording the owner of thread %s failed: %w; deleting its message failed too, so it stays in Slack without an owner: %v", key, err, derr)
		}
		return Reply{}, fmt.Errorf("recording the owner of thread %s failed, so its message was deleted: %w", key, err)
	}
	return Reply{Thread: key}, nil
}

func (s *Server) adopt(ctx context.Context, client string, r Request) error {
	if _, _, err := thread(r); err != nil {
		return err
	}
	err := s.Store.Reassign(ctx, r.Thread, client)
	if errors.Is(err, store.ErrNotFound) {
		return link.Refuse(link.ErrNotFound, "thread %s has no owner", r.Thread)
	}
	return err
}

func (s *Server) channelContext(ctx context.Context, r Request) (Reply, error) {
	if r.Channel == "" {
		return Reply{}, link.Refuse(link.ErrBadRequest, "no channel")
	}
	if s.Slack == nil {
		return Reply{}, errNoSlack
	}
	p, err := s.Slack.Purpose(ctx, r.Channel)
	if errors.Is(err, slack.ErrNotFound) {
		return Reply{}, link.Refuse(link.ErrNotFound, "no channel %s", r.Channel)
	}
	return Reply{Text: p}, err
}

func (s *Server) setChannelContext(ctx context.Context, r Request) error {
	if r.Channel == "" {
		return link.Refuse(link.ErrBadRequest, "no channel")
	}
	if s.Slack == nil {
		return errNoSlack
	}
	err := s.Slack.SetPurpose(ctx, r.Channel, r.Text)
	if errors.Is(err, slack.ErrNotFound) {
		return link.Refuse(link.ErrNotFound, "no channel %s", r.Channel)
	}
	return err
}

// users returns the user list, ordered by id.
func (s *Server) users() []User {
	var us []User
	for _, id := range slices.Sorted(maps.Keys(s.Users)) {
		us = append(us, User{ID: id, Name: s.Users[id]})
	}
	return us
}

func (s *Server) dm(ctx context.Context, r Request) error {
	if r.User == "" || r.Text == "" {
		return link.Refuse(link.ErrBadRequest, "needs a user and a text")
	}
	if _, ok := s.Users[r.User]; !ok {
		return link.Refuse(link.ErrDenied, "user %s is not on the user list", r.User)
	}
	if s.Slack == nil {
		return errNoSlack
	}
	return s.Slack.DM(ctx, r.User, r.Text)
}
