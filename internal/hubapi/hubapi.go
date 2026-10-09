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

	"github.com/Luolc/fednet/internal/link"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
)

// Commands a Request can carry.
const (
	// ReadThread returns the messages of Thread.
	ReadThread = "read-thread"
	// Adopt makes the requesting client the owner of Thread, which must
	// already have an owner.
	Adopt = "adopt"
	// GetChannelContext returns the description of Channel, which is its
	// Slack purpose.
	GetChannelContext = "channel-context-get"
	// SetChannelContext replaces the description of Channel with Text.
	SetChannelContext = "channel-context-set"
)

// Request is one request. Cmd selects the command; the other fields are its
// arguments.
type Request struct {
	Cmd     string `json:"cmd"`
	Thread  string `json:"thread,omitempty"`
	Channel string `json:"channel,omitempty"`
	Text    string `json:"text,omitempty"`
}

// Reply is the hub's answer. Which fields are set depends on the command.
type Reply struct {
	Messages []slack.Message `json:"messages,omitempty"`
	Text     string          `json:"text,omitempty"`
}

// Server answers requests on the hub. Its Answer is meant for
// link.Hub.Answer.
type Server struct {
	Store *store.Hub
	// Slack is nil until the hub is connected to Slack; requests that need
	// it fail until then.
	Slack slack.API
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
	case Adopt:
		err = s.adopt(ctx, client, r)
	case GetChannelContext:
		reply, err = s.channelContext(ctx, r)
	case SetChannelContext:
		err = s.setChannelContext(ctx, r)
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
