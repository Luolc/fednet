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
	"io"
	"log/slog"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Luolc/fednet/internal/approval"
	"github.com/Luolc/fednet/internal/link"
	"github.com/Luolc/fednet/internal/outbound"
	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
)

// MaxTextChars is the longest Text, in characters, of an OpenThread or a
// DM: each goes out as one message, as long as each message of a post.
const MaxTextChars = outbound.DefaultMaxChars

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
	// RequestApproval asks the people on the approver list to approve an
	// action: Text says what it does, Action is its parameters, Agent is
	// the agent that will act, and Requester, if set, is the Slack user id
	// of the person the agent says it asks on behalf of, a note on the
	// card. It returns the approval id; the outcome comes down the link
	// later.
	RequestApproval = "request-approval"
	// Ops carries out Op, one of the fixed operations the hub's config
	// allows the requesting client, and returns the answer as Text.
	Ops = "ops"
	// Delete deletes from Slack the post with MsgID, which the requesting
	// client posted: every message it went out as.
	Delete = "delete"
	// CheckMentions checks that every one of Mentions is on the user
	// list, before a client queues a post that mentions them. A hub that
	// answers it also mentions them when it posts.
	CheckMentions = "check-mentions"
)

// Request is one request. Cmd selects the command; the other fields are its
// arguments.
type Request struct {
	Cmd     string `json:"cmd"`
	Thread  string `json:"thread,omitempty"`
	Channel string `json:"channel,omitempty"`
	Text    string `json:"text,omitempty"`
	User    string `json:"user,omitempty"`
	// Agent, Requester and Action are the arguments of RequestApproval.
	// Action is carried as bytes so that it reaches the hub byte for byte.
	Agent     string `json:"agent,omitempty"`
	Requester string `json:"requester,omitempty"`
	Action    []byte `json:"action,omitempty"`
	// Op is the argument of Ops.
	Op string `json:"op,omitempty"`
	// MsgID is the argument of Delete: the msg_id the post was queued
	// under.
	MsgID string `json:"msg_id,omitempty"`
	// Mentions is the argument of CheckMentions: Slack user ids.
	Mentions []string `json:"mentions,omitempty"`
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
	Messages   []slack.Message `json:"messages,omitempty"`
	Text       string          `json:"text,omitempty"`
	Thread     string          `json:"thread,omitempty"`
	Threads    []string        `json:"threads,omitempty"`
	Users      []User          `json:"users,omitempty"`
	ApprovalID string          `json:"approval_id,omitempty"`
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
	// Approvals runs the approvals; nil means the hub cannot give any, and
	// RequestApproval is refused.
	Approvals *approval.Flow
	// MaxFetchBytes is the largest file Fetch serves. Zero means
	// DefaultMaxFetchBytes.
	MaxFetchBytes int64
	// MaxUploadBytes is the largest file Upload takes, and MaxUploadFiles
	// how many one upload may have. Zero means the default.
	MaxUploadBytes int64
	MaxUploadFiles int
	// BeforeUpload, if set, readies a thread for an upload from a client
	// before the upload starts: it is outbound.Poster.BeforeUpload.
	BeforeUpload func(ctx context.Context, client, thread string) error
	// Ops carries out an op for client, which is the id the link
	// authenticated, never one the request names: it is upgrade.Hub.Op.
	// Nil means Ops is refused.
	Ops func(ctx context.Context, client, op string) (string, error)
}

// Defaults for the zero limits of Server: the largest file served is 200
// MiB, the largest uploaded 50 MiB, ten to an upload.
const (
	DefaultMaxFetchBytes  = 200 << 20
	DefaultMaxUploadBytes = 50 << 20
	DefaultMaxUploadFiles = 10
)

// Upload posts the files of u in its thread, for link.Hub.Upload, as a
// message from client with u.Text; the content comes from body, each
// file's Size bytes in order, and goes to Slack as it is read. An upload
// with no files, with more than MaxUploadFiles, with a file over
// MaxUploadBytes or empty, or with a file without a name, is refused
// before any content is read; so is one for a thread key that is not
// one. A file whose content falls short of its Size fails the upload.
func (s *Server) Upload(ctx context.Context, client string, u link.Upload, body io.Reader) error {
	channel, ts, err := thread(Request{Thread: u.Thread})
	if err != nil {
		return err
	}
	maxBytes, maxFiles := s.MaxUploadBytes, s.MaxUploadFiles
	if maxBytes == 0 {
		maxBytes = DefaultMaxUploadBytes
	}
	if maxFiles == 0 {
		maxFiles = DefaultMaxUploadFiles
	}
	switch {
	case len(u.Files) == 0:
		return link.Refuse(link.ErrBadRequest, "an upload needs at least one file")
	case len(u.Files) > maxFiles:
		return link.Refuse(link.ErrBadRequest, "an upload takes at most %d files, got %d", maxFiles, len(u.Files))
	}
	var files []slack.Upload
	for _, f := range u.Files {
		switch {
		case f.Name == "" || strings.ContainsAny(f.Name, "/\\"):
			return link.Refuse(link.ErrBadRequest, "%q is not a file name", f.Name)
		case f.Size <= 0:
			return link.Refuse(link.ErrBadRequest, "file %s is empty", f.Name)
		case f.Size > maxBytes:
			return link.Refuse(link.ErrBadRequest, "file %s is %d bytes, over the hub's limit of %d", f.Name, f.Size, maxBytes)
		}
		files = append(files, slack.Upload{Name: f.Name, Size: f.Size, Body: &exactly{r: body, n: f.Size, name: f.Name}})
	}
	if s.Slack == nil {
		return errNoSlack
	}
	if s.BeforeUpload != nil {
		if err := s.BeforeUpload(ctx, client, u.Thread); err != nil {
			return err
		}
	}
	err = s.Slack.Upload(ctx, channel, ts, u.Text, files)
	if errors.Is(err, slack.ErrNotFound) {
		return link.Refuse(link.ErrNotFound, "no thread %s", u.Thread)
	}
	return err
}

// exactly reads n bytes of r and then reports EOF; an EOF of r before
// that is an error naming the file, so that a short file is not posted
// as a whole one.
type exactly struct {
	r    io.Reader
	n    int64
	name string
}

func (e *exactly) Read(p []byte) (int, error) {
	if e.n <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > e.n {
		p = p[:e.n]
	}
	n, err := e.r.Read(p)
	e.n -= int64(n)
	if err == io.EOF && e.n > 0 {
		return n, fmt.Errorf("file %s ended %d bytes short", e.name, e.n)
	}
	if err == io.EOF {
		err = nil
	}
	return n, err
}

// fileID matches a Slack file id: letters and digits, nothing a path
// could be made of.
var fileID = regexp.MustCompile(`^[A-Za-z0-9]+$`)

// Fetch serves a file to client, for link.Hub.Fetch: it asks Slack what
// the file is, refuses one the hub does not serve (no such file, or one
// over MaxFetchBytes), and returns the content as Slack streams it. Any
// registered client may fetch any file the bot can see, as it may read
// any thread.
func (s *Server) Fetch(ctx context.Context, client, id string) (link.File, io.ReadCloser, error) {
	if !fileID.MatchString(id) {
		return link.File{}, nil, link.Refuse(link.ErrBadRequest, "%q is not a file id", id)
	}
	if s.Slack == nil {
		return link.File{}, nil, errNoSlack
	}
	f, err := s.Slack.FileInfo(ctx, id)
	if errors.Is(err, slack.ErrNotFound) {
		return link.File{}, nil, link.Refuse(link.ErrNotFound, "file %s no longer exists in Slack", id)
	}
	if err != nil {
		return link.File{}, nil, err
	}
	max := s.MaxFetchBytes
	if max == 0 {
		max = DefaultMaxFetchBytes
	}
	if int64(f.Size) > max {
		return link.File{}, nil, link.Refuse(link.ErrDenied, "file %s is %d bytes, over the hub's limit of %d", id, f.Size, max)
	}
	body, err := s.Slack.Download(ctx, f)
	if errors.Is(err, slack.ErrNotFound) {
		return link.File{}, nil, link.Refuse(link.ErrNotFound, "file %s no longer exists in Slack", id)
	}
	if err != nil {
		return link.File{}, nil, err
	}
	return link.File{Name: f.Name, Mimetype: f.Mimetype, Size: int64(f.Size)}, body, nil
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
	case RequestApproval:
		reply.ApprovalID, err = s.requestApproval(ctx, client, r)
	case Ops:
		if s.Ops == nil {
			err = link.Refuse(link.ErrDenied, "the hub runs no ops")
		} else {
			reply.Text, err = s.Ops(ctx, client, r.Op)
		}
	case Delete:
		err = s.delete(ctx, client, r)
	case CheckMentions:
		err = s.checkMentions(r)
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

// deleteTimeout bounds the delete that undoes an open-thread whose owner
// could not be recorded; tests shorten it.
var deleteTimeout = 30 * time.Second

// openThread posts the thread, then records its owner, with the channel's
// name when Slack gives it. If recording fails it deletes the message it
// posted, so no thread is left in Slack without an owner, and fails.
func (s *Server) openThread(ctx context.Context, client string, r Request) (Reply, error) {
	if r.Channel == "" || r.Text == "" {
		return Reply{}, link.Refuse(link.ErrBadRequest, "needs a channel and a text")
	}
	if err := checkLength(r.Text); err != nil {
		return Reply{}, err
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
	var name string
	if info, err := s.Slack.ChannelInfo(ctx, r.Channel); err != nil {
		slog.Warn("hubapi: reading the name of a new thread's channel, recording none", "thread", key, "err", err)
	} else {
		name = info.Name
	}
	if err := s.Store.Claim(ctx, key, client, name); err != nil {
		// The caller may have given up already; the message goes anyway,
		// within deleteTimeout.
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), deleteTimeout)
		derr := s.Slack.Delete(dctx, r.Channel, ts)
		cancel()
		if derr != nil {
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
	info, err := s.Slack.ChannelInfo(ctx, r.Channel)
	if errors.Is(err, slack.ErrNotFound) {
		return Reply{}, link.Refuse(link.ErrNotFound, "no channel %s", r.Channel)
	}
	return Reply{Text: info.Purpose}, err
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
	if err := checkLength(r.Text); err != nil {
		return err
	}
	if _, ok := s.Users[r.User]; !ok {
		return link.Refuse(link.ErrDenied, "user %s is not on the user list", r.User)
	}
	if s.Slack == nil {
		return errNoSlack
	}
	return s.Slack.DM(ctx, r.User, r.Text)
}

func (s *Server) checkMentions(r Request) error {
	if len(r.Mentions) == 0 {
		return link.Refuse(link.ErrBadRequest, "no users to mention")
	}
	for _, u := range r.Mentions {
		if _, ok := s.Users[u]; !ok {
			return link.Refuse(link.ErrDenied, "user %s is not on the user list", u)
		}
	}
	return nil
}

// delete deletes the messages a post from client went out as. Only the
// client that posted it may; one still on its way to Slack is refused for
// now. A message gone from Slack already counts as deleted, so a delete
// that failed half way can be tried again, but a post with all of them
// gone is not found.
func (s *Server) delete(ctx context.Context, client string, r Request) error {
	if r.MsgID == "" {
		return link.Refuse(link.ErrBadRequest, "needs a msg_id")
	}
	p, err := s.Store.Posted(ctx, r.MsgID)
	if errors.Is(err, store.ErrNotFound) {
		return link.Refuse(link.ErrNotFound, "no post %s on the hub", r.MsgID)
	}
	if err != nil {
		return err
	}
	if p.Client != client {
		return link.Refuse(link.ErrDenied, "post %s is not from client %s", r.MsgID, client)
	}
	var m payload.Message
	if json.Unmarshal(p.Payload, &m) != nil || m.Type != payload.Post {
		return link.Refuse(link.ErrBadRequest, "%s is not a post", r.MsgID)
	}
	channel, _, ok := slack.ParseThreadKey(m.Thread)
	switch {
	case !p.Delivered:
		return link.Refuse(link.ErrBusy, "post %s is not in Slack yet", r.MsgID)
	case !ok || len(p.TS) == 0:
		return link.Refuse(link.ErrNotFound, "post %s has no message in Slack the hub knows of", r.MsgID)
	}
	if s.Slack == nil {
		return errNoSlack
	}
	gone := 0
	for _, ts := range p.TS {
		err := s.Slack.Delete(ctx, channel, ts)
		if errors.Is(err, slack.ErrNotFound) {
			gone++
			continue
		}
		if err != nil {
			return err
		}
	}
	if gone == len(p.TS) {
		return link.Refuse(link.ErrNotFound, "post %s is no longer in Slack", r.MsgID)
	}
	return nil
}

// checkLength refuses a text over MaxTextChars.
func checkLength(text string) error {
	if utf8.RuneCountInString(text) > MaxTextChars {
		return link.Refuse(link.ErrBadRequest, "text over %d characters", MaxTextChars)
	}
	return nil
}

// requestApproval records the request and posts its card; the hub refuses
// when it cannot run approvals, so that no agent waits for an approval
// that cannot come.
func (s *Server) requestApproval(ctx context.Context, client string, r Request) (string, error) {
	switch {
	case r.Agent == "" || r.Text == "" || len(r.Action) == 0:
		return "", link.Refuse(link.ErrBadRequest, "needs an agent, a text and an action")
	case !json.Valid(r.Action):
		return "", link.Refuse(link.ErrBadRequest, "action is not JSON")
	case s.Approvals == nil:
		return "", link.Refuse(link.ErrDenied, "%v", approval.ErrOff)
	}
	id, err := s.Approvals.Request(ctx, client, r.Agent, r.Requester, r.Text, r.Action)
	switch {
	case errors.Is(err, approval.ErrOff):
		return "", link.Refuse(link.ErrDenied, "%v", err)
	case errors.Is(err, approval.ErrTooBig):
		return "", link.Refuse(link.ErrBadRequest, "%v", err)
	}
	return id, err
}
