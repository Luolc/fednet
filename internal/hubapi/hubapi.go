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
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Luolc/fednet/internal/approval"
	"github.com/Luolc/fednet/internal/ask"
	"github.com/Luolc/fednet/internal/link"
	"github.com/Luolc/fednet/internal/outbound"
	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/question"
	"github.com/Luolc/fednet/internal/route"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
)

// How long the hub waits for what a client posted before to be in Slack
// before an upload or a question card from it goes out; tests shorten
// them. An Ask waits less: its client gives the whole request less time
// than an upload.
var (
	uploadWait = 30 * time.Second
	askWait    = 15 * time.Second
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
	// OpenThreadForRoute is OpenThread for the machine of the channel's
	// route, which becomes the owner and gets Text as an inbound message
	// of trigger handoff: the requesting client must be allowed to open
	// threads in Channel and, unless it is that machine, to deliver to it.
	// Thread, if set, is the thread the handoff comes from, which the
	// requesting client must own: the new thread's hop count goes on from
	// that thread's, so that handing work back and forth in new threads
	// stops at the same limit as relays in one thread. It is not looked at
	// when the requesting client is that machine, which nothing is
	// delivered to.
	OpenThreadForRoute = "open-thread-route"
	// CheckRelay checks that the requesting client may deliver its posts
	// in Thread to the thread's owner, before it queues a post to deliver.
	// A hub that answers it also delivers such a post.
	CheckRelay = "check-relay"
	// Threads returns the threads the requesting client owns.
	Threads = "threads"
	// ChannelThreads returns the threads in Channel that have an owner,
	// latest first by the ts of their first message, at most Limit of
	// them, or DefaultChannelThreads when Limit is 0. Any client may list
	// any channel, as it may read any thread.
	ChannelThreads = "channel-threads"
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
	// RequestRoute asks the people on the approver list to approve
	// Route, a change of one route: Text says why, Agent is the agent
	// that asks and Requester, if set, the person it says it asks on
	// behalf of, as for RequestApproval. It returns the approval id; once
	// approved, the hub makes the change itself, and the outcome comes
	// down the link as an approval's does, unsigned.
	RequestRoute = "request-route"
	// RequestChannel is RequestRoute for a channel to create: Route.Create
	// names it, and the rest of Route is its route. Once the card is
	// approved, the hub creates the channel, writes its route and sends
	// the outcome down, with the channel's id or why it failed.
	RequestChannel = "request-channel"
	// Routes returns every route.
	Routes = "routes"
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
	// Ask posts Card, a question card, in Thread, mentioning Mentions,
	// each on the user list, and returns where it is. Ref is the
	// requesting client's id of the question: a ref it asked with before
	// posts nothing and returns that card. The card goes out after what
	// the client posted before, so that it lands below it; when that is
	// not in Slack within 15 seconds, Ask is refused as busy. What people do
	// with the card comes down the link later, as messages of type answer.
	Ask = "ask"
	// AskClose takes back the card with ID, which the requesting client
	// asked, with Text, if set, a note on the card.
	AskClose = "ask-close"
	// ReadAnswer returns the question card with ID as the hub keeps it.
	// Any client may read any card, as it may read any thread.
	ReadAnswer = "read-answer"
	// React adds the bot's reaction Name, an emoji's name without colons,
	// to the first message of Thread, and Unreact removes it. Either is
	// done when the bot's reaction already is, or is not, there. Any
	// client may react in any thread, as it may read and post in any.
	React   = "react"
	Unreact = "unreact"
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
	// Route is the argument of RequestRoute.
	Route *RouteRequest `json:"route,omitempty"`
	// Ref and Card are the arguments of Ask; Card is carried as bytes so
	// that the hub checks it as the agent wrote it.
	Ref  string `json:"ref,omitempty"`
	Card []byte `json:"card,omitempty"`
	// ID is the argument of AskClose and ReadAnswer.
	ID string `json:"id,omitempty"`
	// Name is the argument of React and Unreact.
	Name string `json:"name,omitempty"`
	// Limit is the argument of ChannelThreads.
	Limit int `json:"limit,omitempty"`
}

// DefaultChannelThreads is how many threads ChannelThreads returns when
// the request gives no Limit.
const DefaultChannelThreads = 20

// RouteRequest is a change of one route that RequestRoute asks for: the
// route of Channel becomes Machine, Scope and OpenThread, or, with Delete,
// goes.
type RouteRequest struct {
	// Channel is a Slack channel id, or store.DMRoute.
	Channel string `json:"channel"`
	Machine string `json:"machine,omitempty"`
	Scope   string `json:"scope,omitempty"`
	// OpenThread replaces the clients that may open threads in the
	// channel; nil keeps those of the route there is, and gives a new
	// route Machine alone.
	OpenThread *[]string `json:"open_thread,omitempty"`
	Delete     bool      `json:"delete,omitempty"`
	// Create is the channel RequestChannel creates; Channel is empty then.
	Create *approval.NewChannel `json:"create,omitempty"`
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
	Routes     []store.Route   `json:"routes,omitempty"`
	Question   *ask.Posted     `json:"question,omitempty"`
	Card       *Card           `json:"card,omitempty"`
	// ChannelThreads is the answer to ChannelThreads; absent for a
	// channel with no thread that has an owner.
	ChannelThreads []ChannelThread `json:"channel_threads,omitempty"`
}

// ChannelThread is one thread as ChannelThreads returns it, all from the
// hub's database: the thread's key, the client that owns it, the name of
// its channel as Slack gave it when the thread got its owner (absent when
// the hub had none then, as for a direct message), and when the thread
// started, which is the ts in its key.
type ChannelThread struct {
	Thread      string    `json:"thread"`
	Owner       string    `json:"owner"`
	ChannelName string    `json:"channel_name,omitempty"`
	StartedAt   time.Time `json:"started_at"`
}

// Card is a question card as ReadAnswer returns it: where it is now, the
// question as the agent asked it, and its status. Answer is the last
// press on the card that counted, the JSON that went down to the client
// that asked; it is absent until a press counts, and a snooze is replaced
// by the press after it. Only ask.Flow.Click writes it, for a click that
// came from Slack by a person on the user list who is not a bot; no
// request a client sends does.
type Card struct {
	ID        string            `json:"id"`
	Thread    string            `json:"thread"`
	TS        string            `json:"ts"`
	Permalink string            `json:"permalink,omitempty"`
	AskedAt   time.Time         `json:"asked_at"`
	Question  question.Question `json:"question"`
	Status    string            `json:"status"`
	Answer    json.RawMessage   `json:"answer,omitempty"`
}

// Server answers requests on the hub. Its Answer is meant for
// link.Hub.Answer.
type Server struct {
	Store *store.Hub
	// Slack is nil until the hub is connected to Slack; requests that need
	// it fail until then.
	Slack slack.API
	// Users is the user list: the Slack user id of each person fednet
	// serves, mapped to a name for the agents, which may be empty. Only
	// people on it get direct messages.
	Users map[string]string
	// Questions runs the question cards; nil means the hub runs none,
	// and Ask and AskClose are refused.
	Questions *ask.Flow
	// Approvals runs the approvals; nil means the hub cannot give any, and
	// RequestApproval and RequestRoute are refused.
	Approvals *approval.Flow
	// MaxFetchBytes is the largest file Fetch serves. Zero means
	// DefaultMaxFetchBytes.
	MaxFetchBytes int64
	// MaxUploadBytes is the largest file Upload takes, and MaxUploadFiles
	// how many one upload may have. Zero means the default.
	MaxUploadBytes int64
	MaxUploadFiles int
	// CatchUp, if set, readies a thread for an upload or a question card
	// from a client before it goes out, waiting at most wait for what the
	// client posted before: it is outbound.Poster.CatchUp.
	CatchUp func(ctx context.Context, client, thread string, wait time.Duration) error
	// Ops carries out an op for client, which is the id the link
	// authenticated, never one the request names: it is upgrade.Hub.Op.
	// Nil means Ops is refused.
	Ops func(ctx context.Context, client, op string) (string, error)
	// Relays is the relay list: which clients each client may deliver to.
	Relays route.Relays
	// Stored, if set, is called once a request has queued a message for a
	// client, to wake the downlinks: it is link.Hub.WakeAll.
	Stored func()
	// Alert, if set, sends an alert to the alerts channel: it is
	// alert.Webhook.Send. It reports a handoff refused for the hop limit.
	Alert func(ctx context.Context, text string) error

	// names holds the names of the routes' channels as Slack last gave
	// them, by channel id.
	namesMu sync.Mutex
	names   map[string]channelNameAt
}

type channelNameAt struct {
	name string
	at   time.Time
}

// nameTTL is how long the hub answers routes with the name Slack gave
// for a channel before it asks again, so a renamed channel may keep its
// old name that long. nameBudget bounds the lookups of one answer, well
// within the time the client waits for it.
const (
	nameTTL    = 10 * time.Minute
	nameBudget = 2 * time.Second
)

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
	if s.CatchUp != nil {
		if err := s.CatchUp(ctx, client, u.Thread, uploadWait); err != nil {
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
		reply, err = s.openThread(ctx, client, r, false)
	case OpenThreadForRoute:
		reply, err = s.openThread(ctx, client, r, true)
	case CheckRelay:
		err = s.checkRelay(ctx, client, r)
	case Threads:
		reply.Threads, err = s.Store.Threads(ctx, client)
	case ChannelThreads:
		reply.ChannelThreads, err = s.channelThreads(ctx, r)
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
	case RequestRoute:
		reply.ApprovalID, err = s.requestRoute(ctx, client, r)
	case RequestChannel:
		reply.ApprovalID, err = s.requestChannel(ctx, client, r)
	case Routes:
		reply.Routes, err = s.routes(ctx)
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
	case Ask:
		reply.Question, err = s.ask(ctx, client, r)
	case AskClose:
		err = s.askClose(ctx, client, r)
	case ReadAnswer:
		reply.Card, err = s.readAnswer(ctx, r)
	case React, Unreact:
		err = s.react(ctx, r)
	default:
		err = link.Refuse(link.ErrBadRequest, "unknown command %q", r.Cmd)
	}
	if err != nil {
		return nil, err
	}
	return json.Marshal(reply)
}

// routes returns every route, each but that of direct messages with its
// channel's name. A name Slack does not give in time is left empty, with
// a warning, and the routes go without it.
func (s *Server) routes(ctx context.Context) ([]store.Route, error) {
	rs, err := s.Store.Routes(ctx)
	if err != nil || s.Slack == nil {
		return rs, err
	}
	ctx, cancel := context.WithTimeout(ctx, nameBudget)
	defer cancel()
	for i := range rs {
		if rs[i].Channel != store.DMRoute {
			rs[i].Name = s.nameOf(ctx, rs[i].Channel)
		}
	}
	return rs, nil
}

// nameOf returns the name of channel, asking Slack unless it gave the
// name within nameTTL; empty when Slack does not give it.
func (s *Server) nameOf(ctx context.Context, channel string) string {
	s.namesMu.Lock()
	n, ok := s.names[channel]
	s.namesMu.Unlock()
	if ok && time.Since(n.at) < nameTTL {
		return n.name
	}
	info, err := s.Slack.ChannelInfo(ctx, channel)
	if err != nil {
		slog.Warn("hubapi: reading the name of a route's channel, answering routes without it", "channel", channel, "err", err)
		return ""
	}
	s.namesMu.Lock()
	if s.names == nil {
		s.names = make(map[string]channelNameAt)
	}
	s.names[channel] = channelNameAt{info.Name, time.Now()}
	s.namesMu.Unlock()
	return info.Name
}

// thread checks that r names a thread and returns its channel and ts.
func thread(r Request) (channel, ts string, err error) {
	channel, ts, ok := slack.ParseThreadKey(r.Thread)
	if !ok {
		return "", "", link.Refuse(link.ErrBadRequest, "%q is not a thread key (CHANNEL/TS)", r.Thread)
	}
	return channel, ts, nil
}

func (s *Server) channelThreads(ctx context.Context, r Request) ([]ChannelThread, error) {
	switch {
	case r.Channel == "" || strings.Contains(r.Channel, "/"):
		return nil, link.Refuse(link.ErrBadRequest, "%q is not a channel id", r.Channel)
	case r.Limit < 0:
		return nil, link.Refuse(link.ErrBadRequest, "limit %d is negative", r.Limit)
	}
	limit := r.Limit
	if limit == 0 {
		limit = DefaultChannelThreads
	}
	owned, err := s.Store.OwnedIn(ctx, r.Channel)
	if err != nil {
		return nil, err
	}
	ts := func(o store.Owned) string { _, ts, _ := slack.ParseThreadKey(o.Thread); return ts }
	slices.SortStableFunc(owned, func(a, b store.Owned) int { return slack.CompareTS(ts(b), ts(a)) })
	var threads []ChannelThread
	for _, o := range owned[:min(limit, len(owned))] {
		started, _ := slack.TSTime(ts(o))
		threads = append(threads, ChannelThread{Thread: o.Thread, Owner: o.Client, ChannelName: o.ChannelName, StartedAt: started})
	}
	return threads, nil
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
// name when Slack gives it and the scope of the channel's route. If
// recording fails it deletes the message it posted, so no thread is left
// in Slack without an owner, and fails. The owner is client or, forRoute,
// the machine of the channel's route, for which the message is queued in
// the same transaction as the owner is recorded.
func (s *Server) openThread(ctx context.Context, client string, r Request, forRoute bool) (Reply, error) {
	if r.Channel == "" || r.Text == "" {
		return Reply{}, link.Refuse(link.ErrBadRequest, "needs a channel and a text")
	}
	if err := checkLength(r.Text); err != nil {
		return Reply{}, err
	}
	rt, err := s.Store.Route(ctx, r.Channel)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return Reply{}, err
	}
	if r.Channel == store.DMRoute || !slices.Contains(rt.OpenThread, client) {
		return Reply{}, link.Refuse(link.ErrDenied, "client %s may not open threads in %s", client, r.Channel)
	}
	owner, hop := client, 0
	if forRoute {
		owner = rt.Machine
		switch {
		case owner == "":
			return Reply{}, link.Refuse(link.ErrDenied, "the route of %s has no machine", r.Channel)
		case owner != client && !s.Relays.Allow(client, owner):
			return Reply{}, link.Refuse(link.ErrDenied, "the hub's relay list does not let client %s deliver to %s", client, owner)
		}
		if r.Thread != "" && owner != client {
			if hop, err = s.fromThread(ctx, client, owner, r); err != nil {
				return Reply{}, err
			}
		}
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
	err = s.Store.Transact(ctx, func(tx *store.Hub) error {
		if err := tx.Claim(ctx, key, owner, name, rt.Scope); err != nil || owner == client {
			return err
		}
		if err := tx.SetHop(ctx, key, hop); err != nil {
			return err
		}
		_, err := route.New(tx).Deliver(ctx, s.Relays, client, key, payload.Message{Type: payload.Inbound, Thread: key, Text: r.Text, TS: ts, Trigger: payload.Handoff})
		return err
	})
	if err != nil {
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
	if owner != client && s.Stored != nil {
		s.Stored()
	}
	return Reply{Thread: key}, nil
}

// fromThread returns the hop count of r.Thread, the thread a handoff by
// client to owner comes from, which client must own. One at the limit is
// refused, and alerted, before anything is posted.
func (s *Server) fromThread(ctx context.Context, client, owner string, r Request) (int, error) {
	if _, _, err := thread(r); err != nil {
		return 0, err
	}
	from, err := s.Store.Owner(ctx, r.Thread)
	switch {
	case errors.Is(err, store.ErrNotFound):
		return 0, link.Refuse(link.ErrNotFound, "thread %s has no owner", r.Thread)
	case err != nil:
		return 0, err
	case from != client:
		return 0, link.Refuse(link.ErrDenied, "client %s does not own thread %s", client, r.Thread)
	}
	hop, err := s.Store.Hop(ctx, r.Thread)
	if err != nil || hop < route.MaxHop {
		return hop, err
	}
	slog.Error("hubapi: handoff not delivered", "client", client, "to", owner, "from_thread", r.Thread, "err", route.ErrHopLimit)
	if s.Alert != nil {
		if err := s.Alert(ctx, fmt.Sprintf("handoff from %s to %s out of thread %s not delivered: %v", client, owner, r.Thread, route.ErrHopLimit)); err != nil {
			slog.Warn("hubapi: alert", "err", err)
		}
	}
	return 0, link.Refuse(link.ErrDenied, "thread %s has had %d relays in a row since a person last posted in it, the most the hub delivers", r.Thread, route.MaxHop)
}

// checkRelay refuses unless client may deliver its posts in r.Thread:
// the thread has an owner, and it is client or one the relay list lets
// client deliver to. How many relays in a row the thread has had is left
// to the delivery, which is when it counts.
func (s *Server) checkRelay(ctx context.Context, client string, r Request) error {
	if _, _, err := thread(r); err != nil {
		return err
	}
	owner, err := s.Store.Owner(ctx, r.Thread)
	switch {
	case errors.Is(err, store.ErrNotFound):
		return link.Refuse(link.ErrNotFound, "thread %s has no owner", r.Thread)
	case err != nil:
		return err
	case owner != client && !s.Relays.Allow(client, owner):
		return link.Refuse(link.ErrDenied, "the hub's relay list does not let client %s deliver to %s, the owner of %s", client, owner, r.Thread)
	}
	return nil
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

// Patterns of a route's channel and scope.
var (
	channelID = regexp.MustCompile(`^[A-Z0-9]+$`)
	scopeName = regexp.MustCompile(`^[a-z0-9-]+$`)
)

// requestRoute checks the change r asks for against the routes as they
// are, makes it whole, and posts its card; the card shows the route the
// channel will have, which is what an approval sets.
func (s *Server) requestRoute(ctx context.Context, client string, r Request) (string, error) {
	q := r.Route
	switch {
	case r.Agent == "" || r.Text == "" || q == nil:
		return "", link.Refuse(link.ErrBadRequest, "needs an agent, a text and a route")
	case q.Create != nil:
		return "", link.Refuse(link.ErrBadRequest, "%s creates no channel; %s does", RequestRoute, RequestChannel)
	case q.Channel != store.DMRoute && !channelID.MatchString(q.Channel):
		return "", link.Refuse(link.ErrBadRequest, "%q is neither a Slack channel id nor %s", q.Channel, store.DMRoute)
	case s.Approvals == nil:
		return "", link.Refuse(link.ErrDenied, "%v", approval.ErrOff)
	}
	// A card shows the whole route the channel will have, which its
	// approval writes; a second card for the channel, made from the same
	// route, would undo the first's change when approved after it.
	if err := s.noneWaiting(ctx, "the route of "+q.Channel, func(c approval.RouteChange) bool { return c.Channel == q.Channel }); err != nil {
		return "", err
	}
	current, err := s.Store.Route(ctx, q.Channel)
	exists := err == nil
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return "", err
	}
	change := approval.RouteChange{Route: store.Route{Channel: q.Channel}, Delete: q.Delete}
	if q.Delete {
		switch {
		case q.Machine != "" || q.Scope != "" || q.OpenThread != nil:
			return "", link.Refuse(link.ErrBadRequest, "a delete takes nothing but the channel")
		case !exists:
			return "", link.Refuse(link.ErrNotFound, "no route for %s", q.Channel)
		}
		return s.postRouteRequest(ctx, client, r, change)
	}
	if q.Scope != "" && !scopeName.MatchString(q.Scope) {
		return "", link.Refuse(link.ErrBadRequest, "scope %q is not [a-z0-9-]", q.Scope)
	}
	change.Machine, change.Scope = q.Machine, q.Scope
	switch {
	case q.Channel == store.DMRoute:
		if q.Machine == "" || q.OpenThread != nil {
			return "", link.Refuse(link.ErrBadRequest, "the route of direct messages needs a machine and takes no open_thread")
		}
	case q.OpenThread != nil:
		change.OpenThread = *q.OpenThread
	case exists:
		change.OpenThread = current.OpenThread
	case q.Machine != "":
		change.OpenThread = []string{q.Machine}
	}
	if q.Machine == "" && len(change.OpenThread) == 0 {
		return "", link.Refuse(link.ErrBadRequest, "the route would do nothing: give a machine or open_thread, or delete it")
	}
	if err := s.checkClients(ctx, append([]string{q.Machine}, change.OpenThread...)); err != nil {
		return "", err
	}
	return s.postRouteRequest(ctx, client, r, change)
}

// channelName is the pattern of the name of a channel Slack creates.
var channelName = regexp.MustCompile(`^[a-z0-9_-]{1,80}$`)

// requestChannel checks the channel r asks to create and its route, makes
// the route whole as requestRoute does for a new one, and posts the card;
// the channel is created once the card is approved.
func (s *Server) requestChannel(ctx context.Context, client string, r Request) (string, error) {
	q := r.Route
	switch {
	case r.Agent == "" || r.Text == "" || q == nil || q.Create == nil:
		return "", link.Refuse(link.ErrBadRequest, "needs an agent, a text and a channel to create")
	case !channelName.MatchString(q.Create.Name):
		return "", link.Refuse(link.ErrBadRequest, "channel name %q is not 1 to 80 of a-z, 0-9, - and _", q.Create.Name)
	case q.Channel != "" || q.Delete:
		return "", link.Refuse(link.ErrBadRequest, "a channel to create has no id yet and nothing to delete")
	case q.Machine == "":
		return "", link.Refuse(link.ErrBadRequest, "needs the machine that takes the channel")
	case q.Scope != "" && !scopeName.MatchString(q.Scope):
		return "", link.Refuse(link.ErrBadRequest, "scope %q is not [a-z0-9-]", q.Scope)
	case s.Approvals == nil:
		return "", link.Refuse(link.ErrDenied, "%v", approval.ErrOff)
	}
	for _, u := range q.Create.Invite {
		if _, ok := s.Users[u]; !ok {
			return "", link.Refuse(link.ErrBadRequest, "%s is not on the user list", u)
		}
	}
	if err := s.noneWaiting(ctx, "#"+q.Create.Name, func(c approval.RouteChange) bool { return c.Create != nil && c.Create.Name == q.Create.Name }); err != nil {
		return "", err
	}
	open := []string{q.Machine}
	if q.OpenThread != nil {
		open = *q.OpenThread
	}
	if err := s.checkClients(ctx, append([]string{q.Machine}, open...)); err != nil {
		return "", err
	}
	return s.postRouteRequest(ctx, client, r, approval.RouteChange{Route: store.Route{Machine: q.Machine, Scope: q.Scope, OpenThread: open}, Create: q.Create})
}

// noneWaiting refuses, as busy, when a route request that match picks
// waits for approval; what names it in the refusal.
func (s *Server) noneWaiting(ctx context.Context, what string, match func(approval.RouteChange) bool) error {
	pending, err := s.Store.PendingApprovals(ctx)
	if err != nil {
		return err
	}
	for _, a := range pending {
		var c approval.RouteChange
		if (a.Kind == store.KindRoute || a.Kind == store.KindChannel) && json.Unmarshal(a.Action, &c) == nil && match(c) {
			return link.Refuse(link.ErrBusy, "%s has a request waiting for approval (%s); ask again once it is decided", what, a.ID)
		}
	}
	return nil
}

// checkClients refuses any of ids, empty ones aside, that is not a client
// the hub knows and has not revoked.
func (s *Server) checkClients(ctx context.Context, ids []string) error {
	for _, c := range ids {
		if c == "" {
			continue
		}
		reg, err := s.Store.Registration(ctx, c)
		if errors.Is(err, store.ErrNotFound) || err == nil && reg.Revoked {
			return link.Refuse(link.ErrBadRequest, "no client %s", c)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) postRouteRequest(ctx context.Context, client string, r Request, change approval.RouteChange) (string, error) {
	id, err := s.Approvals.RequestRoute(ctx, client, r.Agent, r.Requester, r.Text, change)
	switch {
	case errors.Is(err, approval.ErrOff):
		return "", link.Refuse(link.ErrDenied, "%v", err)
	case errors.Is(err, approval.ErrTooBig):
		return "", link.Refuse(link.ErrBadRequest, "%v", err)
	}
	return id, err
}

// ask posts a question card for client, as Ask says.
func (s *Server) ask(ctx context.Context, client string, r Request) (*ask.Posted, error) {
	if _, _, err := thread(r); err != nil {
		return nil, err
	}
	if r.Ref == "" || len(r.Card) == 0 {
		return nil, link.Refuse(link.ErrBadRequest, "needs a ref and a card")
	}
	if s.Questions == nil {
		return nil, link.Refuse(link.ErrDenied, "the hub runs no question cards: it has no Slack")
	}
	if s.CatchUp != nil {
		if err := s.CatchUp(ctx, client, r.Thread, askWait); err != nil {
			return nil, link.Refuse(link.ErrBusy, "%v", err)
		}
	}
	p, err := s.Questions.Ask(ctx, client, r.Thread, r.Ref, r.Card, r.Mentions)
	switch {
	case errors.Is(err, ask.ErrInvalid):
		return nil, link.Refuse(link.ErrBadRequest, "%v", err)
	case errors.Is(err, ask.ErrNotListed):
		return nil, link.Refuse(link.ErrDenied, "%v", err)
	case errors.Is(err, slack.ErrNotFound):
		return nil, link.Refuse(link.ErrNotFound, "no thread %s", r.Thread)
	case err != nil:
		return nil, err
	}
	return &p, nil
}

// askClose takes back a question card client asked, as AskClose says.
func (s *Server) askClose(ctx context.Context, client string, r Request) error {
	if r.ID == "" {
		return link.Refuse(link.ErrBadRequest, "needs a card id")
	}
	if s.Questions == nil {
		return link.Refuse(link.ErrDenied, "the hub runs no question cards: it has no Slack")
	}
	err := s.Questions.Withdraw(ctx, client, r.ID, r.Text)
	switch {
	case errors.Is(err, store.ErrNotFound):
		return link.Refuse(link.ErrNotFound, "no card %s", r.ID)
	case errors.Is(err, ask.ErrNotYours):
		return link.Refuse(link.ErrDenied, "card %s was asked by another client", r.ID)
	case errors.Is(err, ask.ErrDone):
		return link.Refuse(link.ErrBadRequest, "card %s is not waiting: %v", r.ID, err)
	}
	return err
}

// readAnswer returns the question card r names, as ReadAnswer says.
func (s *Server) readAnswer(ctx context.Context, r Request) (*Card, error) {
	if r.ID == "" {
		return nil, link.Refuse(link.ErrBadRequest, "needs a card id")
	}
	q, err := s.Store.Question(ctx, r.ID)
	if errors.Is(err, store.ErrNotFound) {
		return nil, link.Refuse(link.ErrNotFound, "no card %s", r.ID)
	}
	if err != nil {
		return nil, err
	}
	c := &Card{ID: q.ID, Thread: q.Thread, TS: q.TS, Permalink: q.Permalink, AskedAt: q.AskedAt, Status: q.Status}
	if err := json.Unmarshal(q.Card, &c.Question); err != nil {
		return nil, err
	}
	if len(q.Answer) > 0 {
		c.Answer = q.Answer
	}
	return c, nil
}

// react adds or removes the bot's reaction on the first message of a
// thread, as React and Unreact say.
func (s *Server) react(ctx context.Context, r Request) error {
	channel, ts, err := thread(r)
	if err != nil {
		return err
	}
	if r.Name == "" || strings.ContainsAny(r.Name, ": ") {
		return link.Refuse(link.ErrBadRequest, "%q is not an emoji name without colons", r.Name)
	}
	if s.Slack == nil {
		return errNoSlack
	}
	if r.Cmd == React {
		err = s.Slack.React(ctx, channel, ts, r.Name)
	} else {
		err = s.Slack.Unreact(ctx, channel, ts, r.Name)
	}
	switch {
	case errors.Is(err, slack.ErrNotFound):
		return link.Refuse(link.ErrNotFound, "no thread %s", r.Thread)
	case slack.Code(err) == "missing_scope":
		return link.Refuse(link.ErrDenied, "the hub's bot token lacks the reactions:write scope; add it to the Slack app and reinstall the app")
	case slack.Code(err) == "invalid_name":
		return link.Refuse(link.ErrBadRequest, "Slack has no emoji %q", r.Name)
	}
	return err
}
