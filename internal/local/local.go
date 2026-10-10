// Package local is the unix socket through which the agents on a machine
// hand requests to the client daemon. Agents never see the client's
// credential; who may connect is decided by the socket file's mode and
// group. Each connection carries one request: the caller writes one JSON
// Request, the daemon replies with one JSON Response and closes.
package local

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/Luolc/fednet/internal/hubapi"
	"github.com/Luolc/fednet/internal/link"
	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/slack"
)

// Commands a Request can carry besides those of hubapi, which the daemon
// hands to the hub and answers with the hub's reply.
const (
	// Post queues a message for a thread and replies with its msg_id
	// without waiting for the hub.
	Post = "post"
	// Progress queues the thread's progress card, to set or to close, as
	// a Post is queued, and replies with its msg_id.
	Progress = "progress"
	// Handoff starts a new process of the daemon that takes over this
	// socket, and replies once it is ready, with the version and pid of the
	// process that answered, or with why the new process did not start.
	Handoff = "handoff"
	// Version replies with the version and pid of the process that answered.
	Version = "version"
	// FetchFile fetches the Slack file with id File through the hub into
	// the client's files directory, unless it is there already, and
	// replies with its path.
	FetchFile = "fetch-file"
)

// Request is what a caller sends. Cmd selects the command; the other fields
// are its arguments.
type Request struct {
	Cmd     string `json:"cmd"`
	Thread  string `json:"thread,omitempty"`
	Channel string `json:"channel,omitempty"`
	Text    string `json:"text,omitempty"`
	User    string `json:"user,omitempty"`
	// Agent, Requester and Action are the arguments of
	// hubapi.RequestApproval.
	Agent     string `json:"agent,omitempty"`
	Requester string `json:"requester,omitempty"`
	Action    []byte `json:"action,omitempty"`
	// File is the Slack file id FetchFile takes.
	File string `json:"file,omitempty"`
	// Files are the files a Post uploads: their content follows the
	// request's JSON value on the connection, right after it, in order,
	// each Size bytes. A post with files is not queued: the daemon hands
	// it to the hub and replies once the hub has posted it in Slack.
	Files []link.FileHeader `json:"files,omitempty"`
	// Footer makes a Post one line of small grey text.
	Footer bool `json:"footer,omitempty"`
	// Title, Items and Close are the arguments of Progress, as
	// payload.Message has them.
	Title string               `json:"title,omitempty"`
	Items []slack.ProgressItem `json:"items,omitempty"`
	Close string               `json:"close,omitempty"`
}

// Response is the daemon's reply. Error is set when the request failed, and
// Kind says why when the reason is one of the Kind constants.
type Response struct {
	MsgID      string          `json:"msg_id,omitempty"`
	Messages   []slack.Message `json:"messages,omitempty"`
	Text       string          `json:"text,omitempty"`
	Thread     string          `json:"thread,omitempty"`
	Threads    []string        `json:"threads,omitempty"`
	Users      []hubapi.User   `json:"users,omitempty"`
	ApprovalID string          `json:"approval_id,omitempty"`
	Path       string          `json:"path,omitempty"`
	Version    string          `json:"version,omitempty"`
	PID        int             `json:"pid,omitempty"`
	Error      string          `json:"error,omitempty"`
	Kind       string          `json:"kind,omitempty"`
}

// Why a request failed, for Response.Kind.
const (
	// BadRequest is a request that is wrong in itself.
	BadRequest = "bad_request"
	// Denied is a request the hub does not allow this client.
	Denied = "denied"
	// NotFound is a request for a thread or channel that does not exist.
	NotFound = "not_found"
	// Unreachable is a request for the hub when the hub cannot be reached.
	Unreachable = "unreachable"
)

// Timeout bounds one request on either side, except a Handoff, which is
// bounded by how long the daemon gives the new process to become ready,
// and a FetchFile or a Post with files, which get FileTimeout: a transfer
// takes longer. The request itself, before the daemon knows which it is,
// must arrive within headerTimeout.
const (
	Timeout     = 10 * time.Second
	FileTimeout = 5 * time.Minute
)

// headerTimeout bounds the reading of the request; tests shorten it.
var headerTimeout = Timeout

// timeout returns ctx bounded for req.
func timeout(ctx context.Context, req Request) (context.Context, context.CancelFunc) {
	switch {
	case req.Cmd == Handoff:
		return context.WithCancel(ctx)
	case req.Cmd == FetchFile || len(req.Files) > 0:
		return context.WithTimeout(ctx, FileTimeout)
	}
	return context.WithTimeout(ctx, Timeout)
}

// hubTimeout bounds the hub's part of a request, so that a hub that does
// not answer is reported before the request itself times out.
const hubTimeout = Timeout / 2

// maxRequestBytes bounds a request: a post's text may grow up to six times
// when escaped as a JSON string.
const maxRequestBytes = 6*link.MaxPayload + 1024

// Listen creates the socket at path. Only one client at a time owns a path:
// Listen takes an exclusive lock on path + ".lock" and holds it until the
// listener is closed or the process exits, and fails if another client
// holds it. A socket already at path is then left over from a client that
// stopped, and is replaced; any other file there makes Listen fail. Without
// group only this Unix user can connect (mode 0600); with group, members of
// that group can too (mode 0660), and this user must be a member of it.
func Listen(path, group string) (*Socket, error) {
	mode, gid := os.FileMode(0o600), -1
	if group != "" {
		g, err := user.LookupGroup(group)
		if err != nil {
			return nil, err
		}
		if gid, err = strconv.Atoi(g.Gid); err != nil {
			return nil, err
		}
		mode = 0o660
	}
	lock, err := os.OpenFile(lockPath(path), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("local: another client is using %s", path)
		}
		return nil, err
	}
	ln, err := listen(path, mode, gid)
	if err != nil {
		lock.Close()
		return nil, err
	}
	return &Socket{Listener: ln, lock: lock}, nil
}

func lockPath(path string) string { return path + ".lock" }

// Socket is the socket's listener together with the lock that makes the
// path this process's. Closing it releases the lock, unless another process
// holds a copy of the lock file: the lock belongs to the open file, not to
// the descriptor.
type Socket struct {
	net.Listener
	lock *os.File
}

// Inherit returns the Socket for a listener and lock file taken over from
// the process that created them.
func Inherit(ln net.Listener, lock *os.File) *Socket {
	return &Socket{Listener: ln, lock: lock}
}

// Lock is the lock file, for passing to a process that takes the socket
// over.
func (s *Socket) Lock() *os.File { return s.lock }

// SyscallConn exposes the listening socket, for passing it to another
// process.
func (s *Socket) SyscallConn() (syscall.RawConn, error) {
	return s.Listener.(syscall.Conn).SyscallConn()
}

func (s *Socket) Close() error {
	err := s.Listener.Close()
	s.lock.Close()
	return err
}

// listen creates the socket at path; the caller holds the lock.
func listen(path string, mode os.FileMode, gid int) (net.Listener, error) {
	if fi, err := os.Lstat(path); err == nil && fi.Mode().Type() != os.ModeSocket {
		return nil, fmt.Errorf("local: %s exists and is not a socket", path)
	} else if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	// The socket is created in a private directory and moved into place
	// once its mode and group are set, so no one else can ever connect to
	// it with the looser mode it is created with.
	dir, err := os.MkdirTemp(filepath.Dir(path), ".fednet-socket-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	tmp := filepath.Join(dir, "socket")
	ln, err := net.Listen("unix", tmp)
	if err != nil {
		return nil, err
	}
	if err := setup(tmp, path, mode, gid); err != nil {
		ln.Close()
		return nil, err
	}
	return ln, nil
}

func setup(tmp, path string, mode os.FileMode, gid int) error {
	if err := os.Lchown(tmp, -1, gid); err != nil {
		return err
	}
	if err := os.Chmod(tmp, mode); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Server answers requests on the socket. A command whose field is nil is
// refused as a bad request: the hub's admin socket serves no Post.
type Server struct {
	// Machine is the client's id, which the hub adds at the end of a
	// footer.
	Machine string
	// Post queues a payload for the hub and returns its msg_id; it is
	// link.Client.Post.
	Post func(ctx context.Context, payload []byte) (string, error)
	// Request sends a request to the hub and returns its answer; it is
	// link.Client.Request.
	Request func(ctx context.Context, req []byte) ([]byte, error)
	// Handoff starts the new process and returns once it is ready, or with
	// why it is not.
	Handoff func(ctx context.Context) error
	// Version is this process's version, for Version and Handoff.
	Version string
	// Fetch fetches the file with the given Slack id into the files
	// directory and returns its path; nil means FetchFile is not served.
	Fetch func(ctx context.Context, id string) (string, error)
	// Upload hands a post with files to the hub, reading their content
	// from body; it is link.Client.Upload. Nil means such a post is not
	// served.
	Upload func(ctx context.Context, u link.Upload, body io.Reader) error
}

// Serve answers connections on ln until ctx is done. When it returns it has
// closed ln and waited for the requests in flight. It returns nil once ctx
// is done, or the error that stopped it accepting.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	var wg sync.WaitGroup
	defer wg.Wait()
	defer ln.Close()
	stop := context.AfterFunc(ctx, func() { ln.Close() })
	defer stop()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		wg.Go(func() { s.serve(ctx, conn) })
	}
}

func (s *Server) serve(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(headerTimeout))
	var req Request
	var res Response
	// The request is bounded; the files' content after it is bounded by
	// the sizes the request declares.
	dec := json.NewDecoder(io.LimitReader(conn, maxRequestBytes))
	if err := dec.Decode(&req); err != nil {
		res = badRequest("unreadable request: " + err.Error())
	} else {
		ctx, cancel := timeout(ctx, req)
		defer cancel()
		// From here the request's own deadline bounds the reading: a
		// post's files take longer than the request did.
		d, _ := ctx.Deadline()
		conn.SetReadDeadline(d)
		stop := context.AfterFunc(ctx, func() { conn.SetDeadline(time.Now()) })
		defer stop()
		res = s.handle(ctx, req, io.MultiReader(dec.Buffered(), conn))
	}
	conn.SetWriteDeadline(time.Now().Add(Timeout))
	if err := json.NewEncoder(conn).Encode(res); err != nil {
		slog.Warn("local: reply", "cmd", req.Cmd, "err", err)
	}
}

func badRequest(msg string) Response { return Response{Error: msg, Kind: BadRequest} }

// handle runs one request; body is what follows it on the connection, the
// content of a post's files. A new command is a new case here.
func (s *Server) handle(ctx context.Context, req Request, body io.Reader) Response {
	switch req.Cmd {
	case Post:
		if len(req.Files) > 0 {
			if s.Upload == nil {
				return badRequest("post with files is not served on this socket")
			}
			return s.upload(ctx, req, body)
		}
		if s.Post == nil {
			return badRequest("post is not served on this socket")
		}
		return s.post(ctx, req)
	case Progress:
		if s.Post == nil {
			return badRequest("progress is not served on this socket")
		}
		return s.progress(ctx, req)
	case hubapi.ReadThread, hubapi.OpenThread, hubapi.Threads, hubapi.Adopt, hubapi.GetChannelContext, hubapi.SetChannelContext,
		hubapi.Users, hubapi.DM, hubapi.RequestApproval:
		if s.Request == nil {
			return badRequest(req.Cmd + " is not served on this socket")
		}
		return s.ask(ctx, req)
	case Handoff:
		if s.Handoff == nil {
			return badRequest("handoff is not served on this socket")
		}
		if err := s.Handoff(ctx); err != nil {
			return Response{Error: "handoff: " + err.Error()}
		}
		return s.version()
	case Version:
		return s.version()
	case FetchFile:
		if s.Fetch == nil {
			return badRequest("fetch-file is not served on this socket")
		}
		return s.fetchFile(ctx, req)
	default:
		return badRequest(fmt.Sprintf("unknown command %q", req.Cmd))
	}
}

func (s *Server) version() Response { return Response{Version: s.Version, PID: os.Getpid()} }

func (s *Server) post(ctx context.Context, req Request) Response {
	if req.Thread == "" || req.Text == "" {
		return badRequest("post needs a thread and a text")
	}
	if req.Footer && !slack.FooterFits(slack.MachineFooter(req.Text, s.Machine)) {
		return badRequest(fmt.Sprintf("post: a footer takes at most %d characters, links written out and the machine's name added", slack.MaxFooterChars))
	}
	return s.queue(ctx, req.Cmd, payload.Message{Type: payload.Post, Thread: req.Thread, Text: req.Text, Footer: req.Footer})
}

func (s *Server) progress(ctx context.Context, req Request) Response {
	if req.Thread == "" {
		return badRequest("progress needs a thread")
	}
	if req.Close != "" && req.Close != slack.Done && req.Close != slack.Failed {
		return badRequest(fmt.Sprintf("progress: close %q is not %s or %s", req.Close, slack.Done, slack.Failed))
	}
	if err := slack.CheckProgress(req.Title, req.Items, req.Close != ""); err != nil {
		return badRequest("progress: " + err.Error())
	}
	return s.queue(ctx, req.Cmd, payload.Message{Type: payload.Progress, Thread: req.Thread, Title: req.Title, Items: req.Items, Close: req.Close})
}

// queue queues m for the hub and replies with its msg_id.
func (s *Server) queue(ctx context.Context, cmd string, m payload.Message) Response {
	p, err := json.Marshal(m)
	if err != nil {
		return Response{Error: err.Error()}
	}
	id, err := s.Post(ctx, p)
	if errors.Is(err, link.ErrPayloadTooBig) {
		return badRequest(cmd + ": too long")
	}
	if err != nil {
		slog.Warn("local: "+cmd, "err", err)
		return Response{Error: cmd + ": " + err.Error()}
	}
	return Response{MsgID: id}
}

// upload hands a post with files to the hub and waits for it; the hub's
// refusals come back with their kind, as a request's do.
func (s *Server) upload(ctx context.Context, req Request, body io.Reader) Response {
	if req.Thread == "" {
		return badRequest("post needs a thread")
	}
	u := link.Upload{Thread: req.Thread, Text: req.Text, Files: req.Files}
	if err := s.Upload(ctx, u, io.LimitReader(body, u.Total())); err != nil {
		return failed(req.Cmd, err)
	}
	return Response{}
}

func (s *Server) fetchFile(ctx context.Context, req Request) Response {
	if req.File == "" {
		return badRequest("fetch-file needs a file id")
	}
	path, err := s.Fetch(ctx, req.File)
	if err != nil {
		return failed(req.Cmd, err)
	}
	return Response{Path: path}
}

// failed is the Response for a request the hub or the link failed, with
// the Kind the error maps to.
func failed(cmd string, err error) Response {
	res := Response{Error: cmd + ": " + err.Error()}
	for _, k := range kinds {
		if errors.Is(err, k.err) {
			res.Kind = k.kind
			break
		}
	}
	return res
}

// kinds maps the link's errors to the Kind they are reported with.
var kinds = []struct {
	err  error
	kind string
}{
	{link.ErrBadRequest, BadRequest},
	{link.ErrDenied, Denied},
	{link.ErrNotFound, NotFound},
	{link.ErrUnreachable, Unreachable},
}

// ask hands req to the hub and replies with the hub's answer.
func (s *Server) ask(ctx context.Context, req Request) Response {
	b, err := json.Marshal(hubapi.Request{Cmd: req.Cmd, Thread: req.Thread, Channel: req.Channel, Text: req.Text, User: req.User,
		Agent: req.Agent, Requester: req.Requester, Action: req.Action})
	if err != nil {
		return Response{Error: err.Error()}
	}
	ctx, cancel := context.WithTimeout(ctx, hubTimeout)
	defer cancel()
	answer, err := s.Request(ctx, b)
	if err != nil {
		return failed(req.Cmd, err)
	}
	var reply hubapi.Reply
	if err := json.Unmarshal(answer, &reply); err != nil {
		return Response{Error: req.Cmd + ": unreadable answer from the hub: " + err.Error()}
	}
	return Response{Messages: reply.Messages, Text: reply.Text, Thread: reply.Thread, Threads: reply.Threads, Users: reply.Users, ApprovalID: reply.ApprovalID}
}

// Do sends req to the socket at path and returns the daemon's response. An
// error means the daemon could not be reached or did not reply; a failed
// request is a Response with Error set. files is the content of
// req.Files, one reader each, which Do sends after the request, each its
// Size bytes; a reader that ends before that is an error.
func Do(ctx context.Context, path string, req Request, files ...io.Reader) (Response, error) {
	if len(files) != len(req.Files) {
		return Response{}, fmt.Errorf("local: %d files declared, %d given", len(req.Files), len(files))
	}
	ctx, cancel := timeout(ctx, req)
	defer cancel()
	var d net.Dialer
	c, err := d.DialContext(ctx, "unix", path)
	if err != nil {
		return Response{}, err
	}
	conn := c.(*net.UnixConn)
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.SetDeadline(time.Now()) })
	defer stop()
	// The content starts right after the JSON value: no newline between.
	b, err := json.Marshal(req)
	if err != nil {
		return Response{}, err
	}
	if _, err := conn.Write(b); err != nil {
		return Response{}, err
	}
	// The daemon may answer before it has read all the content, with a
	// refusal; that answer is what the caller needs, not the write error
	// that follows it. So the content is sent while the answer is awaited,
	// and the sending is given up once an answer is in. A source that
	// ends short or fails to read is the caller's error: the sending side
	// is closed so that the daemon stops waiting for the rest, and that
	// error is returned whatever the daemon then says.
	sent := make(chan error, 1)
	go func() {
		for i, f := range files {
			_, err := io.CopyN(conn, &source{r: f}, req.Files[i].Size)
			if err == nil {
				continue
			}
			var se *sourceError
			switch {
			case errors.Is(err, io.EOF):
				err = io.ErrUnexpectedEOF
				fallthrough
			case errors.As(err, &se):
				conn.CloseWrite()
				sent <- &sourceError{fmt.Errorf("local: sending %s: %w", req.Files[i].Name, err)}
			default:
				sent <- fmt.Errorf("local: sending %s: %w", req.Files[i].Name, err)
			}
			return
		}
		sent <- nil
	}()
	var res Response
	err = json.NewDecoder(conn).Decode(&res)
	// Whether the decode succeeded or not, the sender is ended: with an
	// answer in, nothing more is read on the other side.
	conn.Close()
	serr := <-sent
	var se *sourceError
	switch {
	case errors.As(serr, &se):
		return Response{}, serr
	case err != nil && serr != nil:
		return Response{}, serr
	case err != nil:
		return Response{}, err
	}
	return res, nil
}

// source tells a failure of the caller's reader apart from a failure of
// the connection it is copied to.
type source struct{ r io.Reader }

func (s *source) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	if err != nil && err != io.EOF {
		err = &sourceError{err}
	}
	return n, err
}

type sourceError struct{ error }

func (e *sourceError) Unwrap() error { return e.error }
