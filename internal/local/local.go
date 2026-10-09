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
)

// Request is what a caller sends. Cmd selects the command; the other fields
// are its arguments.
type Request struct {
	Cmd     string `json:"cmd"`
	Thread  string `json:"thread,omitempty"`
	Channel string `json:"channel,omitempty"`
	Text    string `json:"text,omitempty"`
	User    string `json:"user,omitempty"`
}

// Response is the daemon's reply. Error is set when the request failed, and
// Kind says why when the reason is one of the Kind constants.
type Response struct {
	MsgID    string          `json:"msg_id,omitempty"`
	Messages []slack.Message `json:"messages,omitempty"`
	Text     string          `json:"text,omitempty"`
	Thread   string          `json:"thread,omitempty"`
	Threads  []string        `json:"threads,omitempty"`
	Users    []hubapi.User   `json:"users,omitempty"`
	Error    string          `json:"error,omitempty"`
	Kind     string          `json:"kind,omitempty"`
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

// Timeout bounds one request on either side.
const Timeout = 10 * time.Second

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
func Listen(path, group string) (net.Listener, error) {
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
	return &listener{Listener: ln, lock: lock}, nil
}

func lockPath(path string) string { return path + ".lock" }

// listener is the socket's listener; closing it releases the lock.
type listener struct {
	net.Listener
	lock *os.File
}

func (l *listener) Close() error {
	err := l.Listener.Close()
	l.lock.Close()
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

// Server answers requests on the socket.
type Server struct {
	// Post queues a payload for the hub and returns its msg_id; it is
	// link.Client.Post.
	Post func(ctx context.Context, payload []byte) (string, error)
	// Request sends a request to the hub and returns its answer; it is
	// link.Client.Request.
	Request func(ctx context.Context, req []byte) ([]byte, error)
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
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { conn.SetDeadline(time.Now()) })
	defer stop()
	var req Request
	var res Response
	if err := json.NewDecoder(io.LimitReader(conn, maxRequestBytes)).Decode(&req); err != nil {
		res = badRequest("unreadable request: " + err.Error())
	} else {
		res = s.handle(ctx, req)
	}
	if err := json.NewEncoder(conn).Encode(res); err != nil {
		slog.Warn("local: reply", "cmd", req.Cmd, "err", err)
	}
}

func badRequest(msg string) Response { return Response{Error: msg, Kind: BadRequest} }

// handle runs one request. A new command is a new case here.
func (s *Server) handle(ctx context.Context, req Request) Response {
	switch req.Cmd {
	case Post:
		return s.post(ctx, req)
	case hubapi.ReadThread, hubapi.OpenThread, hubapi.Threads, hubapi.Adopt, hubapi.GetChannelContext, hubapi.SetChannelContext,
		hubapi.Users, hubapi.DM:
		return s.ask(ctx, req)
	default:
		return badRequest(fmt.Sprintf("unknown command %q", req.Cmd))
	}
}

func (s *Server) post(ctx context.Context, req Request) Response {
	if req.Thread == "" || req.Text == "" {
		return badRequest("post needs a thread and a text")
	}
	p, err := json.Marshal(payload.Message{Type: payload.Post, Thread: req.Thread, Text: req.Text})
	if err != nil {
		return Response{Error: err.Error()}
	}
	id, err := s.Post(ctx, p)
	if errors.Is(err, link.ErrPayloadTooBig) {
		return badRequest("post: text too long")
	}
	if err != nil {
		slog.Warn("local: post", "err", err)
		return Response{Error: "post: " + err.Error()}
	}
	return Response{MsgID: id}
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
	b, err := json.Marshal(hubapi.Request{Cmd: req.Cmd, Thread: req.Thread, Channel: req.Channel, Text: req.Text, User: req.User})
	if err != nil {
		return Response{Error: err.Error()}
	}
	ctx, cancel := context.WithTimeout(ctx, hubTimeout)
	defer cancel()
	answer, err := s.Request(ctx, b)
	if err != nil {
		res := Response{Error: req.Cmd + ": " + err.Error()}
		for _, k := range kinds {
			if errors.Is(err, k.err) {
				res.Kind = k.kind
				break
			}
		}
		return res
	}
	var reply hubapi.Reply
	if err := json.Unmarshal(answer, &reply); err != nil {
		return Response{Error: req.Cmd + ": unreadable answer from the hub: " + err.Error()}
	}
	return Response{Messages: reply.Messages, Text: reply.Text, Thread: reply.Thread, Threads: reply.Threads, Users: reply.Users}
}

// Do sends req to the socket at path and returns the daemon's response. An
// error means the daemon could not be reached or did not reply; a failed
// request is a Response with Error set.
func Do(ctx context.Context, path string, req Request) (Response, error) {
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", path)
	if err != nil {
		return Response{}, err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.SetDeadline(time.Now()) })
	defer stop()
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return Response{}, err
	}
	var res Response
	if err := json.NewDecoder(conn).Decode(&res); err != nil {
		return Response{}, err
	}
	return res, nil
}
