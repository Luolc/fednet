package link

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/Luolc/fednet/internal/store"
)

// Hub is the hub's end of the links to all clients. Its Handler serves the
// downlink WebSocket and the uplink POST; Send queues a message for a client
// and pushes it over the client's connection, if there is one.
type Hub struct {
	Store *store.Hub
	// Lease is how long after its last heartbeat a client counts as online.
	// Zero means DefaultLease.
	Lease time.Duration
	// Identify returns the id of the client behind r. Nil means the id is
	// read from ClientHeader as is.
	Identify func(r *http.Request) (string, error)
	// Recheck is how often Identify runs again on an open downlink's
	// handshake, so a client revoked or re-registered while connected is
	// dropped. Zero means DefaultRecheck.
	Recheck time.Duration
	// Answer answers a request from client. An error made by Refuse goes
	// back to the client; any other error is logged and the client only
	// learns that the request failed. Nil means every request fails.
	Answer func(ctx context.Context, client string, req []byte) ([]byte, error)

	mu       sync.Mutex
	seen     map[string]time.Time
	sessions map[string]*session
	closed   bool
	// wg counts registered sessions; register adds under mu, so no session
	// is added once Close has started waiting.
	wg sync.WaitGroup
}

// Defaults for the zero fields of Hub.
const (
	DefaultLease   = 30 * time.Second
	DefaultRecheck = DefaultHeartbeat
)

// session is one client's open downlink connection.
type session struct {
	conn *websocket.Conn
	// wake has a buffer of one, so a nudge is kept until the writer looks.
	wake chan struct{}
}

func (h *Hub) lease() time.Duration {
	if h.Lease == 0 {
		return DefaultLease
	}
	return h.Lease
}

func (h *Hub) recheckEvery() time.Duration {
	if h.Recheck == 0 {
		return DefaultRecheck
	}
	return h.Recheck
}

// authorize identifies the client behind r. When it cannot, it logs why,
// replies 401 with a fixed body, and reports false. The reason stays out of
// the reply: it is for the hub's operator, not for whoever sent the request.
func (h *Hub) authorize(w http.ResponseWriter, r *http.Request) (string, bool) {
	var client string
	var err error
	if h.Identify != nil {
		client, err = h.Identify(r)
	} else if client = r.Header.Get(ClientHeader); client == "" {
		err = errors.New("missing " + ClientHeader + " header")
	}
	if err != nil {
		slog.Warn("link: unauthorized", "path", r.URL.Path, "err", err)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return "", false
	}
	return client, true
}

// Handler serves DownlinkPath, UplinkPath and RequestPath.
func (h *Hub) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+DownlinkPath, h.serveDownlink)
	mux.HandleFunc("POST "+UplinkPath, h.serveUplink)
	mux.HandleFunc("POST "+RequestPath, h.serveRequest)
	return mux
}

// Send queues payload for client and pushes it if client is connected.
func (h *Hub) Send(ctx context.Context, client string, payload []byte) (store.Downlink, error) {
	if len(payload) > MaxPayload {
		return store.Downlink{}, ErrPayloadTooBig
	}
	d, err := h.Store.Outbox.Enqueue(ctx, client, payload)
	if err != nil {
		return d, err
	}
	h.mu.Lock()
	s := h.sessions[client]
	h.mu.Unlock()
	if s != nil {
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
	return d, nil
}

// Online reports whether client has sent a heartbeat within the lease.
func (h *Hub) Online(client string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return time.Since(h.seen[client]) < h.lease()
}

func (h *Hub) heartbeat(client string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.seen == nil {
		h.seen = make(map[string]time.Time)
	}
	h.seen[client] = time.Now()
}

// Close drops every open downlink connection, refuses new ones, and waits
// for the dropped connections' handlers.
func (h *Hub) Close() {
	h.mu.Lock()
	h.closed = true
	open := make([]*session, 0, len(h.sessions))
	for _, s := range h.sessions {
		open = append(open, s)
	}
	h.mu.Unlock()
	// Closing a connection waits for its reader, and the reader may be in
	// the ping callback waiting for h.mu, so close outside the lock.
	for _, s := range open {
		s.conn.CloseNow()
	}
	h.wg.Wait()
}

// errClosed is returned by register after Close.
var errClosed = errors.New("hub closed")

// register makes s the client's session, dropping any earlier one.
func (h *Hub) register(client string, s *session) error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return errClosed
	}
	old := h.sessions[client]
	if h.sessions == nil {
		h.sessions = make(map[string]*session)
	}
	h.sessions[client] = s
	h.wg.Add(1)
	h.mu.Unlock()
	if old != nil {
		old.conn.CloseNow()
	}
	return nil
}

func (h *Hub) unregister(client string, s *session) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.sessions[client] == s {
		delete(h.sessions, client)
	}
	h.wg.Done()
}

func (h *Hub) serveDownlink(w http.ResponseWriter, r *http.Request) {
	client, ok := h.authorize(w, r)
	if !ok {
		return
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OnPingReceived: func(context.Context, []byte) bool {
			h.heartbeat(client)
			return true
		},
	})
	if err != nil {
		return
	}
	defer conn.CloseNow()
	s := &session{conn: conn, wake: make(chan struct{}, 1)}
	if err := h.register(client, s); err != nil {
		conn.Close(websocket.StatusGoingAway, err.Error())
		return
	}
	defer h.unregister(client, s)
	h.heartbeat(client)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	errc := make(chan error, 3)
	go func() { errc <- h.push(ctx, client, s) }()
	go func() { errc <- h.readAcks(ctx, client, conn) }()
	if h.Identify != nil {
		go func() { errc <- h.recheck(ctx, r) }()
	}
	if err := <-errc; err != nil && ctx.Err() == nil && websocket.CloseStatus(err) == -1 {
		slog.Warn("link: downlink closed", "client", client, "err", err)
	}
}

// recheck identifies the client behind r again every Recheck and returns
// once it cannot, which ends the connection. The registry may have changed
// since the handshake: hub revoke and hub register are other processes.
func (h *Hub) recheck(ctx context.Context, r *http.Request) error {
	t := time.NewTicker(h.recheckEvery())
	defer t.Stop()
	r = r.WithContext(ctx)
	for {
		select {
		case <-t.C:
		case <-ctx.Done():
			return ctx.Err()
		}
		if _, err := h.Identify(r); err != nil {
			return fmt.Errorf("link: no longer authorized: %w", err)
		}
	}
}

// push writes client's queued messages in seq order, then whatever is
// queued after each wake.
func (h *Hub) push(ctx context.Context, client string, s *session) error {
	var last int64
	for {
		ds, err := h.Store.Outbox.After(ctx, client, last)
		if err != nil {
			return err
		}
		for _, d := range ds {
			if err := wsjson.Write(ctx, s.conn, downlink{Seq: d.Seq, MsgID: d.MsgID, Payload: d.Payload}); err != nil {
				return err
			}
			last = d.Seq
		}
		select {
		case <-s.wake:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// readAcks removes acked messages from client's outbox.
func (h *Hub) readAcks(ctx context.Context, client string, conn *websocket.Conn) error {
	for {
		var a ack
		if err := wsjson.Read(ctx, conn, &a); err != nil {
			return err
		}
		if err := h.Store.Outbox.Ack(ctx, client, a.Seq); err != nil {
			return err
		}
	}
}

// serveUplink stores one message with its sender and replies 204 once it
// is on disk.
func (h *Hub) serveUplink(w http.ResponseWriter, r *http.Request) {
	client, ok := h.authorize(w, r)
	if !ok {
		return
	}
	var u uplink
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxFrameBytes)).Decode(&u); err != nil || u.MsgID == "" {
		http.Error(w, "bad uplink body", http.StatusBadRequest)
		return
	}
	if _, err := h.Store.Inbox.PutFrom(r.Context(), client, store.Message{MsgID: u.MsgID, Payload: u.Payload}); err != nil {
		slog.Warn("link: uplink store", "err", err)
		http.Error(w, "store failed", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// refusals maps each refusal to the status it is sent with.
var refusals = []struct {
	err    error
	status int
}{
	{ErrBadRequest, http.StatusBadRequest},
	{ErrDenied, http.StatusForbidden},
	{ErrNotFound, http.StatusNotFound},
}

// serveRequest answers one request with 200 and the answer, or with the
// status of a refusal and its text.
func (h *Hub) serveRequest(w http.ResponseWriter, r *http.Request) {
	client, ok := h.authorize(w, r)
	if !ok {
		return
	}
	req, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxFrameBytes))
	if err != nil {
		http.Error(w, "bad request body", http.StatusBadRequest)
		return
	}
	if h.Answer == nil {
		http.Error(w, "the hub answers no requests", http.StatusInternalServerError)
		return
	}
	answer, err := h.Answer(r.Context(), client, req)
	var rf refusal
	if errors.As(err, &rf) {
		for _, f := range refusals {
			if rf.kind == f.err {
				http.Error(w, rf.msg, f.status)
				return
			}
		}
	}
	if err != nil {
		slog.Warn("link: request", "client", client, "err", err)
		http.Error(w, "the hub failed to answer", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(answer)
}
