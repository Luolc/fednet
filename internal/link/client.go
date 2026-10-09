package link

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/Luolc/fednet/internal/store"
)

// Client is a client's end of the link to the hub. Run keeps the downlink
// connected and the uplink drained until its context is done; Post queues
// a message for the hub.
type Client struct {
	Store *store.Client
	// ID is sent to the hub in ClientHeader.
	ID string
	// Header is sent to the hub on every request, in addition to
	// ClientHeader; the credential goes here.
	Header http.Header
	// Hub is the hub's base URL, such as http://fednet-hub:8080.
	Hub string
	// Heartbeat is the interval between pings on the downlink. Zero means
	// DefaultHeartbeat.
	Heartbeat time.Duration
	// Backoff paces reconnects and uplink retries. Zero means DefaultBackoff.
	Backoff Backoff
	// Timeout bounds one dial, one uplink request and the wait for one
	// pong. Zero means DefaultTimeout.
	Timeout time.Duration
	// HTTPClient is used for both links. Nil means http.DefaultClient.
	HTTPClient *http.Client

	// nudge has a buffer of one, so a Post is noticed even while the uplink
	// loop is busy.
	nudge     chan struct{}
	nudgeOnce sync.Once
}

// Defaults for the zero fields of Client.
const (
	DefaultHeartbeat = 10 * time.Second
	DefaultTimeout   = 30 * time.Second
)

// DefaultBackoff is the Backoff used when Client.Backoff is zero.
var DefaultBackoff = Backoff{Min: time.Second, Max: time.Minute}

func (c *Client) heartbeat() time.Duration {
	if c.Heartbeat == 0 {
		return DefaultHeartbeat
	}
	return c.Heartbeat
}

func (c *Client) backoff() Backoff {
	if c.Backoff == (Backoff{}) {
		return DefaultBackoff
	}
	return c.Backoff
}

func (c *Client) timeout() time.Duration {
	if c.Timeout == 0 {
		return DefaultTimeout
	}
	return c.Timeout
}

func (c *Client) httpClient() *http.Client {
	if c.HTTPClient == nil {
		return http.DefaultClient
	}
	return c.HTTPClient
}

func (c *Client) header() http.Header {
	h := c.Header.Clone()
	if h == nil {
		h = http.Header{}
	}
	h.Set(ClientHeader, c.ID)
	return h
}

// Post queues payload for the hub and returns its msg_id. The message is
// sent by Run, now if it is running, otherwise when it next starts.
func (c *Client) Post(ctx context.Context, payload []byte) (string, error) {
	if len(payload) > MaxPayload {
		return "", ErrPayloadTooBig
	}
	id, err := c.Store.Outbox.Enqueue(ctx, payload)
	if err != nil {
		return "", err
	}
	select {
	case c.nudgeCh() <- struct{}{}:
	default:
	}
	return id, nil
}

func (c *Client) nudgeCh() chan struct{} {
	c.nudgeOnce.Do(func() { c.nudge = make(chan struct{}, 1) })
	return c.nudge
}

// Run serves both links until ctx is done. It must be called once.
func (c *Client) Run(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.runUplink(ctx)
	}()
	c.runDownlink(ctx)
	<-done
}

// runDownlink dials the hub and, when the connection drops, dials again
// after a backoff that resets once a dial succeeds.
func (c *Client) runDownlink(ctx context.Context) {
	for n := 0; ctx.Err() == nil; n++ {
		if err := c.downlink(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, errDialed) {
				n = 0
			}
			slog.Warn("link: downlink", "err", err)
		}
		c.backoff().sleep(ctx, n)
	}
}

// errDialed wraps an error that happened after a successful dial.
var errDialed = errors.New("after dial")

// downlink runs one connection: stores every frame, acks it, and pings on
// each heartbeat. It returns when the connection fails or ctx is done.
func (c *Client) downlink(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	url := "ws" + strings.TrimPrefix(c.Hub, "http") + DownlinkPath
	dctx, cancelDial := context.WithTimeout(ctx, c.timeout())
	conn, _, err := websocket.Dial(dctx, url, &websocket.DialOptions{HTTPClient: c.httpClient(), HTTPHeader: c.header()})
	cancelDial()
	if err != nil {
		return err
	}
	defer conn.CloseNow()
	conn.SetReadLimit(maxFrameBytes)

	errc := make(chan error, 2)
	go func() { errc <- c.ping(ctx, conn) }()
	go func() { errc <- c.receive(ctx, conn) }()
	return fmt.Errorf("%w: %w", errDialed, <-errc)
}

func (c *Client) ping(ctx context.Context, conn *websocket.Conn) error {
	t := time.NewTicker(c.heartbeat())
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-ctx.Done():
			return ctx.Err()
		}
		pctx, cancel := context.WithTimeout(ctx, c.timeout())
		err := conn.Ping(pctx)
		cancel()
		if err != nil {
			return err
		}
	}
}

func (c *Client) receive(ctx context.Context, conn *websocket.Conn) error {
	for {
		var d downlink
		if err := wsjson.Read(ctx, conn, &d); err != nil {
			return err
		}
		if _, err := c.Store.Inbox.Put(ctx, store.Message{MsgID: d.MsgID, Payload: d.Payload}); err != nil {
			return err
		}
		if err := wsjson.Write(ctx, conn, ack{Seq: d.Seq}); err != nil {
			return err
		}
	}
}

// runUplink sends the outbox, oldest first, whenever there is something in
// it, retrying each message with backoff until the hub has stored it.
func (c *Client) runUplink(ctx context.Context) {
	for ctx.Err() == nil {
		ms, err := c.Store.Outbox.Pending(ctx)
		if err != nil {
			slog.Warn("link: uplink", "err", err)
			ms = nil
		}
		for _, m := range ms {
			for n := 0; ctx.Err() == nil; n++ {
				if err := c.send(ctx, m); err == nil {
					break
				} else {
					slog.Warn("link: uplink", "msg_id", m.MsgID, "err", err)
				}
				c.backoff().sleep(ctx, n)
			}
			if ctx.Err() != nil {
				return
			}
			if err := c.Store.Outbox.Ack(ctx, m.MsgID); err != nil {
				slog.Warn("link: uplink", "msg_id", m.MsgID, "err", err)
			}
		}
		if ms == nil {
			select {
			case <-c.nudgeCh():
			case <-ctx.Done():
			}
		}
	}
}

// send POSTs one message and returns nil only when the hub replied 204.
func (c *Client) send(ctx context.Context, m store.Message) error {
	body, err := json.Marshal(uplink{MsgID: m.MsgID, Payload: m.Payload})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Hub+UplinkPath, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header = c.header()
	req.Header.Set("Content-Type", "application/json")
	res, err := c.httpClient().Do(req)
	if err != nil {
		return err
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		return fmt.Errorf("hub replied %s", res.Status)
	}
	return nil
}

// Request sends req to the hub and returns the hub's answer. Unlike Post it
// queues nothing: when the hub cannot be reached, or does not answer within
// the timeout, it fails at once with an error that wraps ErrUnreachable. A
// refusal comes back as an error that wraps ErrBadRequest, ErrDenied or
// ErrNotFound; a request the hub does not accept the credential for wraps
// ErrDenied.
func (c *Client) Request(ctx context.Context, req []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout())
	defer cancel()
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Hub+RequestPath, bytes.NewReader(req))
	if err != nil {
		return nil, err
	}
	hreq.Header = c.header()
	hreq.Header.Set("Content-Type", "application/json")
	res, err := c.httpClient().Do(hreq)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnreachable, err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, maxAnswerBytes))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnreachable, err)
	}
	msg := strings.TrimSpace(string(body))
	switch res.StatusCode {
	case http.StatusOK:
		return body, nil
	case http.StatusUnauthorized:
		return nil, refusal{ErrDenied, "the hub did not accept this client's credential"}
	}
	for _, f := range refusals {
		if res.StatusCode == f.status {
			return nil, refusal{f.err, msg}
		}
	}
	return nil, fmt.Errorf("hub replied %s: %s", res.Status, msg)
}

// Refuse returns the error with which Hub.Answer refuses a request: kind is
// ErrBadRequest, ErrDenied or ErrNotFound, and the message goes back to the
// client.
func Refuse(kind error, format string, args ...any) error {
	return refusal{kind, fmt.Sprintf(format, args...)}
}

// refusal is a request the hub refused: it wraps the kind of refusal and
// reads as the hub's message.
type refusal struct {
	kind error
	msg  string
}

func (r refusal) Error() string { return r.msg }
func (r refusal) Unwrap() error { return r.kind }
