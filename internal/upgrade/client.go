package upgrade

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/release"
)

// Client is a client's end of an upgrade: its Divert, meant for
// link.Client.Divert, takes the hub's upgrade notices off the downlink
// before they reach the inbox, and writes each as the request file the
// machine's upgrader watches. The client program acts on the notice
// itself; no agent is involved.
type Client struct {
	// Version is this client's version.
	Version string
	// Request is the path of the upgrade request file; empty means this
	// client cannot be upgraded this way, which is reported like a
	// request that cannot be written.
	Request string
	// Alert, if set, reports a notice that could not be turned into a
	// request, once per release, to the hub, which alerts.
	Alert func(ctx context.Context, text string) error

	mu sync.Mutex
	// alerted is the release the last report that was queued was about.
	alerted string
}

// Divert reports whether p is an upgrade notice, taking it if so and
// handing the release it names to Notice.
func (c *Client) Divert(ctx context.Context, p []byte) bool {
	var m payload.Message
	if json.Unmarshal(p, &m) != nil || m.Type != payload.Upgrade {
		return false
	}
	c.Notice(ctx, m.Version)
	return true
}

// Notice acts on a notice to upgrade to version, which the hub sends as a
// downlink message or on the response to each dial: a release newer than
// Version is written as the request; one that is not newer is dropped
// with a log line. A notice that cannot be turned into a request is
// dropped too, and reported through Alert, once per release: the hub
// tells the client again each time it connects, and on its next check for
// a release.
func (c *Client) Notice(ctx context.Context, version string) {
	m := payload.Message{Version: version}
	if !release.Newer(m.Version, c.Version) {
		slog.Info("upgrade: not upgrading", "to", m.Version, "running", c.Version)
		return
	}
	var err error
	if c.Request == "" {
		err = errors.New("this client has no -upgrade-request")
	} else {
		err = WriteRequest(c.Request, m.Version)
	}
	if err == nil {
		slog.Info("upgrade: requested", "to", m.Version, "request", c.Request)
		return
	}
	slog.Warn("upgrade: the hub's notice could not be turned into a request", "to", m.Version, "err", err)
	if c.Alert == nil {
		return
	}
	// One report per release, counted once it is queued: a report that
	// could not be queued is tried again on the next notice.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.alerted == m.Version {
		return
	}
	if aerr := c.Alert(ctx, fmt.Sprintf("cannot upgrade from %s to %s: %v", c.Version, m.Version, err)); aerr != nil {
		slog.Warn("upgrade: reporting the failure to the hub", "err", aerr)
		return
	}
	c.alerted = m.Version
}
