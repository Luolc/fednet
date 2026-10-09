package upgrade

import (
	"context"
	"encoding/json"
	"log/slog"

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
	// client cannot be upgraded this way, and a notice is dropped with a
	// warning.
	Request string
}

// Divert reports whether p is an upgrade notice, taking it if so: a
// notice for a release newer than Version is written as the request; any
// other is dropped with a log line. A request that cannot be written is
// dropped too, and logged: the hub tells the client again when it next
// connects.
func (c *Client) Divert(_ context.Context, p []byte) bool {
	var m payload.Message
	if json.Unmarshal(p, &m) != nil || m.Type != payload.Upgrade {
		return false
	}
	switch {
	case !release.Newer(m.Version, c.Version):
		slog.Info("upgrade: not upgrading", "to", m.Version, "running", c.Version)
	case c.Request == "":
		slog.Warn("upgrade: the hub asks for an upgrade, but this client has no -upgrade-request", "to", m.Version)
	default:
		if err := WriteRequest(c.Request, m.Version); err != nil {
			slog.Warn("upgrade: the request could not be written", "to", m.Version, "err", err)
		} else {
			slog.Info("upgrade: requested", "to", m.Version, "request", c.Request)
		}
	}
	return true
}
