// Package link is the transport between the hub and a client: a WebSocket
// the client dials for the downlink (hub to client) and HTTP requests for
// the uplink (client to hub). Each side stores a message before it
// acknowledges it, and resends until the other side has acknowledged, so a
// message survives disconnects and restarts and the receiving inbox sees it
// at least once; the inbox's msg_id dedup turns that into exactly once.
package link

import (
	"context"
	"encoding/base64"
	"errors"
	"math/rand/v2"
	"time"
)

// Paths on the hub's HTTP server.
const (
	// DownlinkPath is the WebSocket endpoint a client dials.
	DownlinkPath = "/link"
	// UplinkPath takes a POST with one uplink message.
	UplinkPath = "/inbox"
)

// ClientHeader carries the client's id on every request to the hub.
const ClientHeader = "Fednet-Client"

// MaxPayload is the largest payload either link carries. Send and Post
// refuse anything larger, so a queued message always fits the frame and
// request limits on the other side.
const MaxPayload = 256 << 10

// maxFrameBytes bounds a downlink frame and an uplink request body: the
// base64 of MaxPayload plus the other fields.
var maxFrameBytes = int64(base64.StdEncoding.EncodedLen(MaxPayload) + 256)

// ErrPayloadTooBig is returned by Send and Post for a payload over MaxPayload.
var ErrPayloadTooBig = errors.New("link: payload over MaxPayload")

// downlink is a frame the hub sends on the WebSocket: one queued message.
type downlink struct {
	Seq     int64  `json:"seq"`
	MsgID   string `json:"msg_id"`
	Payload []byte `json:"payload"`
}

// ack is a frame the client sends on the WebSocket: every message up to and
// including Seq is stored.
type ack struct {
	Seq int64 `json:"ack"`
}

// uplink is the body of an uplink POST.
type uplink struct {
	MsgID   string `json:"msg_id"`
	Payload []byte `json:"payload"`
}

// Backoff is an exponential backoff with full jitter: the wait before
// retry n is a random duration up to Min doubled n times, capped at Max.
type Backoff struct {
	Min, Max time.Duration
}

// wait returns how long to wait before retry n, n counting from 0.
func (b Backoff) wait(n int) time.Duration {
	d := b.Min << min(n, 30)
	if d > b.Max || d <= 0 {
		d = b.Max
	}
	return rand.N(d) + 1
}

// sleep waits for wait(n) or until ctx is done.
func (b Backoff) sleep(ctx context.Context, n int) {
	select {
	case <-time.After(b.wait(n)):
	case <-ctx.Done():
	}
}
