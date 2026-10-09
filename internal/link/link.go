// Package link is the transport between the hub and a client: a WebSocket
// the client dials for the downlink (hub to client), HTTP requests for the
// uplink (client to hub), and HTTP requests the hub answers at once. On the
// two links each side stores a message before it acknowledges it, and
// resends until the other side has acknowledged, so a message survives
// disconnects and restarts and the receiving inbox sees it at least once;
// the inbox's msg_id dedup turns that into exactly once. A request is not
// stored or resent: it is answered or fails.
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
	// RequestPath takes a POST with one request and replies with the
	// answer. Requests are not queued: they are answered or fail.
	RequestPath = "/request"
	// FilePath takes a GET with the Slack file id in the "id" query
	// parameter and replies with the file's content, streamed from Slack:
	// Content-Type and Content-Length are the file's, FileNameHeader its
	// name. A file the hub does not serve is refused like a request.
	FilePath = "/file"
	// UploadPath takes a POST whose body is an Upload as JSON followed by
	// the content of each of its files, in order, each Size bytes, and
	// replies 204 once the hub has posted them in the thread. Like a
	// request it is not queued: it is done or fails.
	UploadPath = "/upload"
)

// Upload is the header of an upload: the thread the files go to, a text
// to post with them, and the files, whose content follows the header.
type Upload struct {
	Thread string       `json:"thread"`
	Text   string       `json:"text,omitempty"`
	Files  []FileHeader `json:"files"`
}

// FileHeader is one file of an Upload: its name and how many bytes of
// content follow.
type FileHeader struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

// Total is how many bytes of content follow the header.
func (u Upload) Total() int64 {
	var n int64
	for _, f := range u.Files {
		n += f.Size
	}
	return n
}

// Headers on every request to the hub.
const (
	// ClientHeader carries the client's id.
	ClientHeader = "Fednet-Client"
	// VersionHeader carries the client's version.
	VersionHeader = "Fednet-Version"
	// UpgradeHeader, on the hub's response to a downlink handshake, names
	// the release the client should upgrade to.
	UpgradeHeader = "Fednet-Upgrade"
	// FileNameHeader, on the hub's response to a file GET, carries the
	// file's name, percent-encoded as a URL path segment is.
	FileNameHeader = "Fednet-File-Name"
)

// File is what the hub says of a file it serves, before the content.
type File struct {
	Name     string
	Mimetype string
	Size     int64
}

// MaxPayload is the largest payload either link carries. Send and Post
// refuse anything larger, so a queued message always fits the frame and
// request limits on the other side.
const MaxPayload = 256 << 10

// maxFrameBytes bounds a downlink frame and an uplink request body: the
// base64 of MaxPayload plus the other fields.
var maxFrameBytes = int64(base64.StdEncoding.EncodedLen(MaxPayload) + 256)

// ErrPayloadTooBig is returned by Send and Post for a payload over MaxPayload.
var ErrPayloadTooBig = errors.New("link: payload over MaxPayload")

// maxAnswerBytes bounds an answer to a request; a thread read back from
// Slack may be much larger than one message.
const maxAnswerBytes = 16 << 20

// The kinds of refusal, for Refuse. Client.Request returns an error that
// wraps the kind the hub refused with and reads as the hub's message.
var (
	ErrBadRequest = errors.New("link: bad request")
	ErrDenied     = errors.New("link: denied")
	ErrNotFound   = errors.New("link: not found")
)

// ErrUnreachable is wrapped by Client.Request when it got no answer from the
// hub: the hub could not be reached or did not answer in time.
var ErrUnreachable = errors.New("link: hub unreachable")

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
