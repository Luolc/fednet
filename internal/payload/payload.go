// Package payload is the format of the payload a message carries between
// the hub and a client: a JSON object whose "type" field says what the
// message is. The link moves payloads as opaque bytes; both ends read and
// write them with this package.
package payload

// Types of message.
const (
	// Post is a message an agent posts to a thread.
	Post = "post"
	// Inbound is a message a person posted in a Slack thread, sent down to
	// the thread's owner.
	Inbound = "message"
)

// Message is a decoded payload. Type is always set; which other fields are
// set depends on it.
type Message struct {
	Type string `json:"type"`
	// Thread is the key of the thread a post goes to, or the inbound
	// message is in.
	Thread string `json:"thread,omitempty"`
	// Text is the body of a post or of an inbound message; an inbound
	// message's uploaded files are listed at the end of it, each as a line
	// with its name and link.
	Text string `json:"text,omitempty"`
	// User is the Slack user id of the person who posted an inbound
	// message.
	User string `json:"user,omitempty"`
	// TS is the Slack ts of an inbound message.
	TS string `json:"ts,omitempty"`
	// Context is the description of the channel, its Slack purpose, sent
	// with the inbound message that starts a thread in it; empty for a
	// reply, a direct message, or when the hub could not read it.
	Context string `json:"context,omitempty"`
}
