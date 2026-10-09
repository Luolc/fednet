// Package payload is the format of the payload a message carries between
// the hub and a client: a JSON object whose "type" field says what the
// message is. The link moves payloads as opaque bytes; both ends read and
// write them with this package.
package payload

// Types of message.
const (
	// Post is a message an agent posts to a thread.
	Post = "post"
)

// Message is a decoded payload. Type is always set; which other fields are
// set depends on it.
type Message struct {
	Type string `json:"type"`
	// Thread is the key of the thread a post goes to.
	Thread string `json:"thread,omitempty"`
	// Text is the body of a post.
	Text string `json:"text,omitempty"`
}
