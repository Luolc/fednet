// Package payload is the format of the payload a message carries between
// the hub and a client: a JSON object whose "type" field says what the
// message is. The link moves payloads as opaque bytes; both ends read and
// write them with this package.
package payload

import "encoding/json"

// Types of message.
const (
	// Post is a message an agent posts to a thread.
	Post = "post"
	// Inbound is a message a person posted in a Slack thread, sent down to
	// the thread's owner.
	Inbound = "message"
	// Alert is an alert a client raises, such as a dead letter, for the
	// hub to send to the alerts webhook, which only the hub knows.
	Alert = "alert"
	// Approval is the outcome of an approval an agent requested, sent down
	// to the client that requested it.
	Approval = "approval"
	// Upgrade tells a client to upgrade itself to Version, a release. The
	// client acts on it itself; it never reaches the hook.
	Upgrade = "upgrade"
)

// Outcomes of an approval, for Message.Outcome.
const (
	Approved = "approved"
	Rejected = "rejected"
	Expired  = "expired"
)

// Message is a decoded payload. Type is always set; which other fields are
// set depends on it.
type Message struct {
	Type string `json:"type"`
	// Thread is the key of the thread a post goes to, or the inbound
	// message is in.
	Thread string `json:"thread,omitempty"`
	// Text is the body of a post, of an inbound message or of an alert; an inbound
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
	// ApprovalID, Agent and Outcome are set on an approval outcome: which
	// approval, the agent that requested it, and Approved, Rejected or
	// Expired. Text is then the summary the request gave.
	ApprovalID string `json:"approval_id,omitempty"`
	Agent      string `json:"agent,omitempty"`
	Outcome    string `json:"outcome,omitempty"`
	// Approver is the Slack user id of the person who approved or
	// rejected; empty when the approval expired.
	Approver string `json:"approver,omitempty"`
	// Approval is, on an approved outcome, the signed approval document
	// as `fednet approval verify` reads it, to be written to a file as is.
	Approval json.RawMessage `json:"approval,omitempty"`
	// Version is, on an Upgrade, the release to upgrade to.
	Version string `json:"version,omitempty"`
}
