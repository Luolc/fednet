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

// Triggers of an inbound message, for Message.Trigger: why the hub sent
// it down.
const (
	// Mention is a message that mentions the bot in a thread that had no
	// owner: the thread is handed to the client with this message.
	Mention = "mention"
	// Reply is a reply in a thread the client owns.
	Reply = "reply"
	// DM is a message in a direct message conversation.
	DM = "dm"
)

// File is a file uploaded with a message. ID is Slack's id of the file,
// which `fednet client fetch-file` takes; URL is the file's permalink,
// which needs a Slack login. The hub sets Fetch on the files the client
// fetches before it runs the hook, by the hub's limits on type and size;
// the client then sets Path, where the content is on the agent's
// machine, or Error, why it could not be fetched. Neither is set on a
// file of a History, which the agent fetches itself if it wants it.
type File struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Mimetype string `json:"mimetype"`
	Size     int    `json:"size"`
	URL      string `json:"url"`
	Fetch    bool   `json:"fetch,omitempty"`
	Path     string `json:"path,omitempty"`
	Error    string `json:"error,omitempty"`
}

// History is what a thread held before the message that handed it to the
// client: the latest few messages, within the hub's limits, and how many
// there are in all.
type History struct {
	// Total is how many messages the thread had before the trigger;
	// Included how many Messages holds, the latest ones; Omitted the
	// rest, all earlier than those.
	Total    int `json:"total"`
	Included int `json:"included"`
	Omitted  int `json:"omitted"`
	// Messages are oldest first.
	Messages []HistoryMessage `json:"messages"`
	// ReadMore is the command that reads the whole thread.
	ReadMore string `json:"read_more"`
}

// HistoryMessage is one message of a History.
type HistoryMessage struct {
	TS string `json:"ts"`
	// User is the Slack user id of the person who posted it; empty for a
	// message an agent posted.
	User string `json:"user"`
	// Name is the person's name on the hub's user list, or, for a message
	// an agent posted, "fednet (<machine>)".
	Name string `json:"name"`
	Text string `json:"text"`
	// Truncated is set when Text was cut to the hub's limit.
	Truncated bool   `json:"truncated"`
	Files     []File `json:"files"`
}

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
	// with the inbound message that hands a thread in it to the client;
	// empty for a reply, a direct message, or when the hub could not
	// read it.
	Context string `json:"context,omitempty"`
	// ChannelName is the name of the channel, without the #, as it was
	// when the thread got its owner: on the inbound message that hands
	// the thread over and on every reply after it. Empty for a direct
	// message, and when the hub could not read the name or owned the
	// thread from before it recorded names.
	ChannelName string `json:"channel_name,omitempty"`
	// Trigger says why an inbound message was sent: Mention, Reply or DM.
	Trigger string `json:"trigger,omitempty"`
	// UserName is User's name on the hub's user list.
	UserName string `json:"user_name,omitempty"`
	// Files are the files uploaded with an inbound message, which Text
	// also lists.
	Files []File `json:"files,omitempty"`
	// History is, on a Mention in a thread that had messages before it,
	// those messages.
	History *History `json:"history,omitempty"`
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
