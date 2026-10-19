// Package payload is the format of the payload a message carries between
// the hub and a client: a JSON object whose "type" field says what the
// message is. The link moves payloads as opaque bytes; both ends read and
// write them with this package.
package payload

import (
	"encoding/json"

	"github.com/Luolc/fednet/internal/slack"
)

// Types of message.
const (
	// Post is a message an agent posts to a thread.
	Post = "post"
	// Progress sets or closes the progress card of a thread.
	Progress = "progress"
	// Inbound is a message a person posted in a Slack thread, sent down to
	// the thread's owner; or, of trigger Relay or Refused, what the hub
	// sends down about a post an agent asked it to deliver.
	Inbound = "message"
	// Alert is an alert a client raises, such as a dead letter, for the
	// hub to send to the alerts webhook, which only the hub knows.
	Alert = "alert"
	// Approval is the outcome of an approval an agent requested, sent down
	// to the client that requested it.
	Approval = "approval"
	// Answer is what a person did with a question card an agent asked,
	// or a snoozed card coming back, sent down to the client that asked.
	// It reaches the hook like any message.
	Answer = "answer"
	// Reaction is an emoji a person on the user list put on the first
	// message of a thread, sent down to the thread's owner. It reaches
	// the hook like any message.
	Reaction = "reaction"
	// Upgrade tells a client to upgrade itself to Version, a release. The
	// client acts on it itself; it never reaches the hook.
	Upgrade = "upgrade"
)

// Outcomes of an approval, for Message.Outcome.
const (
	Approved = "approved"
	Rejected = "rejected"
	Expired  = "expired"
	// Failed is a route request that creates a channel, approved, that
	// the hub could not carry out: Note says why.
	Failed = "failed"
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
	// Handoff is the first message of a thread another machine opened
	// for the client, the machine of the channel's route.
	Handoff = "handoff"
	// Relay is a post another machine made in a thread the client owns
	// and asked the hub to deliver to it.
	Relay = "relay"
	// Refused tells the client that a post it asked the hub to deliver
	// was not: Text says why, in plain words, and TS is the post's ts
	// in Thread. It is not a person's message.
	Refused = "refused"
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
	// an agent posted, "fednet".
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
	// message, or put a reaction on a thread.
	User string `json:"user,omitempty"`
	// Reaction is the name of the emoji, without colons, of a message of
	// type Reaction.
	Reaction string `json:"reaction,omitempty"`
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
	// Scope is the scope of the route that handed the thread over, as it
	// was when the thread got its owner: on every inbound message of the
	// thread. Empty when the route had none, which the receiver takes as
	// its default.
	Scope string `json:"scope,omitempty"`
	// Trigger says why an inbound message was sent: Mention, Reply, DM,
	// Handoff, Relay or Refused.
	Trigger string `json:"trigger,omitempty"`
	// FromClient and Hop are set on a Handoff or a Relay: the client that
	// sent it, as the hub authenticated it, and how many relays in a row
	// the thread has had since a person last posted in it, this one
	// included.
	FromClient string `json:"from_client,omitempty"`
	Hop        int    `json:"hop,omitempty"`
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
	// Channel and Note are set on the outcome of a route request that
	// creates a channel: the id of the channel created, empty when it
	// failed, and what came of the request.
	Channel string `json:"channel,omitempty"`
	Note    string `json:"note,omitempty"`
	// Approval is, on an approved outcome, the signed approval document
	// as `fednet approval verify` reads it, to be written to a file as is.
	Approval json.RawMessage `json:"approval,omitempty"`
	// Answer is, on an Answer, the question.Answer as JSON: what the
	// card shows the agent got.
	Answer json.RawMessage `json:"answer,omitempty"`
	// Version is, on an Upgrade, the release to upgrade to.
	Version string `json:"version,omitempty"`
	// Deliver is set on a post the hub also delivers to the thread's
	// owner, as an inbound message of trigger Relay, once it is in Slack.
	Deliver bool `json:"deliver,omitempty"`
	// Footer is set on a post whose Text goes out as one line of small
	// grey text.
	Footer bool `json:"footer,omitempty"`
	// Mentions are the Slack user ids a post mentions at its start, each
	// on the hub's user list; a footer has none.
	Mentions []string `json:"mentions,omitempty"`
	// Title and Items are, on a Progress, the whole card; Close is
	// slack.Done or slack.Failed on one that closes the card, when Title
	// and Items may be left out to keep them as they are.
	Title string               `json:"title,omitempty"`
	Items []slack.ProgressItem `json:"items,omitempty"`
	Close string               `json:"close,omitempty"`
}
