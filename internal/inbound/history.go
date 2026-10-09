package inbound

import (
	"strings"
	"unicode/utf8"

	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/slack"
)

// Defaults of Limits.
const (
	DefaultMaxMessages     = 10
	DefaultMaxChars        = 4000
	DefaultMaxMessageChars = 2000
)

// TruncatedMark ends a text cut to the limit.
const TruncatedMark = "…[截断，全文见 read-thread]"

// Limits bounds the history a Mention carries, and the length of any
// inbound message's text. Zero means the default.
type Limits struct {
	// MaxMessages is how many messages a history holds at most.
	MaxMessages int
	// MaxChars is how many characters the texts of a history add up to
	// at most; the latest message is always included.
	MaxChars int
	// MaxMessageChars is how many characters one message's text may have
	// before it is cut, in the history and in the message sent down.
	MaxMessageChars int
}

func (l Limits) messages() int     { return or(l.MaxMessages, DefaultMaxMessages) }
func (l Limits) chars() int        { return or(l.MaxChars, DefaultMaxChars) }
func (l Limits) messageChars() int { return or(l.MaxMessageChars, DefaultMaxMessageChars) }

func or(v, def int) int {
	if v == 0 {
		return def
	}
	return v
}

// history returns what ev's thread, ms as Slack gives it, held before ev,
// within r.History: the latest messages, until there are MaxMessages of
// them or one more would take the texts over MaxChars, the latest one
// always; each text cut to MaxMessageChars. It returns nil when the
// thread held nothing before ev, and when ms is nil, the thread not having
// been read: the message goes without, and the agent can read the thread
// itself.
func (r *Receiver) history(ev Event, thread string, ms []slack.Message) *payload.History {
	var before []payload.HistoryMessage
	for _, m := range ms {
		if slack.CompareTS(m.TS, ev.TS) < 0 {
			before = append(before, r.historyMessage(m))
		}
	}
	if len(before) == 0 {
		return nil
	}
	chars, n := 0, 0
	for n < len(before) && n < r.History.messages() {
		c := utf8.RuneCountInString(before[len(before)-1-n].Text)
		if n > 0 && chars+c > r.History.chars() {
			break
		}
		chars += c
		n++
	}
	return &payload.History{
		Total:    len(before),
		Included: n,
		Omitted:  len(before) - n,
		Messages: before[len(before)-n:],
		ReadMore: "fednet client read-thread -socket <socket> " + thread,
	}
}

// historyMessage converts m for a history. A message the bot posted is
// an agent's: it has no user, and is named by its machine.
func (r *Receiver) historyMessage(m slack.Message) payload.HistoryMessage {
	h := payload.HistoryMessage{TS: m.TS, User: m.User, Name: r.Users[m.User], Files: files(m.Files)}
	if h.Files == nil {
		h.Files = []payload.File{}
	}
	if m.User == r.Bot {
		h.User, h.Name = "", "fednet"
		if m.Machine != "" {
			h.Name = "fednet (" + m.Machine + ")"
		}
	}
	h.Text, h.Truncated = clip(m.Text, r.History.messageChars())
	return h
}

// clip cuts s to its first max characters, TruncatedMark appended, when
// it has more than max; cut says whether it did.
func clip(s string, max int) (out string, cut bool) {
	if utf8.RuneCountInString(s) <= max {
		return s, false
	}
	var b strings.Builder
	n := 0
	for _, r := range s {
		if n == max {
			break
		}
		b.WriteRune(r)
		n++
	}
	b.WriteString(TruncatedMark)
	return b.String(), true
}
