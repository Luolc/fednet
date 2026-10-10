package slack

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	slackgo "github.com/slack-go/slack"
)

// Progress is a thread's progress card: a short title, which says what
// is going on now, and the items under it, which Slack shows when the
// card is opened.
type Progress struct {
	Title string         `json:"title"`
	Items []ProgressItem `json:"items"`
}

// ProgressItem is one item of a progress card, in State Doing, Done or
// Failed.
type ProgressItem struct {
	Text  string `json:"text"`
	State string `json:"state"`
}

// States of a ProgressItem, and the Slack task statuses they show as: a
// spinner, a tick, a red mark.
const (
	Doing  = "doing"
	Done   = "done"
	Failed = "error"
)

var taskStatus = map[string]slackgo.TaskCardStatus{
	Doing:  slackgo.TaskCardStatusInProgress,
	Done:   slackgo.TaskCardStatusComplete,
	Failed: slackgo.TaskCardStatusError,
}

// MaxProgressItems is the most items Slack takes on one card; one more
// and it refuses the whole message.
const MaxProgressItems = 50

// MaxFooterChars is the longest footer, in characters of mrkdwn, links
// written out: the limit of a text object in a context block.
const MaxFooterChars = 3000

// planBlock is Slack's plan block. slackgo's PlanBlock leaves out an
// empty task list, which the block requires.
type planBlock struct {
	Type    slackgo.MessageBlockType `json:"type"`
	BlockID string                   `json:"block_id"`
	Title   string                   `json:"title"`
	Tasks   []slackgo.TaskCardBlock  `json:"tasks"`
}

func (b planBlock) BlockType() slackgo.MessageBlockType { return b.Type }
func (b planBlock) ID() string                          { return b.BlockID }

// progressBlock lays p out as a plan block. Each layout gets a new block
// id, as Slack asks of a message that is updated.
func progressBlock(p Progress) planBlock {
	b := planBlock{Type: slackgo.MBTPlan, BlockID: "progress:" + rand.Text(), Title: p.Title, Tasks: []slackgo.TaskCardBlock{}}
	for i, it := range p.Items {
		t := slackgo.NewTaskCardBlock("t"+strconv.Itoa(i+1), it.Text)
		t.Status = taskStatus[it.State]
		b.Tasks = append(b.Tasks, *t)
	}
	return b
}

// progressText is a card's plain text, for notifications.
func progressText(p Progress) string { return "进度：" + p.Title }

func (w *Web) PostProgress(ctx context.Context, channel, ts string, p Progress) (string, error) {
	var posted string
	err := w.call(ctx, "chat.postMessage", func() (err error) {
		_, posted, err = w.c.PostMessageContext(ctx, channel, slackgo.MsgOptionTS(ts),
			slackgo.MsgOptionText(progressText(p), true), slackgo.MsgOptionBlocks(progressBlock(p)))
		return err
	})
	return posted, err
}

func (w *Web) UpdateProgress(ctx context.Context, channel, ts string, p Progress) error {
	return w.call(ctx, "chat.update", func() error {
		_, _, _, err := w.c.UpdateMessageContext(ctx, channel, ts,
			slackgo.MsgOptionText(progressText(p), true), slackgo.MsgOptionBlocks(progressBlock(p)))
		return err
	})
}

// PostFooter posts a message of a single context block, whose text is
// Footer's mrkdwn; the plain text is Footer's plain version.
func (w *Web) PostFooter(ctx context.Context, channel, ts, text string) (string, error) {
	mrkdwn, plain := Footer(text)
	block := slackgo.NewContextBlock("", slackgo.NewTextBlockObject(slackgo.MarkdownType, mrkdwn, false, false))
	var posted string
	err := w.call(ctx, "chat.postMessage", func() (err error) {
		_, posted, err = w.c.PostMessageContext(ctx, channel, slackgo.MsgOptionTS(ts),
			slackgo.MsgOptionText(plain, true), slackgo.MsgOptionBlocks(block))
		return err
	})
	return posted, err
}

// markdownLink matches a standard Markdown link, [text](url).
var markdownLink = regexp.MustCompile(`\[([^\[\]]+)\]\(([^()\s]+)\)`)

var escapeMrkdwn = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")

// Footer returns text, standard Markdown, as Slack's mrkdwn, its links
// written as <url|text> and the rest escaped, and as plain text, each
// link replaced by its text.
func Footer(text string) (mrkdwn, plain string) {
	var m, p strings.Builder
	last := 0
	for _, loc := range markdownLink.FindAllStringSubmatchIndex(text, -1) {
		m.WriteString(escapeMrkdwn.Replace(text[last:loc[0]]))
		p.WriteString(text[last:loc[0]])
		label, url := text[loc[2]:loc[3]], text[loc[4]:loc[5]]
		m.WriteString("<" + escapeMrkdwn.Replace(url) + "|" + escapeMrkdwn.Replace(label) + ">")
		p.WriteString(label)
		last = loc[1]
	}
	m.WriteString(escapeMrkdwn.Replace(text[last:]))
	p.WriteString(text[last:])
	return m.String(), p.String()
}

// MachineFooter is text as the hub posts it in a footer from machine,
// the machine's name added at the end.
func MachineFooter(text, machine string) string { return text + " · " + machine }

// FooterFits reports whether text, as Footer writes it in mrkdwn, is
// within MaxFooterChars.
func FooterFits(text string) bool {
	mrkdwn, _ := Footer(text)
	return utf8.RuneCountInString(mrkdwn) <= MaxFooterChars
}

// PostProgress posts p as a message from the bot whose text is the card's
// plain text, and keeps p for Progress.
func (f *Fake) PostProgress(_ context.Context, channel, ts string, p Progress) (string, error) {
	posted, err := f.Add(channel, Message{ThreadTS: ts, User: FakeBot, Text: progressText(p)})
	if err != nil {
		return "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.progress == nil {
		f.progress = make(map[string]Progress)
	}
	f.progress[ThreadKey(channel, posted)] = p
	return posted, nil
}

func (f *Fake) UpdateProgress(_ context.Context, channel, ts string, p Progress) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.progress[ThreadKey(channel, ts)]; !ok {
		return ErrNotFound
	}
	f.progress[ThreadKey(channel, ts)] = p
	return nil
}

// Progress returns the progress card at ts in channel as it is now, and
// whether there is one.
func (f *Fake) Progress(channel, ts string) (Progress, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.progress[ThreadKey(channel, ts)]
	return p, ok
}

// DeleteProgress deletes the progress card at ts in channel, as a person
// may in Slack.
func (f *Fake) DeleteProgress(channel, ts string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.progress, ThreadKey(channel, ts))
}

// PostFooter posts a message from the bot whose text is Footer's plain
// version, and keeps its mrkdwn for FooterText.
func (f *Fake) PostFooter(_ context.Context, channel, ts, text string) (string, error) {
	mrkdwn, plain := Footer(text)
	posted, err := f.Add(channel, Message{ThreadTS: ts, User: FakeBot, Text: plain})
	if err != nil {
		return "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.footers == nil {
		f.footers = make(map[string]string)
	}
	f.footers[posted] = mrkdwn
	return posted, nil
}

// FooterText returns the mrkdwn of the footer at ts, and whether there is
// one.
func (f *Fake) FooterText(ts string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, ok := f.footers[ts]
	return m, ok
}

// CheckProgress returns why a card of title and items cannot be shown,
// or nil. A card that closes the open one may leave the title out.
func CheckProgress(title string, items []ProgressItem, closing bool) error {
	if title == "" && !closing {
		return errors.New("a progress card needs a title")
	}
	if len(items) > MaxProgressItems {
		return fmt.Errorf("%d items, a progress card takes at most %d", len(items), MaxProgressItems)
	}
	for _, it := range items {
		if it.Text == "" {
			return errors.New("an item needs a text")
		}
		if _, ok := taskStatus[it.State]; !ok {
			return fmt.Errorf("item %q: state %q is not %s, %s or %s", it.Text, it.State, Doing, Done, Failed)
		}
	}
	return nil
}
