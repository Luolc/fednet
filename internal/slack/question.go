package slack

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"

	slackgo "github.com/slack-go/slack"

	"github.com/Luolc/fednet/internal/question"
)

// QuestionCard is a question card in a thread: the question with its
// options, and, while it waits, a radio button for each option and one
// for "other", a note box, submit, the snooze buttons and close, each
// carrying ID. Once a person has acted on it, or the agent has taken it
// back, it shows how and has no buttons.
type QuestionCard struct {
	// ID is the card's id on the hub; a click reports it.
	ID       string
	Question question.Question
	// Mentions are the Slack user ids mentioned at the top of the card,
	// which notifies them.
	Mentions []string
	// State is empty while the card waits; question.Answered, Snoozed or
	// Closed once a person acted on it, with Answer what the agent got;
	// QuestionWithdrawn once the agent took it back, with Note its note;
	// or QuestionMoved, a snoozed card posted again lower in the thread.
	State  string
	Answer question.Answer
	Note   string
	// Picked, on a card that waits, is the value of the radio button to
	// show selected in place of the recommended option; Warning is a
	// line shown above submit.
	Picked  string
	Warning string
}

// States of a QuestionCard besides the outcomes of an answer.
const (
	QuestionWithdrawn = "withdrawn"
	QuestionMoved     = "moved"
)

// Action ids on a question card, and the callback id of the time picker
// that the pick-time button opens. Each snooze button has its own action
// id, in QuestionSnoozes.
const (
	QuestionPickAction     = "ask-pick"
	QuestionNoteAction     = "ask-note"
	QuestionSubmitAction   = "ask-submit"
	QuestionCloseAction    = "ask-close"
	QuestionPickTimeAction = "ask-snooze-pick"
	QuestionTimePicker     = "ask-snooze-at"
	questionAtAction       = "ask-at"
)

// QuestionOther is the value of the radio button "other".
const QuestionOther = "other"

// MaxQuestionNote is the most a note box takes, in characters, so that
// the answer the card shows with it fits in one block.
const MaxQuestionNote = 2000

// QuestionSnoozes are the snooze buttons, in their order on the card.
var QuestionSnoozes = []struct {
	Action, Label string
	After         time.Duration
}{
	{"ask-snooze-30m", "30 分钟后再问", 30 * time.Minute},
	{"ask-snooze-2h", "2 小时后再问", 2 * time.Hour},
	{"ask-snooze-24h", "明天再问", 24 * time.Hour},
}

// QuestionBlockID is the id of the block part of the card with id is in;
// a click reports it, so that a button from elsewhere carrying the same
// value is told apart.
func QuestionBlockID(id, part string) string { return "ask:" + id + ":" + part }

// Parts of a question card that hold its buttons and boxes.
const (
	QuestionPickPart   = "pick"
	QuestionNotePart   = "note"
	QuestionSubmitPart = "submit"
	QuestionLaterPart  = "later"
)

// QuestionClick is a press on a question card's button, or the time
// picked in the picker one of them opened, with what the card's radio
// buttons and note box held then.
type QuestionClick struct {
	// ID is the card's id; Action the button's action id, or
	// QuestionTimePicker for a time picked, which is Until.
	ID     string
	Action string
	Until  time.Time
	// Choice is the value of the radio button selected: an option's
	// index, QuestionOther, or empty. Note is what the note box held,
	// trimmed.
	Choice string
	Note   string
	// User is the Slack user id of who clicked; Bot is set when the user
	// is a bot.
	User string
	Bot  bool
	// Channel and TS locate the card.
	Channel string
	TS      string
	// TriggerID lets the hub open the time picker, for a few seconds.
	TriggerID string
}

// PickerState is what the time picker carries from the click that opened
// it to the time picked: the card, and the note the box held.
type PickerState struct {
	ID      string `json:"id"`
	Channel string `json:"channel"`
	TS      string `json:"ts"`
	Note    string `json:"note,omitempty"`
}

// questionText is the plain text of a card, which notifications show.
func questionText(c QuestionCard) string {
	s := ""
	for _, u := range c.Mentions {
		s += "<@" + u + "> "
	}
	return s + "❓ " + c.Question.Question
}

// questionBlocks lays out c: the mentions, the question as the header,
// the context and each option written out in markdown blocks, then the
// buttons while it waits, or how it ended. What the agent wrote goes in
// plain text and markdown blocks, never in mrkdwn; the note a person
// wrote goes in plain text, and the answer the agent got in a
// preformatted block, both shown as they are.
func questionBlocks(c QuestionCard) []slackgo.Block {
	plain := func(s string) *slackgo.TextBlockObject {
		return slackgo.NewTextBlockObject(slackgo.PlainTextType, s, false, false)
	}
	mrkdwn := func(s string) *slackgo.TextBlockObject {
		return slackgo.NewTextBlockObject(slackgo.MarkdownType, s, false, false)
	}
	q := c.Question
	var bs []slackgo.Block
	if len(c.Mentions) > 0 {
		var els []slackgo.RichTextSectionElement
		for i, u := range c.Mentions {
			if i > 0 {
				els = append(els, slackgo.NewRichTextSectionTextElement(" ", nil))
			}
			els = append(els, slackgo.NewRichTextSectionUserElement(u, nil))
		}
		bs = append(bs, slackgo.NewRichTextBlock("", slackgo.NewRichTextSection(els...)))
	}
	bs = append(bs, slackgo.NewHeaderBlock(plain("❓ "+q.Question)))
	idLine := slackgo.NewContextBlock("", mrkdwn("编号 "+c.ID))
	if c.State == QuestionMoved {
		return append(bs, slackgo.NewSectionBlock(mrkdwn("💤 已延后，卡片重发在下方"), nil, nil), idLine)
	}
	if q.Context != "" {
		bs = append(bs, slackgo.NewMarkdownBlock("", q.Context))
	}
	for i := range q.Options {
		bs = append(bs, slackgo.NewMarkdownBlock("", q.OptionMarkdown(i)))
	}
	bs = append(bs, slackgo.NewDividerBlock())
	switch c.State {
	case "":
		return append(bs, waitingBlocks(c, plain, mrkdwn)...)
	case QuestionWithdrawn:
		bs = append(bs, slackgo.NewSectionBlock(mrkdwn("🗂 提问的 agent 收回了这个问题"), nil, nil))
		if c.Note != "" {
			bs = append(bs, slackgo.NewSectionBlock(plain(c.Note), nil, nil))
		}
		return append(bs, idLine)
	}
	a := c.Answer
	var line, choice string
	switch c.State {
	case question.Answered:
		if a.Choice != nil && a.Choice.Index < len(q.Options) {
			line, choice = "✅ <@"+a.By+"> 选了", q.OptionLabel(a.Choice.Index)
		} else {
			line = "✍️ <@" + a.By + "> 选了「其他」，写了补充"
		}
	case question.Snoozed:
		line = "💤 <@" + a.By + "> 延后到 " + snoozeDate(a) + " 再问"
	case question.Closed:
		line = "🚫 <@" + a.By + "> 关闭了这个问题"
	}
	bs = append(bs, slackgo.NewSectionBlock(mrkdwn(line), nil, nil))
	if choice != "" {
		bs = append(bs, slackgo.NewSectionBlock(plain(choice), nil, nil))
	}
	if a.Note != "" {
		bs = append(bs, slackgo.NewSectionBlock(plain("补充："+a.Note), nil, nil))
	}
	j, _ := a.MarshalJSON()
	return append(bs,
		slackgo.NewContextBlock("", mrkdwn("agent 收到：")),
		slackgo.NewRichTextBlock("", &slackgo.RichTextPreformatted{Type: slackgo.RTEPreformatted, Elements: []slackgo.RichTextSectionElement{slackgo.NewRichTextSectionTextElement(string(j), nil)}}),
		idLine,
	)
}

// waitingBlocks are the buttons and the note box of a card that waits.
func waitingBlocks(c QuestionCard, plain, mrkdwn func(string) *slackgo.TextBlockObject) []slackgo.Block {
	q := c.Question
	var opts []*slackgo.OptionBlockObject
	for i := range q.Options {
		opts = append(opts, slackgo.NewOptionBlockObject(strconv.Itoa(i), plain(q.OptionLabel(i)), nil))
	}
	opts = append(opts, slackgo.NewOptionBlockObject(QuestionOther, plain("其他"), nil))
	radio := slackgo.NewRadioButtonsBlockElement(QuestionPickAction, opts...)
	if i := q.Recommended(); i >= 0 {
		radio.InitialOption = opts[i]
	}
	for _, o := range opts {
		if o.Value == c.Picked {
			radio.InitialOption = o
		}
	}
	note := slackgo.NewPlainTextInputBlockElement(plain("选不选都可以写。选「其他」的话，请把想法写在这里"), QuestionNoteAction)
	note.Multiline, note.MaxLength = true, MaxQuestionNote
	later := []slackgo.BlockElement{}
	for _, s := range QuestionSnoozes {
		later = append(later, slackgo.NewButtonBlockElement(s.Action, c.ID, plain(s.Label)))
	}
	later = append(later,
		slackgo.NewButtonBlockElement(QuestionPickTimeAction, c.ID, plain("自选时间…")),
		slackgo.NewButtonBlockElement(QuestionCloseAction, c.ID, plain("关闭问题")).WithStyle(slackgo.StyleDanger).WithConfirm(
			slackgo.NewConfirmationBlockObject(plain("关闭这个问题？"), plain("agent 会收到「关闭」，自己决定不再问，还是换个问法。补充框里写的会一起带上。"), plain("关闭"), plain("取消")).WithStyle(slackgo.StyleDanger)),
	)
	bs := []slackgo.Block{
		slackgo.NewActionBlock(QuestionBlockID(c.ID, QuestionPickPart), radio),
		slackgo.NewInputBlock(QuestionBlockID(c.ID, QuestionNotePart), plain("补充 (可选)"), nil, note).WithOptional(true),
	}
	if c.Warning != "" {
		bs = append(bs, slackgo.NewSectionBlock(mrkdwn("*"+c.Warning+"*"), nil, nil))
	}
	return append(bs,
		slackgo.NewActionBlock(QuestionBlockID(c.ID, QuestionSubmitPart),
			slackgo.NewButtonBlockElement(QuestionSubmitAction, c.ID, plain("提交")).WithStyle(slackgo.StylePrimary)),
		slackgo.NewContextBlock("", mrkdwn("稍后再问，或者关掉这个问题：")),
		slackgo.NewActionBlock(QuestionBlockID(c.ID, QuestionLaterPart), later...),
		slackgo.NewContextBlock("", mrkdwn("编号 "+c.ID)),
	)
}

// snoozeDate writes when a snooze ends as a Slack date token.
func snoozeDate(a question.Answer) string {
	t, err := time.Parse(time.RFC3339, a.SnoozeUntil)
	if err != nil {
		return a.SnoozeUntil
	}
	return date(t)
}

// pickerView is the time picker: one date and time, two hours from now
// to begin with, and state carried to the submission.
func pickerView(state PickerState, now time.Time) (slackgo.ModalViewRequest, error) {
	plain := func(s string) *slackgo.TextBlockObject {
		return slackgo.NewTextBlockObject(slackgo.PlainTextType, s, false, false)
	}
	md, err := json.Marshal(state)
	if err != nil {
		return slackgo.ModalViewRequest{}, err
	}
	at := slackgo.NewDateTimePickerBlockElement(questionAtAction)
	at.InitialDateTime = now.Add(2 * time.Hour).Unix()
	return slackgo.ModalViewRequest{
		Type: slackgo.VTModal, CallbackID: QuestionTimePicker, PrivateMetadata: string(md),
		Title: plain("稍后再问"), Submit: plain("确定"), Close: plain("取消"),
		Blocks: slackgo.Blocks{BlockSet: []slackgo.Block{slackgo.NewInputBlock(questionAtAction, plain("到什么时候再问"), nil, at)}},
	}, nil
}

// PickedTime returns the time picked in the time picker whose submitted
// state is values, and whether one was.
func PickedTime(values map[string]map[string]slackgo.BlockAction) (time.Time, bool) {
	a, ok := values[questionAtAction][questionAtAction]
	if !ok || a.SelectedDateTime == 0 {
		return time.Time{}, false
	}
	return time.Unix(a.SelectedDateTime, 0), true
}

// PostQuestion posts the card's blocks as a reply; the plain text, which
// notifications show, starts with the mentions.
func (w *Web) PostQuestion(ctx context.Context, channel, ts string, c QuestionCard) (string, error) {
	return w.post(ctx, channel, slackgo.MsgOptionTS(ts), slackgo.MsgOptionText(questionText(c), false), slackgo.MsgOptionBlocks(questionBlocks(c)...))
}

func (w *Web) UpdateQuestion(ctx context.Context, channel, ts string, c QuestionCard) error {
	return w.call(ctx, "chat.update", func() error {
		_, _, _, err := w.c.UpdateMessageContext(ctx, channel, ts, slackgo.MsgOptionText(questionText(c), false), slackgo.MsgOptionBlocks(questionBlocks(c)...))
		return err
	})
}

func (w *Web) Permalink(ctx context.Context, channel, ts string) (string, error) {
	var link string
	err := w.call(ctx, "chat.getPermalink", func() (err error) {
		link, err = w.c.GetPermalinkContext(ctx, &slackgo.PermalinkParameters{Channel: channel, Ts: ts})
		return err
	})
	return link, err
}

func (w *Web) PickTime(ctx context.Context, triggerID string, state PickerState) error {
	v, err := pickerView(state, time.Now())
	if err != nil {
		return err
	}
	return w.call(ctx, "views.open", func() error {
		_, err := w.c.OpenViewContext(ctx, triggerID, v)
		return err
	})
}

func (w *Web) WhisperInThread(ctx context.Context, channel, ts, user, text string) error {
	return w.call(ctx, "chat.postEphemeral", func() error {
		_, err := w.c.PostEphemeralContext(ctx, channel, user, slackgo.MsgOptionText(text, false), slackgo.MsgOptionTS(ts))
		return err
	})
}

// PostQuestion posts the card as a reply from the bot whose text is the
// card's plain text, and keeps the card for Question.
func (f *Fake) PostQuestion(ctx context.Context, channel, ts string, c QuestionCard) (string, error) {
	if n := len(questionBlocks(c)); n > maxBlocks {
		return "", fmt.Errorf("slack: the card has %d blocks, Slack takes at most %d", n, maxBlocks)
	}
	posted, err := f.PostReply(ctx, channel, ts, questionText(c))
	if err != nil {
		return "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.questions == nil {
		f.questions = make(map[string]QuestionCard)
	}
	f.questions[ThreadKey(channel, posted)] = c
	return posted, nil
}

func (f *Fake) UpdateQuestion(_ context.Context, channel, ts string, c QuestionCard) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.questions[ThreadKey(channel, ts)]; !ok {
		return ErrNotFound
	}
	f.questions[ThreadKey(channel, ts)] = c
	return nil
}

// Question returns the question card at ts in channel as it is now, and
// whether there is one.
func (f *Fake) Question(channel, ts string) (QuestionCard, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.questions[ThreadKey(channel, ts)]
	return c, ok
}

// Questions returns the question cards in channel, oldest first, each
// with its ts.
func (f *Fake) Questions(channel string) (ts []string, cards []QuestionCard) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, key := range slices.SortedFunc(maps.Keys(f.questions), func(a, b string) int {
		_, ats, _ := ParseThreadKey(a)
		_, bts, _ := ParseThreadKey(b)
		return CompareTS(ats, bts)
	}) {
		ch, t, _ := ParseThreadKey(key)
		if ch == channel {
			ts = append(ts, t)
			cards = append(cards, f.questions[key])
		}
	}
	return ts, cards
}

func (f *Fake) Permalink(_ context.Context, channel, ts string) (string, error) {
	return "https://slack.example.invalid/archives/" + channel + "/p" + ts, nil
}

// PickTime keeps state for Picked, under triggerID.
func (f *Fake) PickTime(_ context.Context, triggerID string, state PickerState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pickers == nil {
		f.pickers = make(map[string]PickerState)
	}
	f.pickers[triggerID] = state
	return nil
}

// Picked returns the state of the time picker opened with triggerID, and
// whether one was.
func (f *Fake) Picked(triggerID string) (PickerState, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.pickers[triggerID]
	return s, ok
}

// WhisperInThread keeps text with the other ephemeral texts shown to
// user, and ts with it for WhisperedIn.
func (f *Fake) WhisperInThread(_ context.Context, channel, ts, user, text string) error {
	return f.whisper(channel, ts, user, text)
}

// WhisperedIn returns, for each ephemeral text shown to user, oldest
// first, the thread it was shown in, or "" for the channel.
func (f *Fake) WhisperedIn(user string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var ts []string
	for _, w := range f.whispers[user] {
		ts = append(ts, w.ts)
	}
	return ts
}
