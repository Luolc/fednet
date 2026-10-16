// Package ask runs the question cards on the hub: an agent asks a person
// a question in a thread, the card waits for a press on its buttons, and
// what the person did goes down to the client that asked, as a message of
// type answer. A snooze is timed by the hub from its database, so it
// survives a restart and a handoff.
package ask

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/question"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
)

// DefaultInterval is how often Run looks for snoozes that have ended and
// for cards that do not show their state yet.
const DefaultInterval = time.Minute

// DefaultSlackTimeout bounds each Slack call a sweep makes.
const DefaultSlackTimeout = 30 * time.Second

// pickTimeout bounds opening the time picker: Slack takes a trigger id
// for three seconds after the click.
const pickTimeout = 2500 * time.Millisecond

// maxHints is how many hints wait for a sweep at most; past it a hint is
// dropped, with a log line.
const maxHints = 64

// Errors of Ask and Withdraw.
var (
	// ErrInvalid is a card that is not a question within the limits; the
	// error says which field and by how much.
	ErrInvalid = errors.New("ask: invalid card")
	// ErrNotListed is a mention of someone not on the user list.
	ErrNotListed = errors.New("ask: not on the user list")
	// ErrNotYours is a withdrawal of a card another client asked.
	ErrNotYours = errors.New("ask: the card is another client's")
	// ErrDone is a withdrawal of a card a person has answered or closed,
	// or that was withdrawn before.
	ErrDone = errors.New("ask: the card is done")
)

// Flow runs the question cards. Its exported fields are set before use
// and not changed after.
type Flow struct {
	Store *store.Hub
	Slack slack.API
	// Users is the user list: only the people on it may act on a card or
	// be mentioned.
	Users map[string]string
	// Interval is how often Run sweeps; zero means DefaultInterval.
	Interval time.Duration
	// SlackTimeout bounds each Slack call a sweep makes; zero means
	// DefaultSlackTimeout.
	SlackTimeout time.Duration
	// Now returns the current time; nil means time.Now.
	Now func() time.Time
	// Stored, if set, is called each time an answer has been queued for a
	// client. It must not block.
	Stored func()

	nudge     chan struct{}
	nudgeOnce sync.Once
	// mu guards hints, the hints clicks left for Pass to deliver.
	mu    sync.Mutex
	hints []hint
}

// hint is a text to show user alone, in the thread that starts at ts in
// channel, or in channel when ts is empty.
type hint struct{ channel, ts, user, text string }

// Posted is where a card is: its id, its message, and the link to it,
// empty when Slack did not give one.
type Posted struct {
	ID        string `json:"id"`
	Channel   string `json:"channel"`
	TS        string `json:"ts"`
	Permalink string `json:"permalink,omitempty"`
}

// Ask posts card, a question as JSON, in thread, mentioning mentions, for
// client, which calls the question ref, and records it. A ref client has
// asked with before posts nothing: Ask returns that card.
func (f *Flow) Ask(ctx context.Context, client, thread, ref string, card []byte, mentions []string) (Posted, error) {
	q, err := question.Parse(card)
	if err != nil {
		return Posted{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	for _, u := range mentions {
		if _, ok := f.Users[u]; !ok {
			return Posted{}, fmt.Errorf("%w: %s", ErrNotListed, u)
		}
	}
	channel, root, _ := slack.ParseThreadKey(thread)
	if old, err := f.Store.QuestionByRef(ctx, client, ref); err == nil {
		return posted(old), nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return Posted{}, err
	}
	id := rand.Text()
	ts, err := f.Slack.PostQuestion(ctx, channel, root, slack.QuestionCard{ID: id, Question: q, Mentions: mentions})
	if err != nil {
		return Posted{}, err
	}
	link, err := f.Slack.Permalink(ctx, channel, ts)
	if err != nil {
		slog.Warn("ask: no link to the card", "card", id, "err", err)
	}
	rec := store.Question{ID: id, Client: client, Ref: ref, Thread: thread, Card: card, Mentions: mentions, AskedAt: f.now(), TS: ts, Permalink: link}
	err = f.Store.PutQuestion(ctx, rec)
	if err == nil {
		return posted(rec), nil
	}
	// The card is up without a record behind it: take it down, even if
	// the caller has given up. A ref asked with twice at once is
	// recorded once; the other card goes.
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), f.slackTimeout())
	derr := f.Slack.Delete(dctx, channel, ts)
	cancel()
	if errors.Is(err, store.ErrExists) && derr == nil {
		if old, err := f.Store.QuestionByRef(ctx, client, ref); err == nil {
			return posted(old), nil
		}
	}
	if derr != nil {
		return Posted{}, fmt.Errorf("ask: recording card %s failed: %w; deleting it failed too, so a card nobody can answer stays in Slack: %v", id, err, derr)
	}
	return Posted{}, fmt.Errorf("ask: recording card %s failed, so it was deleted: %w", id, err)
}

func posted(q store.Question) Posted {
	channel, _, _ := slack.ParseThreadKey(q.Thread)
	return Posted{ID: q.ID, Channel: channel, TS: q.TS, Permalink: q.Permalink}
}

// Withdraw takes back the card with id, which client asked, with note
// for the card: it stops waiting, and the card says so by the next Pass.
// Nothing goes down: the client knows.
func (f *Flow) Withdraw(ctx context.Context, client, id, note string) error {
	_, err := f.Store.ChangeQuestion(ctx, id, func(_ *store.Hub, q *store.Question) (bool, error) {
		switch {
		case q.Client != client:
			return false, ErrNotYours
		case q.Status != store.QuestionOpen && q.Status != question.Snoozed:
			return false, fmt.Errorf("%w: %s", ErrDone, q.Status)
		}
		q.Status, q.Note, q.SnoozeUntil, q.CardFinal = store.QuestionWithdrawn, note, time.Time{}, false
		return true, nil
	})
	if err == nil {
		f.Nudge()
	}
	return err
}

// Click applies c. A click counts only if the clicker is on the user list
// and not a bot, c names the card where it is now, and the card waits,
// all judged again under the database's write lock; a submit of "other"
// needs a note. A click that does not count changes nothing, and the
// clicker is told why in a hint the next Pass delivers. A click that
// counts records the answer and queues it for the client in the same
// transaction; the card is brought up to date by the next Pass. Click
// calls Slack itself only to open the time picker, which must be within
// seconds of the click. An error means nothing was recorded and the click
// should not be acked.
func (f *Flow) Click(ctx context.Context, c slack.QuestionClick) error {
	h, err := f.click(ctx, c)
	if err != nil {
		return err
	}
	if h.text != "" {
		h.user = c.User
		f.mu.Lock()
		if len(f.hints) < maxHints {
			f.hints = append(f.hints, h)
		} else {
			slog.Warn("ask: hints are piling up, dropping one", "card", c.ID, "user", c.User, "hint", h.text)
		}
		f.mu.Unlock()
	}
	f.Nudge()
	return nil
}

// click returns the hint for a click that did not count, one with no text
// for one that did.
func (f *Flow) click(ctx context.Context, c slack.QuestionClick) (hint, error) {
	h := hint{channel: c.Channel}
	switch {
	case c.Bot:
		h.text = "bot 不能回答问题"
		return h, nil
	case !f.listed(c.User):
		h.text = "你不在用户名单上，不能回答这个问题"
		return h, nil
	}
	q, err := f.Store.Question(ctx, c.ID)
	if errors.Is(err, store.ErrNotFound) {
		h.text = "没有这张问题卡 (编号 " + c.ID + ")"
		return h, nil
	}
	if err != nil {
		return h, err
	}
	channel, root, _ := slack.ParseThreadKey(q.Thread)
	h.ts = root
	if c.Channel != channel || c.TS != q.TS {
		h.text = "这个按钮不在编号 " + c.ID + " 的问题卡上"
		return h, nil
	}
	if q.Status != store.QuestionOpen {
		h.text = "这个问题已经处理过了"
		return h, nil
	}
	now := f.now()
	a := question.Answer{QuestionID: q.Ref, CardID: q.ID, Note: c.Note, By: c.User, At: now.Format(time.RFC3339)}
	switch c.Action {
	case slack.QuestionSubmitAction:
		a.Outcome = question.Answered
		var card question.Question
		if err := json.Unmarshal(q.Card, &card); err != nil {
			return h, err
		}
		if i, err := strconv.Atoi(c.Choice); err == nil && i >= 0 && i < len(card.Options) {
			a.Choice = &question.Choice{Index: i, Short: card.Options[i].Short}
		} else if c.Note == "" {
			h.text = "选了「其他」的话，请先在补充框里写几句，再点「提交」。"
			return h, nil
		}
	case slack.QuestionCloseAction:
		a.Outcome = question.Closed
	case slack.QuestionPickTimeAction:
		pctx, cancel := context.WithTimeout(ctx, pickTimeout)
		err := f.Slack.PickTime(pctx, c.TriggerID, slack.PickerState{ID: c.ID, Channel: c.Channel, TS: c.TS, Note: c.Note})
		cancel()
		if err != nil {
			slog.Warn("ask: opening the time picker", "card", c.ID, "err", err)
			h.text = "没能打开选时间的窗口，请再点一次「自选时间…」"
		}
		return h, nil
	case slack.QuestionTimePicker:
		if !c.Until.After(now) {
			h.text = "选的时间已经过了，问题没有延后"
			return h, nil
		}
		a.Outcome, a.SnoozeUntil = question.Snoozed, c.Until.In(now.Location()).Format(time.RFC3339)
	default:
		for _, s := range slack.QuestionSnoozes {
			if c.Action == s.Action {
				a.Outcome, a.SnoozeUntil = question.Snoozed, now.Add(s.After).Truncate(time.Minute).Format(time.RFC3339)
			}
		}
		if a.Outcome == "" {
			return hint{}, nil
		}
	}
	queued, err := f.Store.ChangeQuestion(ctx, q.ID, func(tx *store.Hub, cur *store.Question) (bool, error) {
		if cur.Status != store.QuestionOpen || cur.TS != q.TS {
			return false, nil
		}
		b, err := f.queue(ctx, tx, cur.Client, a)
		if err != nil {
			return false, err
		}
		cur.Status, cur.Answer, cur.CardFinal = a.Outcome, b, false
		if a.Outcome == question.Snoozed {
			cur.SnoozeUntil, _ = time.Parse(time.RFC3339, a.SnoozeUntil)
		}
		return true, nil
	})
	if err != nil {
		return h, err
	}
	if !queued {
		h.text = "这个问题已经处理过了"
		return h, nil
	}
	f.stored()
	return hint{}, nil
}

// queue queues a for client in tx and returns it as JSON.
func (f *Flow) queue(ctx context.Context, tx *store.Hub, client string, a question.Answer) ([]byte, error) {
	b, err := a.MarshalJSON()
	if err != nil {
		return nil, err
	}
	m, err := json.Marshal(payload.Message{Type: payload.Answer, Answer: b})
	if err != nil {
		return nil, err
	}
	_, err = tx.Outbox.Enqueue(ctx, client, m)
	return b, err
}

func (f *Flow) listed(user string) bool {
	_, ok := f.Users[user]
	return ok
}

// Nudge tells Run that there is a card to bring up to date or a hint to
// deliver. It never blocks.
func (f *Flow) Nudge() {
	select {
	case f.nudgeCh() <- struct{}{}:
	default:
	}
}

func (f *Flow) nudgeCh() chan struct{} {
	f.nudgeOnce.Do(func() { f.nudge = make(chan struct{}, 1) })
	return f.nudge
}

// Run sweeps every Interval, and when nudged, until ctx is done. It must
// be called once, by one process at a time.
func (f *Flow) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if err := f.Pass(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("ask: sweep", "err", err)
		}
		t := time.NewTimer(f.interval())
		select {
		case <-f.nudgeCh():
		case <-t.C:
		case <-ctx.Done():
		}
		t.Stop()
	}
}

// Pass posts again, at the bottom of its thread, each snoozed card whose
// snooze has ended, mentioning who snoozed it, and sends resurfaced down;
// brings each card up to date with its state, and each card a snooze
// moved down to say where it went; and delivers the hints clicks left.
// Each Slack call gets SlackTimeout; what Slack does not take now is left
// for the next Pass, and a hint it does not take is logged and dropped.
func (f *Flow) Pass(ctx context.Context) error {
	due, err := f.Store.DueQuestions(ctx, f.now())
	if err != nil {
		return err
	}
	for _, q := range due {
		if err := f.resurface(ctx, q); err != nil {
			return err
		}
	}
	unfinished, err := f.Store.UnfinishedQuestions(ctx)
	if err != nil {
		return err
	}
	for _, q := range unfinished {
		if err := f.finish(ctx, q); err != nil {
			return err
		}
	}
	f.mu.Lock()
	hints := f.hints
	f.hints = nil
	f.mu.Unlock()
	for _, h := range hints {
		sctx, cancel := context.WithTimeout(ctx, f.slackTimeout())
		var err error
		if h.ts == "" {
			err = f.Slack.Whisper(sctx, h.channel, h.user, h.text)
		} else {
			err = f.Slack.WhisperInThread(sctx, h.channel, h.ts, h.user, h.text)
		}
		cancel()
		if err != nil {
			slog.Warn("ask: telling the clicker why the click did not count", "user", h.user, "err", err)
		}
	}
	return nil
}

// resurface posts q, snoozed, again at the bottom of its thread, then
// records it there, open, and queues resurfaced for the client, in one
// transaction. A card withdrawn in between is taken down again. A hub
// that dies between the post and the record posts the card once more on
// its next Pass; the copy before, clicked, only gets a hint.
func (f *Flow) resurface(ctx context.Context, q store.Question) error {
	var card question.Question
	if err := json.Unmarshal(q.Card, &card); err != nil {
		return err
	}
	var snooze question.Answer
	if err := json.Unmarshal(q.Answer, &snooze); err != nil {
		return err
	}
	channel, root, _ := slack.ParseThreadKey(q.Thread)
	var ts, link string
	err := f.call(ctx, func(ctx context.Context) (err error) {
		ts, err = f.Slack.PostQuestion(ctx, channel, root, slack.QuestionCard{ID: q.ID, Question: card, Mentions: []string{snooze.By}})
		return err
	})
	if err != nil {
		slog.Warn("ask: posting a snoozed card again, will retry", "card", q.ID, "err", err)
		return nil
	}
	if err := f.call(ctx, func(ctx context.Context) (err error) {
		link, err = f.Slack.Permalink(ctx, channel, ts)
		return err
	}); err != nil {
		slog.Warn("ask: no link to the card posted again", "card", q.ID, "err", err)
	}
	a := question.Answer{QuestionID: q.Ref, CardID: q.ID, Outcome: question.Resurfaced, At: f.now().Format(time.RFC3339)}
	moved, err := f.Store.ChangeQuestion(ctx, q.ID, func(tx *store.Hub, cur *store.Question) (bool, error) {
		if cur.Status != question.Snoozed || cur.TS != q.TS {
			return false, nil
		}
		if _, err := f.queue(ctx, tx, cur.Client, a); err != nil {
			return false, err
		}
		cur.MovedTS, cur.TS, cur.Permalink = cur.TS, ts, link
		cur.Status, cur.SnoozeUntil, cur.CardFinal = store.QuestionOpen, time.Time{}, true
		return true, nil
	})
	if err != nil || !moved {
		if derr := f.call(ctx, func(ctx context.Context) error { return f.Slack.Delete(ctx, channel, ts) }); derr != nil {
			slog.Warn("ask: deleting a card posted again in vain", "card", q.ID, "err", derr)
		}
		return err
	}
	f.stored()
	return nil
}

// finish brings q's card up to date with its state, and the card it was
// before a snooze to say where it went, and records each once done. A
// card that is gone from Slack has nothing to show.
func (f *Flow) finish(ctx context.Context, q store.Question) error {
	channel, _, _ := slack.ParseThreadKey(q.Thread)
	update := func(ts string, c slack.QuestionCard) bool {
		err := f.call(ctx, func(ctx context.Context) error { return f.Slack.UpdateQuestion(ctx, channel, ts, c) })
		if errors.Is(err, slack.ErrNotFound) {
			slog.Warn("ask: the card is gone from Slack", "card", q.ID, "ts", ts)
		} else if err != nil {
			slog.Warn("ask: updating the card, will retry", "card", q.ID, "ts", ts, "err", err)
			return false
		}
		return true
	}
	var card question.Question
	if err := json.Unmarshal(q.Card, &card); err != nil {
		return err
	}
	if q.MovedTS != "" && update(q.MovedTS, slack.QuestionCard{ID: q.ID, Question: card, State: slack.QuestionMoved}) {
		if _, err := f.Store.ChangeQuestion(ctx, q.ID, func(_ *store.Hub, cur *store.Question) (bool, error) {
			if cur.MovedTS != q.MovedTS {
				return false, nil
			}
			cur.MovedTS = ""
			return true, nil
		}); err != nil {
			return err
		}
	}
	if q.CardFinal {
		return nil
	}
	c := slack.QuestionCard{ID: q.ID, Question: card, State: q.Status, Note: q.Note}
	if q.Status == store.QuestionWithdrawn {
		c.State = slack.QuestionWithdrawn
	} else if err := json.Unmarshal(q.Answer, &c.Answer); err != nil {
		return err
	}
	if !update(q.TS, c) {
		return nil
	}
	_, err := f.Store.ChangeQuestion(ctx, q.ID, func(_ *store.Hub, cur *store.Question) (bool, error) {
		if cur.TS != q.TS || cur.Status != q.Status {
			return false, nil
		}
		cur.CardFinal = true
		return true, nil
	})
	return err
}

// call runs op with SlackTimeout.
func (f *Flow) call(ctx context.Context, op func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, f.slackTimeout())
	defer cancel()
	return op(ctx)
}

func (f *Flow) stored() {
	if f.Stored != nil {
		f.Stored()
	}
}

func (f *Flow) now() time.Time {
	if f.Now != nil {
		return f.Now()
	}
	return time.Now()
}

func (f *Flow) slackTimeout() time.Duration {
	if f.SlackTimeout != 0 {
		return f.SlackTimeout
	}
	return DefaultSlackTimeout
}

func (f *Flow) interval() time.Duration {
	if f.Interval != 0 {
		return f.Interval
	}
	return DefaultInterval
}
