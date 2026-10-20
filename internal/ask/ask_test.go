package ask

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/question"
	"github.com/Luolc/fednet/internal/route"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
)

const card = `{"question": "要不要放宽第 5 条？", "options": [{"short": "照原样"}, {"short": "放宽", "recommended": true}]}`

// testFlow is a Flow on a fresh store and a Fake Slack with a thread in
// C1, U1 and U2 on the user list, and a clock the test moves.
type testFlow struct {
	*Flow
	f      *slack.Fake
	path   string
	thread string
	now    time.Time
	seen   int64
}

func newFlow(t *testing.T) *testFlow {
	t.Helper()
	path := filepath.Join(t.TempDir(), "hub.db")
	f := &slack.Fake{}
	f.AddChannel("C1", "")
	root, err := f.Start("C1", "U1", "帮我看看第 5 条")
	if err != nil {
		t.Fatal(err)
	}
	tf := &testFlow{f: f, path: path, thread: slack.ThreadKey("C1", root), now: time.Date(2026, 10, 16, 9, 0, 20, 0, time.UTC)}
	tf.open(t)
	return tf
}

// open gives tf a new Flow on its database, as a hub that restarted.
func (tf *testFlow) open(t *testing.T) {
	t.Helper()
	st, err := store.OpenHub(t.Context(), tf.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	tf.Flow = &Flow{Store: st, Slack: tf.f, Users: map[string]string{"U1": "maintainer", "U2": ""}, Now: func() time.Time { return tf.now }}
}

// ask asks card for workstation with ref and returns where it is.
func (tf *testFlow) ask(t *testing.T, ref string, mentions ...string) Posted {
	t.Helper()
	p, err := tf.Ask(t.Context(), "workstation", tf.thread, ref, []byte(card), mentions)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// answers returns the answers queued for workstation since the last call.
func (tf *testFlow) answers(t *testing.T) []question.Answer {
	t.Helper()
	ds, err := tf.Store.Outbox.After(t.Context(), "workstation", tf.seen)
	if err != nil {
		t.Fatal(err)
	}
	var as []question.Answer
	for _, d := range ds {
		tf.seen = d.Seq
		var m payload.Message
		if err := json.Unmarshal(d.Payload, &m); err != nil || m.Type != payload.Answer {
			t.Fatalf("queued %s, %v; want an answer", d.Payload, err)
		}
		var a question.Answer
		if err := json.Unmarshal(m.Answer, &a); err != nil {
			t.Fatal(err)
		}
		as = append(as, a)
	}
	return as
}

// click clicks action on the card at p, as user, with choice and note.
func (tf *testFlow) click(t *testing.T, p Posted, user, action, choice, note string) {
	t.Helper()
	if err := tf.Click(t.Context(), slack.QuestionClick{ID: p.ID, Action: action, Choice: choice, Note: note, User: user, Channel: p.Channel, TS: p.TS, TriggerID: "T-" + action}); err != nil {
		t.Fatal(err)
	}
}

func (tf *testFlow) pass(t *testing.T) {
	t.Helper()
	if err := tf.Pass(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// Ask posts the card as a reply in the thread, mentioning who it names,
// and gives its id, ts and link; asking with the same ref again posts
// nothing and gives the same card. A card over the limits, a mention off
// the user list and a thread Slack does not have post nothing.
func TestAsk(t *testing.T) {
	tf := newFlow(t)
	p := tf.ask(t, "q-12", "U1")
	c, ok := tf.f.Question("C1", p.TS)
	if !ok || c.ID != p.ID || c.State != "" || len(c.Mentions) != 1 || c.Mentions[0] != "U1" || c.Question.Question != "要不要放宽第 5 条？" {
		t.Fatalf("card at %s = %+v, %v; want the question waiting, mentioning U1", p.TS, c, ok)
	}
	_, root, _ := slack.ParseThreadKey(tf.thread)
	if ms, _ := tf.f.Replies(t.Context(), "C1", root); len(ms) != 2 || ms[1].TS != p.TS {
		t.Fatalf("thread = %+v, want the card as the reply", ms)
	}
	if p.Channel != "C1" || p.Permalink == "" {
		t.Fatalf("Ask = %+v, want channel C1 and a link", p)
	}
	if again := tf.ask(t, "q-12"); again != p {
		t.Fatalf("Ask with the same ref = %+v, want %+v", again, p)
	}
	if ts, _ := tf.f.Questions("C1"); len(ts) != 1 {
		t.Fatalf("%d cards after asking twice with one ref, want 1", len(ts))
	}
	for _, c := range []struct {
		card, thread string
		mentions     []string
		want         error
	}{
		{`{"question": ""}`, tf.thread, nil, ErrInvalid},
		{card, tf.thread, []string{"U9"}, ErrNotListed},
		{card, "C1/9.9", nil, slack.ErrNotFound},
	} {
		if _, err := tf.Ask(t.Context(), "workstation", c.thread, "q-13", []byte(c.card), c.mentions); !errors.Is(err, c.want) {
			t.Errorf("Ask(%s, %s, %v) = %v, want %v", c.card, c.thread, c.mentions, err, c.want)
		}
	}
	if ts, _ := tf.f.Questions("C1"); len(ts) != 1 {
		t.Fatalf("%d cards after the refused asks, want 1", len(ts))
	}
}

// Submit sends the choice and the note down once and the card then shows
// them; a second press, a press by someone off the user list, by a bot or
// on a card that is not this one changes nothing and gets a hint in the
// card's thread.
func TestSubmit(t *testing.T) {
	tf := newFlow(t)
	p := tf.ask(t, "q-12")
	tf.click(t, p, "U9", slack.QuestionSubmitAction, "1", "")
	if err := tf.Click(t.Context(), slack.QuestionClick{ID: p.ID, Action: slack.QuestionSubmitAction, Choice: "1", User: "U1", Bot: true, Channel: p.Channel, TS: p.TS}); err != nil {
		t.Fatal(err)
	}
	if err := tf.Click(t.Context(), slack.QuestionClick{ID: p.ID, Action: slack.QuestionSubmitAction, Choice: "1", User: "U1", Channel: p.Channel, TS: "1.0"}); err != nil {
		t.Fatal(err)
	}
	if as := tf.answers(t); len(as) != 0 {
		t.Fatalf("answers after clicks that do not count = %+v, want none", as)
	}
	tf.pass(t)
	if w1, w9 := tf.f.Whispers("U1"), tf.f.Whispers("U9"); len(w1) != 2 || len(w9) != 1 {
		t.Fatalf("hints: U1 %q, U9 %q; want 2, 1", w1, w9)
	}
	_, root, _ := slack.ParseThreadKey(tf.thread)
	for _, u := range []string{"U1", "U9"} {
		for _, in := range tf.f.WhisperedIn(u) {
			if in != root {
				t.Fatalf("hints to %s shown in %q, want all in thread %s", u, tf.f.WhisperedIn(u), root)
			}
		}
	}

	tf.click(t, p, "U2", slack.QuestionSubmitAction, "1", "补充一句")
	tf.click(t, p, "U1", slack.QuestionSubmitAction, "0", "")
	want := question.Answer{QuestionID: "q-12", CardID: p.ID, Outcome: question.Answered, Choice: &question.Choice{Index: 1, Short: "放宽"}, Note: "补充一句", By: "U2", At: "2026-10-16T09:00:20Z"}
	if as := tf.answers(t); len(as) != 1 || !same(as[0], want) {
		t.Fatalf("answers = %+v, want only %+v", as, want)
	}
	tf.pass(t)
	if c, _ := tf.f.Question("C1", p.TS); c.State != question.Answered || !same(c.Answer, want) {
		t.Fatalf("card = %+v, want it to show %+v", c, want)
	}
	if w := tf.f.Whispers("U1"); len(w) != 3 || w[2] != "这个问题已经处理过了" {
		t.Fatalf("hints to U1 = %q, want one that the question is done", w)
	}

	// "other" with a note: no choice.
	p = tf.ask(t, "q-13")
	tf.click(t, p, "U1", slack.QuestionSubmitAction, slack.QuestionOther, "都不对")
	if as := tf.answers(t); len(as) != 1 || as[0].Choice != nil || as[0].Note != "都不对" {
		t.Fatalf("answers = %+v, want one with no choice and the note", as)
	}
}

// Submit with nothing picked, or "other", and no note does not count: the
// card says so above submit until a click that counts; a card that cannot be updated gets the
// warning to the clicker alone instead.
func TestSubmitWarning(t *testing.T) {
	const warning = "⚠️ 请先选一项；选「其他」的话，请先在补充框里写几句，再点「提交」。"
	tf := newFlow(t)
	p := tf.ask(t, "q-12")
	for _, choice := range []string{"", slack.QuestionOther} {
		tf.click(t, p, "U1", slack.QuestionSubmitAction, choice, "")
		tf.pass(t)
		if c, _ := tf.f.Question("C1", p.TS); c.State != "" || c.Warning != warning {
			t.Fatalf("card after submitting %q with no note = %+v, want it waiting with the warning", choice, c)
		}
	}
	if as, w := tf.answers(t), tf.f.Whispers("U1"); len(as) != 0 || len(w) != 0 {
		t.Fatalf("answers %+v, hints %q; want neither", as, w)
	}
	tf.click(t, p, "U1", "ask-snooze-2h", slack.QuestionOther, "")
	tf.pass(t)
	if c, _ := tf.f.Question("C1", p.TS); c.State != question.Snoozed || c.Warning != "" {
		t.Fatalf("card after a snooze = %+v, want it snoozed without the warning", c)
	}

	p = tf.ask(t, "q-13")
	tf.click(t, p, "U1", slack.QuestionSubmitAction, slack.QuestionOther, "")
	if err := tf.f.Delete(t.Context(), "C1", p.TS); err != nil {
		t.Fatal(err)
	}
	tf.pass(t)
	_, root, _ := slack.ParseThreadKey(tf.thread)
	if w, in := tf.f.Whispers("U1"), tf.f.WhisperedIn("U1"); len(w) != 1 || "⚠️ "+w[0] != warning || in[0] != root {
		t.Fatalf("hints to U1 = %q in %q, want the warning in thread %s", w, in, root)
	}
}

func same(a, b question.Answer) bool {
	x, _ := a.MarshalJSON()
	y, _ := b.MarshalJSON()
	return string(x) == string(y)
}

// Close, after its confirmation, sends closed down with the note.
func TestClose(t *testing.T) {
	tf := newFlow(t)
	p := tf.ask(t, "q-12")
	tf.click(t, p, "U1", slack.QuestionCloseAction, "1", "该换个问法")
	if as := tf.answers(t); len(as) != 1 || as[0].Outcome != question.Closed || as[0].Choice != nil || as[0].Note != "该换个问法" || as[0].By != "U1" {
		t.Fatalf("answers = %+v, want closed by U1 with the note", as)
	}
	tf.pass(t)
	if c, _ := tf.f.Question("C1", p.TS); c.State != question.Closed {
		t.Fatalf("card = %+v, want closed", c)
	}
}

// A snooze is timed from the database: a hub that restarts keeps it. When
// it ends, the card is posted again at the bottom of the thread,
// mentioning who snoozed it, the card before says where it went, and
// resurfaced goes down; the new card takes the next press, the old one
// only gives a hint.
func TestSnooze(t *testing.T) {
	tf := newFlow(t)
	p := tf.ask(t, "q-12")
	tf.click(t, p, "U2", "ask-snooze-2h", "1", "晚点再说")
	want := question.Answer{QuestionID: "q-12", CardID: p.ID, Outcome: question.Snoozed, Note: "晚点再说", SnoozeUntil: "2026-10-16T11:00:00Z", By: "U2", At: "2026-10-16T09:00:20Z"}
	if as := tf.answers(t); len(as) != 1 || !same(as[0], want) {
		t.Fatalf("answers = %+v, want %+v", as, want)
	}
	tf.pass(t)
	if c, _ := tf.f.Question("C1", p.TS); c.State != question.Snoozed {
		t.Fatalf("card = %+v, want snoozed", c)
	}
	_, root, _ := slack.ParseThreadKey(tf.thread)
	if _, err := tf.f.Reply("C1", root, "U1", "别的话题"); err != nil {
		t.Fatal(err)
	}

	tf.now = time.Date(2026, 10, 16, 10, 59, 0, 0, time.UTC)
	tf.open(t)
	tf.pass(t)
	if ts, _ := tf.f.Questions("C1"); len(ts) != 1 {
		t.Fatalf("%d cards before the snooze ends, want 1", len(ts))
	}
	tf.now = time.Date(2026, 10, 16, 11, 0, 30, 0, time.UTC)
	tf.pass(t)
	ms, _ := tf.f.Replies(t.Context(), "C1", root)
	last := ms[len(ms)-1]
	c, ok := tf.f.Question("C1", last.TS)
	if !ok || c.ID != p.ID || c.State != "" || len(c.Mentions) != 1 || c.Mentions[0] != "U2" {
		t.Fatalf("last message of the thread = %+v, card %+v; want the card again, waiting, mentioning U2", last, c)
	}
	if old, _ := tf.f.Question("C1", p.TS); old.State != slack.QuestionMoved {
		t.Fatalf("card before = %+v, want it to say where it went", old)
	}
	if as := tf.answers(t); len(as) != 1 || !same(as[0], question.Answer{QuestionID: "q-12", CardID: p.ID, Outcome: question.Resurfaced, At: "2026-10-16T11:00:30Z"}) {
		t.Fatalf("answers = %+v, want resurfaced", as)
	}
	tf.pass(t)
	if ts, _ := tf.f.Questions("C1"); len(ts) != 2 {
		t.Fatalf("%d cards after another pass, want 2", len(ts))
	}

	tf.click(t, p, "U1", slack.QuestionSubmitAction, "0", "")
	if as := tf.answers(t); len(as) != 0 {
		t.Fatalf("answers to a press on the card before = %+v, want none", as)
	}
	now := Posted{ID: p.ID, Channel: "C1", TS: last.TS}
	tf.click(t, now, "U1", slack.QuestionSubmitAction, "0", "")
	if as := tf.answers(t); len(as) != 1 || as[0].Outcome != question.Answered || as[0].Choice.Index != 0 {
		t.Fatalf("answers = %+v, want (a) answered", as)
	}
}

// "Pick a time" opens the picker with the card and the note; the time
// picked snoozes the card until then, a time gone by does not.
func TestPickTime(t *testing.T) {
	tf := newFlow(t)
	p := tf.ask(t, "q-12")
	tf.click(t, p, "U1", slack.QuestionPickTimeAction, "1", "下周再说")
	st, ok := tf.f.Picked("T-" + slack.QuestionPickTimeAction)
	if !ok || st != (slack.PickerState{ID: p.ID, Channel: p.Channel, TS: p.TS, Note: "下周再说"}) {
		t.Fatalf("picker = %+v, %v; want one for the card with the note", st, ok)
	}
	pick := func(at time.Time) {
		if err := tf.Click(t.Context(), slack.QuestionClick{ID: st.ID, Action: slack.QuestionTimePicker, Until: at, Note: st.Note, User: "U1", Channel: st.Channel, TS: st.TS}); err != nil {
			t.Fatal(err)
		}
	}
	pick(tf.now.Add(-time.Minute))
	if as := tf.answers(t); len(as) != 0 {
		t.Fatalf("answers to a time gone by = %+v, want none", as)
	}
	pick(time.Date(2026, 10, 20, 9, 30, 0, 0, time.UTC))
	if as := tf.answers(t); len(as) != 1 || as[0].Outcome != question.Snoozed || as[0].SnoozeUntil != "2026-10-20T09:30:00Z" || as[0].Note != "下周再说" {
		t.Fatalf("answers = %+v, want snoozed until 2026-10-20T09:30:00Z with the note", as)
	}
}

// The client that asked can take a card back, waiting or snoozed: the
// card says so, nothing goes down, and a snoozed one does not come back.
// Another client cannot, nor can anyone take back a card twice.
func TestWithdraw(t *testing.T) {
	tf := newFlow(t)
	p := tf.ask(t, "q-12")
	if err := tf.Withdraw(t.Context(), "datamachine", p.ID, ""); !errors.Is(err, ErrNotYours) {
		t.Fatalf("Withdraw by another client = %v, want ErrNotYours", err)
	}
	if err := tf.Withdraw(t.Context(), "workstation", p.ID, "job 结束了"); err != nil {
		t.Fatal(err)
	}
	tf.pass(t)
	if c, _ := tf.f.Question("C1", p.TS); c.State != slack.QuestionWithdrawn || c.Note != "job 结束了" {
		t.Fatalf("card = %+v, want withdrawn with the note", c)
	}
	if err := tf.Withdraw(t.Context(), "workstation", p.ID, ""); !errors.Is(err, ErrDone) {
		t.Fatalf("Withdraw again = %v, want ErrDone", err)
	}
	if _, err := tf.Store.Question(t.Context(), "nope"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Question(nope) = %v, want ErrNotFound", err)
	}

	p = tf.ask(t, "q-13")
	tf.click(t, p, "U1", "ask-snooze-30m", "", "")
	if err := tf.Withdraw(t.Context(), "workstation", p.ID, ""); err != nil {
		t.Fatal(err)
	}
	tf.now = tf.now.Add(time.Hour)
	tf.pass(t)
	if ts, _ := tf.f.Questions("C1"); len(ts) != 2 {
		t.Fatalf("%d cards, want the two asked and none posted again", len(ts))
	}
	if as := tf.answers(t); len(as) != 1 || as[0].Outcome != question.Snoozed {
		t.Fatalf("answers = %+v, want only the snooze", as)
	}
}

// limited makes U3 a limited user of tf on the workstation and returns a
// new thread in C1 that owner owns, or no one when owner is empty.
func (tf *testFlow) limited(t *testing.T, owner string) string {
	t.Helper()
	tf.Limited = route.Limited{"U3": {Name: "collaborator", Machines: []string{"workstation"}}}
	root, err := tf.f.Start("C1", "U1", "另一件事")
	if err != nil {
		t.Fatal(err)
	}
	thread := slack.ThreadKey("C1", root)
	if owner != "" {
		if err := tf.Store.Claim(t.Context(), thread, owner, "", ""); err != nil {
			t.Fatal(err)
		}
	}
	return thread
}

// A limited user may be mentioned on a card, and their press counts, by
// them, only on a card in a thread whose owner is on their list; on any
// other card it changes nothing and gets a hint.
func TestLimitedUserAnswers(t *testing.T) {
	tf := newFlow(t)
	ctx := t.Context()
	var ps []Posted
	for i, owner := range []string{"", "datamachine", "workstation"} {
		p, err := tf.Ask(ctx, "workstation", tf.limited(t, owner), "q-"+strconv.Itoa(i), []byte(card), []string{"U3"})
		if err != nil {
			t.Fatalf("Ask in a thread owned by %q, mentioning a limited user: %v", owner, err)
		}
		tf.click(t, p, "U3", slack.QuestionSubmitAction, "1", "")
		ps = append(ps, p)
	}
	want := question.Answer{QuestionID: "q-2", CardID: ps[2].ID, Outcome: question.Answered, Choice: &question.Choice{Index: 1, Short: "放宽"}, By: "U3", At: "2026-10-16T09:00:20Z"}
	if as := tf.answers(t); len(as) != 1 || !same(as[0], want) {
		t.Fatalf("answers = %+v, want only %+v", as, want)
	}
	tf.pass(t)
	for _, p := range ps[:2] {
		if c, _ := tf.f.Question("C1", p.TS); c.State != "" {
			t.Fatalf("card %s = %+v, want it still waiting", p.ID, c)
		}
	}
	if w := tf.f.Whispers("U3"); len(w) != 2 || w[0] != notOpenText || w[1] != notOpenText {
		t.Fatalf("hints to U3 = %q, want two that the card is not theirs to answer", w)
	}
}

// A limited user's press does not count on a card a client off their list
// asked, even in a thread whose owner is on it.
func TestLimitedUserAnswersOnlyTheirClients(t *testing.T) {
	tf := newFlow(t)
	ctx := t.Context()
	p, err := tf.Ask(ctx, "datamachine", tf.limited(t, "workstation"), "q-1", []byte(card), nil)
	if err != nil {
		t.Fatal(err)
	}
	tf.click(t, p, "U3", slack.QuestionSubmitAction, "1", "")
	if ds, err := tf.Store.Outbox.After(ctx, "datamachine", 0); err != nil || len(ds) != 0 {
		t.Fatalf("queued for datamachine = %d, %v; want nothing", len(ds), err)
	}
	tf.pass(t)
	if c, _ := tf.f.Question("C1", p.TS); c.State != "" {
		t.Fatalf("card = %+v, want it still waiting", c)
	}
	if w := tf.f.Whispers("U3"); len(w) != 1 || w[0] != notOpenText {
		t.Fatalf("hints to U3 = %q, want one that the card is not theirs to answer", w)
	}
}
