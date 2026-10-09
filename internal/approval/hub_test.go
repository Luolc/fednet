package approval

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
)

const action = `{"op": "delete", "bucket": "b"}`

// testFlow is a Flow on a fresh store and a Fake Slack with the channel
// "C9" for cards, U1 and U2 as approvers, and a clock the test moves.
type testFlow struct {
	*Flow
	f    *slack.Fake
	pub  ed25519.PublicKey
	now  time.Time
	sent int
}

func newFlow(t *testing.T) *testFlow {
	t.Helper()
	st, err := store.OpenHub(t.Context(), filepath.Join(t.TempDir(), "hub.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	f := &slack.Fake{}
	f.AddChannel("C9", "approvals")
	pub, priv := keyPair(t)
	tf := &testFlow{f: f, pub: pub, now: time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)}
	tf.Flow = &Flow{Store: st, Slack: f, Key: priv, Channel: "C9", Approvers: []string{"U1", "U2"},
		Now: func() time.Time { return tf.now }, Stored: func() { tf.sent++ }}
	return tf
}

// request asks for approval of action on behalf of requester and returns
// the id and the card's ts.
func (tf *testFlow) request(t *testing.T, requester string) (id, ts string) {
	t.Helper()
	id, err := tf.Request(t.Context(), "workstation", "ops-exec", requester, "delete bucket b", []byte(action))
	if err != nil {
		t.Fatal(err)
	}
	tss, cards := tf.f.Cards("C9")
	for i, c := range cards {
		if c.ID == id {
			return id, tss[i]
		}
	}
	t.Fatalf("no card for %s in C9, cards = %+v", id, cards)
	return "", ""
}

func (tf *testFlow) click(t *testing.T, id, ts, user string, approve bool) {
	t.Helper()
	if err := tf.Click(t.Context(), slack.Click{ID: id, Approve: approve, User: user, Channel: "C9", TS: ts}); err != nil {
		t.Fatal(err)
	}
}

// outcomes returns the approval outcomes queued for client.
func (tf *testFlow) outcomes(t *testing.T, client string) []payload.Message {
	t.Helper()
	ds, err := tf.Store.Outbox.After(t.Context(), client, 0)
	if err != nil {
		t.Fatal(err)
	}
	var ms []payload.Message
	for _, d := range ds {
		var m payload.Message
		if err := json.Unmarshal(d.Payload, &m); err != nil {
			t.Fatal(err)
		}
		ms = append(ms, m)
	}
	return ms
}

func (tf *testFlow) card(t *testing.T, ts string) slack.Card {
	t.Helper()
	c, ok := tf.f.Card("C9", ts)
	if !ok {
		t.Fatalf("no card at %s", ts)
	}
	return c
}

// A request posts a card with the buttons; an approver's click signs the
// approval, sends it to the client and makes the card final; a second
// click on the same card changes nothing and only tells the clicker.
func TestFlowApprove(t *testing.T) {
	tf := newFlow(t)
	id, ts := tf.request(t, "")
	c := tf.card(t, ts)
	if c.Outcome != "" || c.ID != id || c.Summary != "delete bucket b" || c.Params != action || c.Machine != "workstation" || c.Agent != "ops-exec" || !c.Expires.Equal(tf.now.Add(TTL)) {
		t.Fatalf("card = %+v, want a pending card for %s expiring at %s", c, id, tf.now.Add(TTL))
	}
	if ms := tf.outcomes(t, "workstation"); len(ms) != 0 {
		t.Fatalf("outcomes before any click = %+v", ms)
	}

	tf.now = tf.now.Add(10 * time.Minute)
	tf.click(t, id, ts, "U1", true)
	ms := tf.outcomes(t, "workstation")
	if len(ms) != 1 || ms[0].Type != payload.Approval || ms[0].ApprovalID != id || ms[0].Outcome != payload.Approved || ms[0].Approver != "U1" || ms[0].Agent != "ops-exec" || ms[0].Text != "delete bucket b" {
		t.Fatalf("outcomes = %+v, want one approved %s by U1", ms, id)
	}
	if tf.sent != 1 {
		t.Fatalf("Stored called %d times, want 1", tf.sent)
	}
	var a Approval
	if err := json.Unmarshal(ms[0].Approval, &a); err != nil {
		t.Fatal(err)
	}
	want := Content{ApprovalID: id, ParamsSHA256: sha256.Sum256([]byte(action)), TargetMachine: "workstation", TargetAgent: "ops-exec",
		Approver: "U1", ApprovedAt: tf.now, ExpiresAt: tf.now.Add(TTL), Nonce: a.Nonce}
	if !a.ApprovedAt.Equal(want.ApprovedAt) || !a.ExpiresAt.Equal(want.ExpiresAt) || len(a.Nonce) != 32 {
		t.Fatalf("approval = %+v, want approved at %s, expiring at %s, with a 32-byte nonce", a.Content, want.ApprovedAt, want.ExpiresAt)
	}
	a.ApprovedAt, a.ExpiresAt = want.ApprovedAt, want.ExpiresAt
	if err := Verify(tf.pub, a.Content, a.Signature, tf.now.Add(TTL-time.Second)); err != nil {
		t.Fatalf("the hub's signature does not verify: %v", err)
	}
	if a.ParamsSHA256 != want.ParamsSHA256 || a.TargetMachine != want.TargetMachine || a.TargetAgent != want.TargetAgent || a.Approver != want.Approver {
		t.Fatalf("content = %+v, want %+v", a.Content, want)
	}
	c = tf.card(t, ts)
	if c.Outcome != payload.Approved || c.Approver != "U1" || !c.DecidedAt.Equal(tf.now) {
		t.Fatalf("card after approval = %+v, want approved by U1 at %s", c, tf.now)
	}
	rec, err := tf.Store.Approval(t.Context(), id)
	if err != nil || !rec.CardFinal {
		t.Fatalf("record = %+v, %v; want the card marked final", rec, err)
	}

	// Again, by the other approver: nothing more is signed or sent.
	tf.click(t, id, ts, "U2", true)
	tf.click(t, id, ts, "U1", false)
	if ms := tf.outcomes(t, "workstation"); len(ms) != 1 {
		t.Fatalf("outcomes after clicking again = %+v, want still one", ms)
	}
	if w := tf.f.Whispers("U2"); len(w) != 1 || !strings.Contains(w[0], "已经处理过了") {
		t.Fatalf("U2 was told %q, want that the card was handled", w)
	}
	if w := tf.f.Whispers("U1"); len(w) != 1 || !strings.Contains(w[0], "已经处理过了") {
		t.Fatalf("U1 was told %q, want that the card was handled", w)
	}
}

// Clicks by someone not on the approver list, by a bot, and by the
// requester do not count: the approval stays pending, nothing is sent,
// and the clicker is told why. An approver's click still counts after.
func TestFlowRefusesClicks(t *testing.T) {
	tf := newFlow(t)
	id, ts := tf.request(t, "U2")
	for _, c := range []struct {
		user string
		bot  bool
		want string
	}{
		{"U3", false, "不在审批人名单"},
		{"B1", true, "bot"},
		{"U2", false, "请求方"},
	} {
		if err := tf.Click(t.Context(), slack.Click{ID: id, Approve: true, User: c.user, Bot: c.bot, Channel: "C9", TS: ts}); err != nil {
			t.Fatal(err)
		}
		if w := tf.f.Whispers(c.user); len(w) != 1 || !strings.Contains(w[0], c.want) {
			t.Errorf("%s (bot %v) was told %q, want %q", c.user, c.bot, w, c.want)
		}
		if err := tf.Click(t.Context(), slack.Click{ID: id, Approve: false, User: c.user, Bot: c.bot, Channel: "C9", TS: ts}); err != nil {
			t.Fatal(err)
		}
	}
	if ms := tf.outcomes(t, "workstation"); len(ms) != 0 {
		t.Fatalf("outcomes after refused clicks = %+v, want none", ms)
	}
	if c := tf.card(t, ts); c.Outcome != "" {
		t.Fatalf("card after refused clicks = %+v, want still pending", c)
	}
	// A click on a card that does not exist is told so.
	tf.click(t, "nope", ts, "U1", true)
	if w := tf.f.Whispers("U1"); len(w) != 1 || !strings.Contains(w[0], "没有这张审批卡") {
		t.Fatalf("U1 was told %q, want that there is no such card", w)
	}

	tf.click(t, id, ts, "U1", true)
	if ms := tf.outcomes(t, "workstation"); len(ms) != 1 || ms[0].Outcome != payload.Approved {
		t.Fatalf("outcomes after U1's click = %+v, want one approved", ms)
	}
}

// A rejection is sent to the client without a signature, and the card
// says so.
func TestFlowReject(t *testing.T) {
	tf := newFlow(t)
	id, ts := tf.request(t, "")
	tf.click(t, id, ts, "U2", false)
	ms := tf.outcomes(t, "workstation")
	if len(ms) != 1 || ms[0].ApprovalID != id || ms[0].Outcome != payload.Rejected || ms[0].Approver != "U2" || len(ms[0].Approval) != 0 {
		t.Fatalf("outcomes = %+v, want one rejected %s by U2 with no approval document", ms, id)
	}
	if c := tf.card(t, ts); c.Outcome != payload.Rejected || c.Approver != "U2" {
		t.Fatalf("card = %+v, want rejected by U2", c)
	}
	tf.click(t, id, ts, "U1", true)
	if ms := tf.outcomes(t, "workstation"); len(ms) != 1 {
		t.Fatalf("outcomes after approving a rejected card = %+v, want still one", ms)
	}
}

// At the expiry a pending approval expires: the sweep sends the outcome,
// unsigned, and the card says so; a click at the expiry does the same and
// signs nothing; a click after that only tells the clicker.
func TestFlowExpiry(t *testing.T) {
	tf := newFlow(t)
	tf.TTL = 20 * time.Minute
	swept, _ := tf.request(t, "")
	clicked, clickedTS := tf.request(t, "")
	tf.now = tf.now.Add(20*time.Minute - time.Second)
	if err := tf.Pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	if ms := tf.outcomes(t, "workstation"); len(ms) != 0 {
		t.Fatalf("outcomes a second before the expiry = %+v, want none", ms)
	}
	tf.now = tf.now.Add(time.Second)
	tf.click(t, clicked, clickedTS, "U1", true)
	if w := tf.f.Whispers("U1"); len(w) != 1 || !strings.Contains(w[0], "过期") {
		t.Fatalf("U1 was told %q, want that the card expired", w)
	}
	if err := tf.Pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	ms := tf.outcomes(t, "workstation")
	if len(ms) != 2 || ms[0].ApprovalID != clicked || ms[1].ApprovalID != swept {
		t.Fatalf("outcomes = %+v, want the clicked one then the swept one", ms)
	}
	for _, m := range ms {
		if m.Outcome != payload.Expired || m.Approver != "" || len(m.Approval) != 0 {
			t.Fatalf("outcome %+v, want expired, by no one, unsigned", m)
		}
	}
	_, cards := tf.f.Cards("C9")
	for _, c := range cards {
		if c.Outcome != payload.Expired || !c.DecidedAt.Equal(tf.now) {
			t.Fatalf("card %+v, want expired at %s", c, tf.now)
		}
	}
	tf.click(t, clicked, clickedTS, "U1", true)
	if w := tf.f.Whispers("U1"); len(w) != 2 || !strings.Contains(w[1], "已经处理过了：已过期") {
		t.Fatalf("U1 was told %q, want that the card was handled, expired", w)
	}
	if ms := tf.outcomes(t, "workstation"); len(ms) != 2 {
		t.Fatalf("outcomes after clicking an expired card = %+v, want still two", ms)
	}
}

// Without a key, Slack, a channel or approvers, a request fails with
// ErrOff and posts nothing.
func TestFlowOff(t *testing.T) {
	tf := newFlow(t)
	for name, off := range map[string]func(f *Flow){
		"key":       func(f *Flow) { f.Key = nil },
		"slack":     func(f *Flow) { f.Slack = nil },
		"channel":   func(f *Flow) { f.Channel = "" },
		"approvers": func(f *Flow) { f.Approvers = nil },
	} {
		save := *tf.Flow
		off(tf.Flow)
		_, err := tf.Request(t.Context(), "workstation", "ops-exec", "", "delete bucket b", []byte(action))
		if !errors.Is(err, ErrOff) {
			t.Errorf("without the %s: Request returned %v, want ErrOff", name, err)
		}
		*tf.Flow = save
	}
	if _, cards := tf.f.Cards("C9"); len(cards) != 0 {
		t.Fatalf("cards posted while off = %+v, want none", cards)
	}
	if ps, err := tf.Store.PendingApprovals(t.Context()); err != nil || len(ps) != 0 {
		t.Fatalf("pending while off = %+v, %v; want none", ps, err)
	}
}

// A card Slack does not update when the approval is decided is updated by
// the next sweep.
func TestFlowCardFinishedLater(t *testing.T) {
	tf := newFlow(t)
	fl := &flakyUpdate{Fake: tf.f, fail: true}
	tf.Slack = fl
	id, ts := tf.request(t, "")
	tf.click(t, id, ts, "U1", true)
	if ms := tf.outcomes(t, "workstation"); len(ms) != 1 || ms[0].Outcome != payload.Approved {
		t.Fatalf("outcomes = %+v, want one approved although the card update failed", ms)
	}
	if c := tf.card(t, ts); c.Outcome != "" {
		t.Fatalf("card = %+v, want still as posted after the failed update", c)
	}
	rec, err := tf.Store.Approval(t.Context(), id)
	if err != nil || rec.CardFinal {
		t.Fatalf("record = %+v, %v; want the card not final yet", rec, err)
	}
	fl.fail = false
	if err := tf.Pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	if c := tf.card(t, ts); c.Outcome != payload.Approved {
		t.Fatalf("card after the sweep = %+v, want approved", c)
	}
	if rec, err := tf.Store.Approval(t.Context(), id); err != nil || !rec.CardFinal {
		t.Fatalf("record after the sweep = %+v, %v; want the card final", rec, err)
	}
	if ms := tf.outcomes(t, "workstation"); len(ms) != 1 {
		t.Fatalf("outcomes after the sweep = %+v, want still one", ms)
	}
}

// flakyUpdate is a Fake whose UpdateCard fails while fail is set.
type flakyUpdate struct {
	*slack.Fake
	fail bool
}

func (f *flakyUpdate) UpdateCard(ctx context.Context, channel, ts string, c slack.Card) error {
	if f.fail {
		return errors.New("slack is away")
	}
	return f.Fake.UpdateCard(ctx, channel, ts, c)
}

// A request whose approval cannot be recorded takes its card down and
// fails, so no card without an approval behind it stays in Slack.
func TestFlowRequestUndoneWhenRecordFails(t *testing.T) {
	tf := newFlow(t)
	tf.Store.Close()
	_, err := tf.Request(t.Context(), "workstation", "ops-exec", "", "delete bucket b", []byte(action))
	if err == nil || !strings.Contains(err.Error(), "card was deleted") {
		t.Fatalf("Request = %v, want an error saying the card was deleted", err)
	}
	if _, cards := tf.f.Cards("C9"); len(cards) != 0 {
		t.Fatalf("cards = %+v, want none", cards)
	}
}
