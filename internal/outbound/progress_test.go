package outbound

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
)

// send stores m from workstation and runs a pass.
func (f *fixture) send(m payload.Message) {
	f.t.Helper()
	m.Thread = f.thread
	b, err := json.Marshal(m)
	if err != nil {
		f.t.Fatal(err)
	}
	f.putRaw("workstation", b)
	if err := f.p.Pass(f.t.Context()); err != nil {
		f.t.Fatal(err)
	}
}

func progress(title string, items ...slack.ProgressItem) payload.Message {
	return payload.Message{Type: payload.Progress, Title: title, Items: items}
}

func item(text, state string) slack.ProgressItem { return slack.ProgressItem{Text: text, State: state} }

// card returns the card at ts in the fake Slack.
func (f *fixture) card(ts string) slack.Progress {
	f.t.Helper()
	p, ok := f.fake.Progress("C1", ts)
	if !ok {
		f.t.Fatalf("no progress card at %s", ts)
	}
	return p
}

// openCard returns the ts of the card the store has open in the thread,
// or "" when none is.
func (f *fixture) openCard() string {
	f.t.Helper()
	ts, _, err := f.st.OpenProgress(f.t.Context(), f.thread)
	if errors.Is(err, store.ErrNotFound) {
		return ""
	}
	if err != nil {
		f.t.Fatal(err)
	}
	return ts
}

// The first progress posts a card, the next ones replace it whole; -done
// closes it, the items still doing then done, and the next progress posts
// a new card.
func TestProgressCard(t *testing.T) {
	f := newFixture(t)
	f.send(progress("reading the issue", item("read the issue", slack.Doing)))
	ms := f.replies()
	if len(ms) != 1 || ms[0].Text != "进度：reading the issue" {
		t.Fatalf("thread = %q, want the card", texts(ms))
	}
	first := ms[0].TS
	if f.openCard() != first {
		t.Fatalf("open card = %q, want %s", f.openCard(), first)
	}

	f.send(progress("running the tests", item("read the issue", slack.Done), item("run the tests", slack.Doing), item("lint", slack.Failed)))
	if n := len(f.replies()); n != 1 {
		t.Fatalf("%d messages in the thread, want the one card, replaced", n)
	}
	want := slack.Progress{Title: "running the tests", Items: []slack.ProgressItem{item("read the issue", slack.Done), item("run the tests", slack.Doing), item("lint", slack.Failed)}}
	if got := f.card(first); !slices.Equal(got.Items, want.Items) || got.Title != want.Title {
		t.Fatalf("card = %+v, want %+v", got, want)
	}

	f.send(payload.Message{Type: payload.Progress, Close: slack.Done, Title: "tests pass"})
	want = slack.Progress{Title: "tests pass", Items: []slack.ProgressItem{item("read the issue", slack.Done), item("run the tests", slack.Done), item("lint", slack.Failed)}}
	if got := f.card(first); !slices.Equal(got.Items, want.Items) || got.Title != want.Title {
		t.Fatalf("closed card = %+v, want %+v", got, want)
	}
	if f.openCard() != "" {
		t.Fatalf("a card is still open after -done")
	}

	f.send(progress("a second look"))
	ms = f.replies()
	if len(ms) != 2 || ms[1].TS == first || f.openCard() != ms[1].TS {
		t.Fatalf("thread = %q, open card %q; want a new card after the first", texts(ms), f.openCard())
	}
	if u := f.undelivered(); len(u) != 0 {
		t.Fatalf("undelivered = %v, want none", u)
	}
}

// -error closes the card with the items still doing in error; -done and
// -error with no card open do nothing.
func TestProgressError(t *testing.T) {
	f := newFixture(t)
	f.send(payload.Message{Type: payload.Progress, Close: slack.Done})
	if ms := f.replies(); len(ms) != 0 {
		t.Fatalf("thread = %q after -done with no card, want nothing", texts(ms))
	}
	f.send(progress("deploying", item("build", slack.Done), item("deploy", slack.Doing)))
	ts := f.replies()[0].TS
	f.send(payload.Message{Type: payload.Progress, Close: slack.Failed})
	if got := f.card(ts); got.Title != "deploying" || !slices.Equal(got.Items, []slack.ProgressItem{item("build", slack.Done), item("deploy", slack.Failed)}) {
		t.Fatalf("card = %+v, want deploy in error", got)
	}
	f.send(payload.Message{Type: payload.Progress, Close: slack.Failed})
	if n := len(f.replies()); n != 1 || f.openCard() != "" {
		t.Fatalf("%d messages, open card %q after a second -error; want the one closed card", n, f.openCard())
	}
}

// A post, a footer among them, first closes the open card as -done does;
// the next progress then posts a new card below the post. A footer ends
// with the machine it came from.
func TestPostClosesTheCard(t *testing.T) {
	f := newFixture(t)
	f.send(progress("fixing", item("fix", slack.Doing)))
	f.send(payload.Message{Type: payload.Post, Text: "fixed"})
	ms := f.replies()
	if got := texts(ms); !slices.Equal(got, []string{"进度：fixing", "fixed"}) {
		t.Fatalf("thread = %q, want the card, then the post", got)
	}
	if got := f.card(ms[0].TS); got.Items[0].State != slack.Done || f.openCard() != "" {
		t.Fatalf("card = %+v, open %q; want it closed, done", got, f.openCard())
	}

	f.send(progress("checking", item("check", slack.Doing)))
	f.send(payload.Message{Type: payload.Post, Footer: true, Text: "会话已结束 · [T-1](https://example.invalid/T-1)"})
	ms = f.replies()
	if got := texts(ms); !slices.Equal(got, []string{"进度：fixing", "fixed", "进度：checking", "会话已结束 · T-1 · workstation"}) {
		t.Fatalf("thread = %q, want a new card after the post, then the footer", got)
	}
	if got := f.card(ms[2].TS); got.Items[0].State != slack.Done {
		t.Fatalf("card = %+v, want it closed by the footer", got)
	}
	if m, ok := f.fake.FooterText(ms[3].TS); !ok || m != "会话已结束 · <https://example.invalid/T-1|T-1> · workstation" {
		t.Fatalf("footer = %q, %v; want the link in mrkdwn and the machine at the end", m, ok)
	}
}

// The open card is in the store: a hub started over the same database
// replaces the same card.
func TestProgressSurvivesRestart(t *testing.T) {
	f := newFixture(t)
	f.send(progress("step 1", item("one", slack.Doing)))
	ts := f.replies()[0].TS
	f.st.Close()
	st, err := store.OpenHub(t.Context(), f.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	f.st, f.p = st, &Poster{Store: st, Slack: f.slack}
	f.send(progress("step 2", item("one", slack.Done), item("two", slack.Doing)))
	if n := len(f.replies()); n != 1 {
		t.Fatalf("%d messages in the thread, want the card replaced", n)
	}
	if got := f.card(ts); got.Title != "step 2" || len(got.Items) != 2 {
		t.Fatalf("card = %+v, want step 2", got)
	}
}

// A card deleted in Slack is posted again; a card Slack would refuse is
// dropped without holding up what follows.
func TestProgressCardGoneOrTooBig(t *testing.T) {
	f := newFixture(t)
	f.send(progress("step 1"))
	first := f.replies()[0].TS
	f.fake.DeleteProgress("C1", first)
	f.send(progress("step 2"))
	ms := f.replies()
	if len(ms) != 2 || f.openCard() != ms[1].TS || f.card(ms[1].TS).Title != "step 2" {
		t.Fatalf("thread = %q, open card %q; want a new card for step 2", texts(ms), f.openCard())
	}

	items := make([]slack.ProgressItem, slack.MaxProgressItems+1)
	for i := range items {
		items[i] = item("x", slack.Done)
	}
	f.send(progress("too many", items...))
	f.send(payload.Message{Type: payload.Post, Text: "after"})
	if got := texts(f.replies()); got[len(got)-1] != "after" || f.card(ms[1].TS).Title != "step 2" {
		t.Fatalf("thread = %q, want the post after the refused card", got)
	}
}

// BeforeUpload waits until the client's earlier posts are in Slack, then
// closes the card: a progress queued just before an upload is closed
// before the upload goes out. When the client's posts do not go out, it
// gives up.
func TestBeforeUpload(t *testing.T) {
	uploadPoll = time.Millisecond
	t.Cleanup(func() { uploadPoll = 100 * time.Millisecond })
	f := newFixture(t)
	b, _ := json.Marshal(payload.Message{Type: payload.Progress, Thread: f.thread, Title: "taking a screenshot", Items: []slack.ProgressItem{item("shoot", slack.Doing)}})
	f.putRaw("workstation", b)

	ready := make(chan error, 1)
	go func() { ready <- f.p.BeforeUpload(t.Context(), "workstation", f.thread) }()
	select {
	case err := <-ready:
		t.Fatalf("BeforeUpload = %v before the progress went out", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := f.p.Pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
	ms := f.replies()
	if got := f.card(ms[0].TS); got.Items[0].State != slack.Done || f.openCard() != "" {
		t.Fatalf("card = %+v, open %q; want it closed before the upload", got, f.openCard())
	}

	f.put("workstation", f.thread, "stuck")
	f.p.UploadWait = 5 * time.Millisecond
	if err := f.p.BeforeUpload(t.Context(), "workstation", f.thread); err == nil || !strings.Contains(err.Error(), "not in Slack") {
		t.Fatalf("BeforeUpload with a post not out = %v, want it to give up", err)
	}
}
