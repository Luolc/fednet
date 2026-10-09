package watch

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Luolc/fednet/internal/alert"
	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
)

// link is a SlackLink whose DownFor the test sets.
type link struct {
	mu   sync.Mutex
	down time.Duration
}

func (l *link) DownFor() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.down
}

// hook is a test alerts webhook; while failing, it answers 500.
type hook struct {
	mu      sync.Mutex
	failing bool
	alerts  []string
}

func (h *hook) got() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.alerts)
}

type fixture struct {
	t      *testing.T
	w      *Watch
	link   *link
	hook   *hook
	fake   *slack.Fake
	online map[string]bool
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	st, err := store.OpenHub(t.Context(), filepath.Join(t.TempDir(), "hub.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	h := &hook{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m struct{ Text string }
		json.NewDecoder(r.Body).Decode(&m)
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.failing {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		h.alerts = append(h.alerts, m.Text)
	}))
	t.Cleanup(srv.Close)
	fake := &slack.Fake{}
	fake.AddChannel("C1", "")
	f := &fixture{t: t, link: &link{}, hook: h, fake: fake, online: map[string]bool{}}
	f.w = &Watch{
		Store:     st,
		Slack:     fake,
		Link:      f.link,
		Online:    func(c string) bool { return f.online[c] },
		Alert:     &alert.Webhook{URL: srv.URL, From: "fednet-hub"},
		SlackDown: 5 * time.Minute,
		// Every queued message has waited this long.
		Offline: time.Nanosecond,
	}
	return f
}

func (f *fixture) check() {
	f.t.Helper()
	if err := f.w.Check(f.t.Context()); err != nil {
		f.t.Fatal(err)
	}
}

func TestSlackDown(t *testing.T) {
	f := newFixture(t)
	steps := []struct {
		down time.Duration
		want int
	}{
		{0, 0},
		{4 * time.Minute, 0},
		{6 * time.Minute, 1},
		{7 * time.Minute, 1},
		// Back, then cut off again: a new occurrence.
		{0, 1},
		{time.Minute, 1},
		{6 * time.Minute, 2},
	}
	for i, s := range steps {
		f.link.down = s.down
		f.check()
		if got := f.hook.got(); len(got) != s.want {
			t.Fatalf("step %d (down %v): %d alerts %q, want %d", i, s.down, len(got), got, s.want)
		}
	}
	if got := f.hook.got()[0]; !strings.Contains(got, "fednet-hub") || !strings.Contains(got, "Slack") {
		t.Fatalf("alert %q does not name the hub and Slack", got)
	}
}

// An alert that fails to go out is tried again.
func TestFailedAlertIsRetried(t *testing.T) {
	f := newFixture(t)
	f.link.down = 6 * time.Minute
	f.hook.failing = true
	if err := f.w.Check(t.Context()); err == nil {
		t.Fatal("Check with the webhook failing returned no error")
	}
	f.hook.failing = false
	f.check()
	f.check()
	if got := f.hook.got(); len(got) != 1 {
		t.Fatalf("alerts = %q, want one", got)
	}
}

// queue starts a thread in C1 and queues a message in it for client; it
// returns the thread's ts.
func (f *fixture) queue(client string) string {
	f.t.Helper()
	ts, err := f.fake.Start("C1", "U1", "please fix the build")
	if err != nil {
		f.t.Fatal(err)
	}
	b, _ := json.Marshal(payload.Message{Type: "message", Thread: slack.ThreadKey("C1", ts), Text: "please fix the build"})
	if _, err := f.w.Store.Outbox.Enqueue(f.t.Context(), client, b); err != nil {
		f.t.Fatal(err)
	}
	return ts
}

// told returns the texts fednet posted in the thread at ts.
func (f *fixture) told(ts string) []string {
	f.t.Helper()
	ms, err := f.fake.Replies(f.t.Context(), "C1", ts)
	if err != nil {
		f.t.Fatal(err)
	}
	var texts []string
	for _, m := range ms {
		if m.User == "fednet" {
			texts = append(texts, m.Text)
		}
	}
	return texts
}

func TestOfflineWithQueue(t *testing.T) {
	f := newFixture(t)
	ctx := t.Context()
	for _, c := range []string{"workstation", "datamachine"} {
		if err := f.w.Store.Register(ctx, c, []byte("hash")); err != nil {
			t.Fatal(err)
		}
	}
	// datamachine is online with a queue, workstation offline without one:
	// neither is alerted.
	f.online["datamachine"] = true
	f.queue("datamachine")
	f.check()
	if got := f.hook.got(); len(got) != 0 {
		t.Fatalf("alerts = %q, want none", got)
	}

	// Under the threshold: no alert.
	t1 := f.queue("workstation")
	f.w.Offline = time.Hour
	f.check()
	if got := f.hook.got(); len(got) != 0 {
		t.Fatalf("alerts before the threshold = %q, want none", got)
	}

	f.w.Offline = time.Nanosecond
	f.check()
	f.check()
	got := f.hook.got()
	if len(got) != 1 || !strings.Contains(got[0], "workstation") || !strings.Contains(got[0], "offline") {
		t.Fatalf("alerts = %q, want one about workstation", got)
	}
	if told := f.told(t1); len(told) != 1 || !strings.Contains(told[0], "workstation is offline") {
		t.Fatalf("thread got %q, want one notice that workstation is offline", told)
	}

	// A thread queued later is told too, without a second alert.
	t2 := f.queue("workstation")
	f.check()
	if len(f.told(t2)) != 1 || len(f.told(t1)) != 1 || len(f.hook.got()) != 1 {
		t.Fatalf("after a new thread: told %q and %q, alerts %q; want one notice each, one alert", f.told(t1), f.told(t2), f.hook.got())
	}

	// Back online, then offline again: a new occurrence.
	f.online["workstation"] = true
	f.check()
	f.online["workstation"] = false
	f.check()
	if len(f.hook.got()) != 2 || len(f.told(t1)) != 2 {
		t.Fatalf("after going offline again: alerts %q, told %q; want a second alert and notice", f.hook.got(), f.told(t1))
	}
}
