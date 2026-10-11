package outbound

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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
	_ "modernc.org/sqlite"
)

// flaky is a Slack API whose PostReply fails while fail is above zero,
// counting down.
type flaky struct {
	slack.API
	mu   sync.Mutex
	fail int
}

func (f *flaky) PostReply(ctx context.Context, channel, ts, text string) (string, error) {
	f.mu.Lock()
	if f.fail > 0 {
		f.fail--
		f.mu.Unlock()
		return "", errors.New("slack is down")
	}
	f.mu.Unlock()
	return f.API.PostReply(ctx, channel, ts, text)
}

type fixture struct {
	t      *testing.T
	st     *store.Hub
	path   string
	fake   *slack.Fake
	slack  *flaky
	p      *Poster
	thread string
	n      int
}

// newFixture returns a Poster over a fresh store and a fake Slack with one
// thread, the fixture's thread, in channel C1.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	path := filepath.Join(t.TempDir(), "hub.db")
	st, err := store.OpenHub(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	fake := &slack.Fake{}
	fake.AddChannel("C1", "")
	ts, err := fake.Start("C1", "U1", "please fix the build")
	if err != nil {
		t.Fatal(err)
	}
	fl := &flaky{API: fake}
	return &fixture{t: t, st: st, path: path, fake: fake, slack: fl, p: &Poster{Store: st, Slack: fl}, thread: slack.ThreadKey("C1", ts)}
}

// put stores a post of text to thread from client in the hub's inbox and
// returns its msg_id.
func (f *fixture) put(client, thread, text string) string {
	f.t.Helper()
	b, err := json.Marshal(payload.Message{Type: payload.Post, Thread: thread, Text: text})
	if err != nil {
		f.t.Fatal(err)
	}
	return f.putRaw(client, b)
}

func (f *fixture) putRaw(client string, b []byte) string {
	f.t.Helper()
	f.n++
	id := "m" + string(rune('0'+f.n))
	if _, err := f.st.Inbox.PutFrom(f.t.Context(), client, store.Message{MsgID: id, Payload: b}); err != nil {
		f.t.Fatal(err)
	}
	return id
}

// replies returns the fixture thread's replies, the first message left out.
func (f *fixture) replies() []slack.Message {
	f.t.Helper()
	_, ts, _ := slack.ParseThreadKey(f.thread)
	ms, err := f.fake.Replies(f.t.Context(), "C1", ts)
	if err != nil {
		f.t.Fatal(err)
	}
	return ms[1:]
}

func texts(ms []slack.Message) []string {
	var ts []string
	for _, m := range ms {
		ts = append(ts, m.Text)
	}
	return ts
}

func (f *fixture) undelivered() []string {
	f.t.Helper()
	us, err := f.st.Inbox.UndeliveredFrom(f.t.Context())
	if err != nil {
		f.t.Fatal(err)
	}
	var ids []string
	for _, u := range us {
		ids = append(ids, u.MsgID)
	}
	return ids
}

// A post goes to its thread as it is, without the name of the machine it
// came from, and is then delivered.
func TestPost(t *testing.T) {
	f := newFixture(t)
	f.put("workstation", f.thread, "the build is fixed")
	f.put("datamachine", f.thread, "the data is in")
	if err := f.p.Pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	ms := f.replies()
	if got := texts(ms); !slices.Equal(got, []string{"the build is fixed", "the data is in"}) {
		t.Fatalf("thread = %q, want both posts in order", got)
	}
	if u := f.undelivered(); len(u) != 0 {
		t.Fatalf("undelivered after the pass: %v", u)
	}
}

// A long post goes out as consecutive messages in its thread, in order.
func TestLongPostIsSplit(t *testing.T) {
	f := newFixture(t)
	f.p.MaxChars = 10
	text := "第一行写满了九个字\n第二行\n" + strings.Repeat("长", 25)
	f.put("workstation", f.thread, text)
	f.put("workstation", f.thread, "after")
	if err := f.p.Pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	got := texts(f.replies())
	want := []string{"第一行写满了九个字\n", "第二行\n长长长长长长", "长长长长长长长长长长", "长长长长长长长长长", "after"}
	if !slices.Equal(got, want) {
		t.Fatalf("thread = %q, want %q", got, want)
	}
}

// A post mentions, at the start of its first message only, the people
// it names who are on the user list; the Slack ts of each of its
// messages is recorded, footers' too, for a delete.
func TestPostMentionsAndRecordsWhere(t *testing.T) {
	f := newFixture(t)
	f.p.MaxChars = 10
	f.p.Users = map[string]string{"U1": "Ann", "U2": ""}
	b, err := json.Marshal(payload.Message{Type: payload.Post, Thread: f.thread, Text: "abcdefghijklm", Mentions: []string{"U1", "U9", "U2"}})
	if err != nil {
		t.Fatal(err)
	}
	long := f.putRaw("workstation", b)
	b, err = json.Marshal(payload.Message{Type: payload.Post, Thread: f.thread, Text: "done", Footer: true, Mentions: []string{"U1"}})
	if err != nil {
		t.Fatal(err)
	}
	footer := f.putRaw("workstation", b)
	if err := f.p.Pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	ms := f.replies()
	if got, want := texts(ms[:2]), []string{"<@U1> <@U2> abcdefghij", "klm"}; !slices.Equal(got, want) {
		t.Fatalf("thread = %q, want %q", got, want)
	}
	if slack.Mentions(ms[2].Text, "U1") {
		t.Fatalf("footer %q mentions U1", ms[2].Text)
	}
	for _, c := range []struct {
		id   string
		want []string
	}{{long, []string{ms[0].TS, ms[1].TS}}, {footer, []string{ms[2].TS}}} {
		p, err := f.st.Posted(t.Context(), c.id)
		if err != nil || !p.Delivered || !slices.Equal(p.TS, c.want) {
			t.Fatalf("Posted(%s) = %+v, %v; want delivered at %v", c.id, p, err, c.want)
		}
	}
}

func TestSplit(t *testing.T) {
	tests := []struct {
		text string
		max  int
		want []string
	}{
		{"short", 10, []string{"short"}},
		{"exactly10!", 10, []string{"exactly10!"}},
		{"abcdefghijk", 10, []string{"abcdefghij", "k"}},
		// A line break that would leave the part less than half full is
		// not used.
		{"ab\ncdefghijkl", 10, []string{"ab\ncdefghi", "jkl"}},
		{"abcdef\nghijkl", 10, []string{"abcdef\n", "ghijkl"}},
		{"ééééééééééé", 5, []string{"ééééé", "ééééé", "é"}},
	}
	for _, tt := range tests {
		if got := Split(tt.text, tt.max); !slices.Equal(got, tt.want) {
			t.Errorf("Split(%q, %d) = %q, want %q", tt.text, tt.max, got, tt.want)
		}
	}
}

// A post Slack does not take stays in the inbox, holds up the posts after
// it, and goes out on a later pass; a split post that failed halfway does
// not post its first parts again.
func TestFailedPostIsKeptAndRetried(t *testing.T) {
	f := newFixture(t)
	f.slack.fail = 1
	first := f.put("workstation", f.thread, "first")
	second := f.put("workstation", f.thread, "second")
	if err := f.p.Pass(t.Context()); err == nil {
		t.Fatal("a pass with Slack failing returned no error")
	}
	if got := f.undelivered(); !slices.Equal(got, []string{first, second}) {
		t.Fatalf("undelivered = %v, want both", got)
	}
	if got := f.replies(); len(got) != 0 {
		t.Fatalf("thread = %q, want nothing posted", texts(got))
	}
	if err := f.p.Pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := texts(f.replies()); !slices.Equal(got, []string{"first", "second"}) {
		t.Fatalf("thread after the retry = %q, want both in order", got)
	}

	f.p.MaxChars = 3
	f.put("workstation", f.thread, "abcdef")
	// The first part goes out, the second fails.
	f.slack.API = &failSecond{API: f.fake}
	if err := f.p.Pass(t.Context()); err == nil {
		t.Fatal("a pass with the second part failing returned no error")
	}
	f.slack.API = f.fake
	if err := f.p.Pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := texts(f.replies()); !slices.Equal(got, []string{"first", "second", "abc", "def"}) {
		t.Fatalf("thread = %q, want each part once", got)
	}
}

// failSecond fails every PostReply after the first.
type failSecond struct {
	slack.API
	n int
}

func (s *failSecond) PostReply(ctx context.Context, channel, ts, text string) (string, error) {
	s.n++
	if s.n > 1 {
		return "", errors.New("slack is down")
	}
	return s.API.PostReply(ctx, channel, ts, text)
}

// A post that can never go out is alerted and delivered, and the posts
// after it still go out.
func TestPermanentFailureIsAlertedAndSkipped(t *testing.T) {
	f := newFixture(t)
	var mu sync.Mutex
	var alerts []string
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m struct{ Text string }
		json.NewDecoder(r.Body).Decode(&m)
		mu.Lock()
		alerts = append(alerts, m.Text)
		mu.Unlock()
	}))
	defer hook.Close()
	f.p.Alert = &alert.Webhook{URL: hook.URL, From: "fednet-hub"}

	gone := f.put("workstation", "C1/1600000000.000001", "to a thread Slack does not know")
	bad := f.putRaw("workstation", []byte("not json"))
	f.put("workstation", f.thread, "still posted")
	if err := f.p.Pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := texts(f.replies()); !slices.Equal(got, []string{"still posted"}) {
		t.Fatalf("thread = %q, want the good post", got)
	}
	if u := f.undelivered(); len(u) != 0 {
		t.Fatalf("undelivered = %v, want none", u)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(alerts) != 2 || !strings.Contains(alerts[0], gone) || !strings.Contains(alerts[1], bad) {
		t.Fatalf("alerts = %q, want one for %s and one for %s", alerts, gone, bad)
	}
}

// Run posts what arrives when nudged.
func TestRun(t *testing.T) {
	f := newFixture(t)
	f.p.Interval = time.Hour
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.p.Run(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()
	f.put("workstation", f.thread, "hello")
	f.p.Nudge()
	deadline := time.Now().Add(5 * time.Second)
	for len(f.undelivered()) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the post")
		}
		time.Sleep(time.Millisecond)
	}
}

// Stop makes Run return without taking another post; the one in hand goes
// out and is marked first.
func TestStopTakesNoMorePosts(t *testing.T) {
	f := newFixture(t)
	f.p.Interval = time.Hour
	first := f.put("workstation", f.thread, "first")
	second := f.put("workstation", f.thread, "second")
	// Stop lands while the first post is with Slack.
	f.slack.API = &stopDuring{API: f.fake, p: f.p}
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.p.Run(t.Context())
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Stop")
	}
	if got := texts(f.replies()); !slices.Equal(got, []string{"first"}) {
		t.Fatalf("thread = %q, want just the post in hand", got)
	}
	if got := f.undelivered(); !slices.Equal(got, []string{second}) {
		t.Fatalf("undelivered = %v, want just %s; %s must be marked", got, second, first)
	}
}

// The parts of a split post already in Slack are recorded as they go: a
// Poster that takes the inbox over after a Stop in the middle of a failing
// post, or after the hub died there, posts only the parts not yet out.
func TestSplitProgressSurvivesStop(t *testing.T) {
	f := newFixture(t)
	f.p.MaxChars = 4
	f.p.Interval = time.Hour
	f.put("workstation", f.thread, "abcdefgh")
	// The first part goes out, the second fails; Stop lands then.
	f.slack.API = &failSecond{API: f.fake}
	stopper := &stopDuring{API: f.slack.API, p: f.p, after: 2}
	f.slack.API = stopper
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.p.Run(t.Context())
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Stop")
	}
	if got := texts(f.replies()); !slices.Equal(got, []string{"abcd"}) {
		t.Fatalf("thread after the stop = %q, want the first part", got)
	}
	next := &Poster{Store: f.st, Slack: f.fake, MaxChars: 4}
	if err := next.Pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := texts(f.replies()); !slices.Equal(got, []string{"abcd", "efgh"}) {
		t.Fatalf("thread after the next Poster = %q, want each part once", got)
	}
	if got := f.undelivered(); len(got) != 0 {
		t.Fatalf("undelivered = %v, want none", got)
	}
}

// A part's record the store refuses is retried until it is taken; the
// part is not posted again meanwhile, nor by the next Poster.
func TestPartRecordIsRetried(t *testing.T) {
	f := newFixture(t)
	f.p.MaxChars = 4
	f.p.Interval = 20 * time.Millisecond
	f.put("workstation", f.thread, "abcdefgh")
	db, err := sql.Open("sqlite", f.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.ExecContext(t.Context(), "CREATE TRIGGER fail BEFORE UPDATE OF parts_sent ON inbox BEGIN SELECT RAISE(ABORT, 'disk full'); END"); err != nil {
		t.Fatal(err)
	}
	passed := make(chan error, 1)
	go func() { passed <- f.p.Pass(t.Context()) }()
	deadline := time.Now().Add(5 * time.Second)
	for len(f.replies()) != 1 {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the first part")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-passed:
		t.Fatalf("Pass returned %v with the part unrecorded", err)
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := db.ExecContext(t.Context(), "DROP TRIGGER fail"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-passed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Pass did not return once the store took the record")
	}
	if got := texts(f.replies()); !slices.Equal(got, []string{"abcd", "efgh"}) {
		t.Fatalf("thread = %q, want each part once", got)
	}
}

// stopDuring calls Stop on the poster while posting: on the first post,
// or on the after-th.
type stopDuring struct {
	slack.API
	p     *Poster
	after int
	n     int
}

func (s *stopDuring) PostReply(ctx context.Context, channel, ts, text string) (string, error) {
	s.n++
	if s.n >= s.after {
		s.p.Stop()
	}
	return s.API.PostReply(ctx, channel, ts, text)
}

// An alert a client raised goes to the webhook as from that client, once.
// One the webhook does not take stays in the inbox for the next pass, and
// the posts behind it still go out.
func TestClientAlertIsRelayed(t *testing.T) {
	f := newFixture(t)
	var mu sync.Mutex
	var alerts []string
	down := true
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if down {
			http.Error(w, "down", http.StatusInternalServerError)
			return
		}
		var m struct{ Text string }
		json.NewDecoder(r.Body).Decode(&m)
		alerts = append(alerts, m.Text)
	}))
	defer hook.Close()
	f.p.Alert = &alert.Webhook{URL: hook.URL, From: "fednet-hub"}

	b, err := json.Marshal(payload.Message{Type: payload.Alert, Text: "dead letter: msg_id d1"})
	if err != nil {
		t.Fatal(err)
	}
	id := f.putRaw("workstation", b)
	f.put("workstation", f.thread, "posted behind the alert")
	if err := f.p.Pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := texts(f.replies()); !slices.Equal(got, []string{"posted behind the alert"}) {
		t.Fatalf("thread = %q, want the post", got)
	}
	if u := f.undelivered(); !slices.Equal(u, []string{id}) {
		t.Fatalf("undelivered = %v, want the alert %s", u, id)
	}

	mu.Lock()
	down = false
	mu.Unlock()
	for range 2 {
		if err := f.p.Pass(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if u := f.undelivered(); len(u) != 0 {
		t.Fatalf("undelivered = %v, want none", u)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(alerts) != 1 || !strings.HasPrefix(alerts[0], "[workstation] ") || !strings.Contains(alerts[0], "d1") {
		t.Fatalf("alerts = %q, want one, from workstation, about d1", alerts)
	}
}
