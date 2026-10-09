package upgrade

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
)

// bench is a Hub on a fresh store with two registered clients, with what
// it sends and says recorded.
type bench struct {
	h  *Hub
	f  *fakeReleases
	sl *slack.Fake
	// online is the set of clients Online reports.
	mu     sync.Mutex
	online map[string]bool
	sent   map[string][]string // client -> versions told
	said   []string
	alerts chan string
}

func newBench(t *testing.T) *bench {
	t.Helper()
	st, err := store.OpenHub(t.Context(), filepath.Join(t.TempDir(), "hub.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	for _, c := range []string{"datamachine", "workstation"} {
		if err := st.Register(t.Context(), c, []byte("hash")); err != nil {
			t.Fatal(err)
		}
		if err := st.SetVersion(t.Context(), c, "v0.1.0"); err != nil {
			t.Fatal(err)
		}
	}
	b := &bench{f: newFakeReleases(t), sl: &slack.Fake{}, online: map[string]bool{"datamachine": true, "workstation": true}, sent: make(map[string][]string), alerts: make(chan string, 64)}
	b.f.publish("v0.2.0", map[string][]byte{"amd64": []byte("two")})
	b.h = &Hub{
		Store: st, Version: "v0.1.0", Releases: b.f.releases(), Slack: b.sl,
		Request: filepath.Join(t.TempDir(), "upgrade"),
		Users:   map[string]string{"U1": "maintainer", "U2": "someone"}, Admins: []string{"U1"},
		Wait: 300 * time.Millisecond, Poll: 5 * time.Millisecond, Interval: 20 * time.Millisecond,
		Online: func(c string) bool {
			b.mu.Lock()
			defer b.mu.Unlock()
			return b.online[c]
		},
		Send: func(_ context.Context, client string, p []byte) error {
			var m payload.Message
			if err := json.Unmarshal(p, &m); err != nil || m.Type != payload.Upgrade {
				return errors.New("not an upgrade notice")
			}
			b.mu.Lock()
			defer b.mu.Unlock()
			b.sent[client] = append(b.sent[client], m.Version)
			return nil
		},
		Alert: func(_ context.Context, text string) error {
			b.mu.Lock()
			b.said = append(b.said, text)
			b.mu.Unlock()
			b.alerts <- text
			return nil
		},
	}
	return b
}

// told returns the versions client was told to upgrade to.
func (b *bench) told(client string) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.sent[client]...)
}

// next returns the next thing the hub said, within a few seconds.
func (b *bench) next(t *testing.T) string {
	t.Helper()
	select {
	case s := <-b.alerts:
		return s
	case <-time.After(5 * time.Second):
		t.Fatal("the hub said nothing")
		return ""
	}
}

// quiet checks that the hub says nothing more.
func (b *bench) quiet(t *testing.T) {
	t.Helper()
	select {
	case s := <-b.alerts:
		t.Fatalf("the hub went on to say %q", s)
	case <-time.After(50 * time.Millisecond):
	}
}

func (b *bench) request(t *testing.T) (string, bool) {
	t.Helper()
	v, err := ReadRequest(b.h.Request)
	if errors.Is(err, ErrNoRequest) {
		return "", false
	}
	if err != nil {
		t.Fatal(err)
	}
	return v, true
}

// run runs the hub's Run until the test ends.
func (b *bench) run(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.h.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

// cmd sends the slash command text from user and returns the reply.
func (b *bench) cmd(t *testing.T, user, text string) slack.CommandReply {
	t.Helper()
	r, err := b.h.Command(t.Context(), slack.Command{Name: "/fednet", Text: text, User: user, Channel: "D1", ResponseURL: "https://example.invalid/respond/" + user})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// click presses the confirm (or cancel) button of the card with id as
// user and returns what the user was told.
func (b *bench) click(t *testing.T, user, id string, confirm bool) string {
	t.Helper()
	url := "https://example.invalid/click/" + user
	n := len(b.sl.Responses(url))
	if err := b.h.Click(t.Context(), slack.Click{ID: id, Approve: confirm, User: user, ResponseURL: url}); err != nil {
		t.Fatal(err)
	}
	rs := b.sl.Responses(url)
	if len(rs) != n+1 {
		t.Fatalf("the clicker was told %q, want one more reply", rs)
	}
	return rs[n]
}

// `version` tells anyone on the user list what runs where; `upgrade` is
// for admins alone, and shows the card only when there is a newer
// release to go to.
func TestCommand(t *testing.T) {
	b := newBench(t)
	if r := b.cmd(t, "U3", "version"); !strings.Contains(r.Text, "用户名单") {
		t.Fatalf("someone off the user list got %q", r.Text)
	}
	r := b.cmd(t, "U2", "version")
	for _, want := range []string{"hub：v0.1.0", "最新 release：v0.2.0", "datamachine：v0.1.0 在线", "workstation：v0.1.0 在线"} {
		if !strings.Contains(r.Text, want) {
			t.Errorf("version = %q, want %q in it", r.Text, want)
		}
	}
	b.mu.Lock()
	b.online["workstation"] = false
	b.mu.Unlock()
	if r := b.cmd(t, "U2", "version"); !strings.Contains(r.Text, "workstation：v0.1.0 离线") {
		t.Fatalf("version = %q, want workstation offline", r.Text)
	}
	if r := b.cmd(t, "U2", "help"); !strings.Contains(r.Text, "用法") {
		t.Fatalf("an unknown command got %q", r.Text)
	}
	if r := b.cmd(t, "U2", "upgrade"); r.Card != nil || !strings.Contains(r.Text, "管理员") {
		t.Fatalf("someone who is not an admin got %+v", r)
	}
	r = b.cmd(t, "U1", "upgrade")
	if r.Card == nil || r.Card.From != "v0.1.0" || r.Card.To != "v0.2.0" || len(r.Card.Clients) != 2 {
		t.Fatalf("the admin got %+v, want the card from v0.1.0 to v0.2.0 with both clients", r)
	}
	b.h.mu.Lock()
	shownTo := b.h.cards[r.Card.ID].user
	b.h.mu.Unlock()
	if shownTo != "U1" {
		t.Fatal("the card is not recorded for the admin")
	}

	b.f.publish("v0.1.0", nil)
	if r := b.cmd(t, "U1", "upgrade"); r.Card != nil || !strings.Contains(r.Text, "已经是最新") {
		t.Fatalf("with no newer release the admin got %+v", r)
	}
	b.f.srv.Close()
	if r := b.cmd(t, "U1", "upgrade"); r.Card != nil || !strings.Contains(r.Text, "查不到") {
		t.Fatalf("with the releases unreachable the admin got %+v", r)
	}
	if r := b.cmd(t, "U1", "version"); !strings.Contains(r.Text, "最新 release：查不到") || !strings.Contains(r.Text, "hub：v0.1.0") {
		t.Fatalf("with the releases unreachable version = %q", r.Text)
	}
	b.h.Version = "dev"
	if r := b.cmd(t, "U1", "upgrade"); r.Card != nil || !strings.Contains(r.Text, "不是发布版") {
		t.Fatalf("a dev hub got %+v", r)
	}
	b.h.Version, b.h.Request = "v0.1.0", ""
	if r := b.cmd(t, "U1", "upgrade"); r.Card != nil || !strings.Contains(r.Text, "upgrade-request") {
		t.Fatalf("a hub without a request path got %+v", r)
	}
}

// Nothing happens until the admin the card was shown to presses confirm:
// another user's press, a cancel, a bot's press and a press on a card
// already used start nothing and are answered through the response URL.
func TestClickStartsTheUpgrade(t *testing.T) {
	b := newBench(t)
	b.run(t)
	card := b.cmd(t, "U1", "upgrade").Card
	if s := b.click(t, "U2", card.ID, true); !strings.Contains(s, "只有发") {
		t.Fatalf("another user was told %q", s)
	}
	if err := b.h.Click(t.Context(), slack.Click{ID: card.ID, Approve: true, User: "U1", Bot: true, ResponseURL: "https://example.invalid/bot"}); err != nil {
		t.Fatal(err)
	}
	if rs := b.sl.Responses("https://example.invalid/bot"); len(rs) != 1 || !strings.Contains(rs[0], "bot") {
		t.Fatalf("the bot was told %q", rs)
	}
	if s := b.click(t, "U1", "nonsense", true); !strings.Contains(s, "失效") {
		t.Fatalf("a press on no card was told %q", s)
	}
	if s := b.click(t, "U1", card.ID, false); s != "已取消" {
		t.Fatalf("cancel was told %q", s)
	}
	if s := b.click(t, "U1", card.ID, true); !strings.Contains(s, "失效") {
		t.Fatalf("confirm after cancel was told %q", s)
	}
	b.quiet(t)
	if n := len(b.told("workstation")) + len(b.told("datamachine")); n != 0 {
		t.Fatalf("%d notices sent before any confirm", n)
	}

	card = b.cmd(t, "U1", "upgrade").Card
	if s := b.click(t, "U1", card.ID, true); !strings.Contains(s, "开始从 v0.1.0 升到 v0.2.0") {
		t.Fatalf("confirm was told %q", s)
	}
	if s := b.next(t); !strings.Contains(s, "开始升级到 v0.2.0") || !strings.Contains(s, "<@U1>") {
		t.Fatalf("the hub said %q", s)
	}
	if s := b.click(t, "U1", card.ID, true); !strings.Contains(s, "失效") {
		t.Fatalf("a second confirm was told %q", s)
	}
	if r := b.cmd(t, "U1", "upgrade"); r.Card != nil || !strings.Contains(r.Text, "正在升级") {
		t.Fatalf("upgrade during an upgrade got %+v", r)
	}
	if r := b.cmd(t, "U2", "version"); !strings.Contains(r.Text, "正在升级到 v0.2.0") {
		t.Fatalf("version during an upgrade = %q", r.Text)
	}
}

// The order: every online client is told first, the hub's own request is
// written only once each has reported the release, and the hub says so
// at each step; a client already on the release is not told; an offline
// client is not waited for.
func TestRolloutClientsThenHub(t *testing.T) {
	b := newBench(t)
	ctx := t.Context()
	if err := b.h.Store.Register(ctx, "idle", []byte("hash")); err != nil {
		t.Fatal(err)
	}
	if err := b.h.Store.Register(ctx, "done", []byte("hash")); err != nil {
		t.Fatal(err)
	}
	if err := b.h.Store.SetVersion(ctx, "done", "v0.2.0"); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	b.online["done"] = true
	b.mu.Unlock()
	b.run(t)
	b.click(t, "U1", b.cmd(t, "U1", "upgrade").Card.ID, true)
	s := b.next(t)
	for _, want := range []string{"datamachine、workstation", "离线不等 idle", "已是 v0.2.0 的 done"} {
		if !strings.Contains(s, want) {
			t.Errorf("the hub said %q, want %q in it", s, want)
		}
	}
	for _, c := range []string{"datamachine", "workstation"} {
		if got := b.told(c); len(got) != 1 || got[0] != "v0.2.0" {
			t.Fatalf("%s was told %q, want v0.2.0 once", c, got)
		}
	}
	if n := len(b.told("idle")) + len(b.told("done")); n != 0 {
		t.Fatalf("an offline or upgraded client was told %d times", n)
	}
	// One client reports the release: the hub keeps waiting for the other.
	if err := b.h.Store.SetVersion(ctx, "datamachine", "v0.2.0"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if _, ok := b.request(t); ok {
		t.Fatal("the hub requested its own upgrade before every client reported the release")
	}
	if err := b.h.Store.SetVersion(ctx, "workstation", "v0.2.0"); err != nil {
		t.Fatal(err)
	}
	if s := b.next(t); !strings.Contains(s, "client 都已是 v0.2.0，hub 开始") {
		t.Fatalf("the hub said %q", s)
	}
	if v, ok := b.request(t); !ok || v != "v0.2.0" {
		t.Fatalf("the hub's request is %q, %v; want v0.2.0", v, ok)
	}
	// The hub does not hand off within the wait; the upgrader's result,
	// left next to the request, goes into the summary and is consumed.
	if err := WriteResult(b.h.Request, "failed v0.2.0: rolled back to v0.1.0"); err != nil {
		t.Fatal(err)
	}
	if s := b.next(t); !strings.Contains(s, "没有换成新进程") || !strings.Contains(s, "升级器说：failed v0.2.0: rolled back to v0.1.0") {
		t.Fatalf("the hub said %q", s)
	}
	if got := ReadResult(b.h.Request); got != "" {
		t.Fatalf("the result %q is still there after the summary", got)
	}
	b.quiet(t)
	b.h.mu.Lock()
	target := b.h.target
	b.h.mu.Unlock()
	if target != "" {
		t.Fatal("the upgrade is still in progress after its end")
	}

	// Once more, on a hub that does hand off: the summary says so, and
	// Run returns.
	b = newBench(t)
	handedOff := make(chan struct{})
	b.h.HandedOff = handedOff
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.h.Run(ctx)
	}()
	b.click(t, "U1", b.cmd(t, "U1", "upgrade").Card.ID, true)
	b.next(t)
	for _, c := range []string{"datamachine", "workstation"} {
		if err := b.h.Store.SetVersion(ctx, c, "v0.2.0"); err != nil {
			t.Fatal(err)
		}
	}
	b.next(t)
	close(handedOff)
	if s := b.next(t); !strings.Contains(s, "升级到 v0.2.0 完成") {
		t.Fatalf("the hub said %q", s)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the handoff")
	}
}

// A client that does not report the release within the wait, or that
// could not be told, keeps the hub where it is: no request is written,
// and the summary names the client.
func TestRolloutStopsWhenAClientFails(t *testing.T) {
	b := newBench(t)
	b.run(t)
	b.click(t, "U1", b.cmd(t, "U1", "upgrade").Card.ID, true)
	b.next(t)
	if err := b.h.Store.SetVersion(t.Context(), "datamachine", "v0.2.0"); err != nil {
		t.Fatal(err)
	}
	s := b.next(t)
	if !strings.Contains(s, "没完成") || !strings.Contains(s, "workstation：超时") || strings.Contains(s, "datamachine：") || !strings.Contains(s, "hub 不升") {
		t.Fatalf("the hub said %q", s)
	}
	if _, ok := b.request(t); ok {
		t.Fatal("the hub requested its own upgrade although a client timed out")
	}

	// The notice to one client cannot be queued: the hub does not wait
	// for the wait to run out.
	send := b.h.Send
	b.h.Send = func(ctx context.Context, client string, p []byte) error {
		if client == "workstation" {
			return errors.New("outbox is read-only")
		}
		return send(ctx, client, p)
	}
	if err := b.h.Store.SetVersion(t.Context(), "datamachine", "v0.2.0"); err != nil {
		t.Fatal(err)
	}
	b.click(t, "U1", b.cmd(t, "U1", "upgrade").Card.ID, true)
	b.next(t)
	if s := b.next(t); !strings.Contains(s, "workstation：outbox is read-only") {
		t.Fatalf("the hub said %q", s)
	}
	if _, ok := b.request(t); ok {
		t.Fatal("the hub requested its own upgrade although a client could not be told")
	}
}

// A client that connects with an older release than the hub's is told to
// upgrade, each time it connects; one with the hub's release, or a newer
// one, is not.
func TestConnectedTellsOldClients(t *testing.T) {
	b := newBench(t)
	b.h.Version = "v0.2.0"
	b.h.Connected("workstation", "v0.1.0")
	b.h.Connected("workstation", "v0.1.0")
	b.h.Connected("datamachine", "v0.2.0")
	b.h.Connected("idle", "v0.3.0")
	b.h.Connected("odd", "dev")
	if got := b.told("workstation"); len(got) != 2 || got[0] != "v0.2.0" || got[1] != "v0.2.0" {
		t.Fatalf("workstation was told %q, want v0.2.0 at each connection", got)
	}
	for _, c := range []string{"datamachine", "idle", "odd"} {
		if got := b.told(c); len(got) != 0 {
			t.Fatalf("%s was told %q, want nothing", c, got)
		}
	}
	// A hub that is not a release tells no one.
	b.h.Version = "dev"
	b.h.Connected("datamachine", "v0.1.0")
	if got := b.told("datamachine"); len(got) != 0 {
		t.Fatalf("a dev hub told datamachine %q", got)
	}
}

// With Auto, Run looks for a new release every Interval and upgrades to
// it; without Auto it never looks.
func TestAutoCheck(t *testing.T) {
	b := newBench(t)
	b.h.Auto = false
	b.run(t)
	time.Sleep(5 * b.h.Interval)
	if n := b.f.fetches.Load(); n != 0 {
		t.Fatalf("with Auto off the releases were fetched %d times", n)
	}

	b = newBench(t)
	b.h.Auto = true
	b.run(t)
	if s := b.next(t); !strings.Contains(s, "开始升级到 v0.2.0 (定时检查 发起)") {
		t.Fatalf("the hub said %q", s)
	}
	if n := b.f.fetches.Load(); n == 0 {
		t.Fatal("with Auto on the releases were never fetched")
	}
	for _, c := range []string{"datamachine", "workstation"} {
		if got := b.told(c); len(got) != 1 || got[0] != "v0.2.0" {
			t.Fatalf("%s was told %q, want v0.2.0 once", c, got)
		}
	}
	// Nothing to upgrade to: Run keeps looking and starts nothing.
	b = newBench(t)
	b.h.Auto = true
	b.f.publish("v0.1.0", nil)
	b.run(t)
	time.Sleep(5 * b.h.Interval)
	if n := b.f.fetches.Load(); n == 0 {
		t.Fatal("the releases were never fetched")
	}
	b.quiet(t)
}
