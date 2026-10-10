package upgrade

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Luolc/fednet/internal/link"
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
		Ops:  map[string][]string{OpVersion: {"workstation"}, OpUpgrade: {"workstation"}},
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
	// The answer goes out after Click returns.
	var rs []string
	for i := 0; i < 500; i++ {
		if rs = b.sl.Responses(url); len(rs) > n {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
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
	var rs []string
	for i := 0; i < 500 && len(rs) == 0; i++ {
		rs = b.sl.Responses("https://example.invalid/bot")
		time.Sleep(10 * time.Millisecond)
	}
	if len(rs) != 1 || !strings.Contains(rs[0], "bot") {
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

// A client dialing with an older release than the hub's is told the
// hub's; one with the hub's release, a newer one, or no release is not.
func TestUpgradeTo(t *testing.T) {
	b := newBench(t)
	b.h.Version = "v0.2.0"
	for v, want := range map[string]string{"v0.1.0": "v0.2.0", "v0.1.9": "v0.2.0", "v0.2.0": "", "v0.3.0": "", "dev": "", "": ""} {
		if got := b.h.UpgradeTo(v); got != want {
			t.Errorf("UpgradeTo(%q) = %q, want %q", v, got, want)
		}
	}
	b.h.Version = "dev"
	if got := b.h.UpgradeTo("v0.1.0"); got != "" {
		t.Fatalf("a dev hub tells clients to upgrade to %q", got)
	}
}

// slowRespond is a Fake whose Respond returns only when its context is
// done, reporting why on ended.
type slowRespond struct {
	*slack.Fake
	ended chan error
}

func (s slowRespond) Respond(ctx context.Context, _, _ string) error {
	<-ctx.Done()
	s.ended <- ctx.Err()
	return ctx.Err()
}

// Click returns, so that the press can be acked, without waiting for the
// answer to the clicker to go out; the answer still on its way out when
// Run stops is cancelled and waited for.
func TestClickDoesNotWaitForTheResponse(t *testing.T) {
	b := newBench(t)
	slow := slowRespond{b.sl, make(chan error, 1)}
	b.h.Slack = slow
	ctx, cancel := context.WithCancel(t.Context())
	ran := make(chan struct{})
	go func() {
		defer close(ran)
		b.h.Run(ctx)
	}()
	card := b.cmd(t, "U1", "upgrade").Card
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.h.Click(t.Context(), slack.Click{ID: card.ID, Approve: true, User: "U1", ResponseURL: "https://example.invalid/slow"})
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Click waited for the response")
	}
	if s := b.next(t); !strings.Contains(s, "开始升级到 v0.2.0") {
		t.Fatalf("the hub said %q", s)
	}
	select {
	case err := <-slow.ended:
		t.Fatalf("the response ended with %v before Run stopped", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
	select {
	case err := <-slow.ended:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("the response ended with %v, want it cancelled", err)
		}
	default:
		t.Fatal("Run returned while the response was still on its way out")
	}
}

// Cards nobody clicked are forgotten when the next card is shown, once
// past their TTL.
func TestExpiredCardsAreForgotten(t *testing.T) {
	b := newBench(t)
	now := time.Unix(1_760_000_000, 0)
	b.h.Now = func() time.Time { return now }
	first := b.cmd(t, "U1", "upgrade").Card
	now = now.Add(cardTTL + time.Second)
	b.cmd(t, "U1", "upgrade")
	now = now.Add(cardTTL + time.Second)
	third := b.cmd(t, "U1", "upgrade").Card
	b.h.mu.Lock()
	n := len(b.h.cards)
	_, keptThird := b.h.cards[third.ID]
	b.h.mu.Unlock()
	if n != 1 || !keptThird {
		t.Fatalf("%d cards kept, want only the latest", n)
	}
	if s := b.click(t, "U1", first.ID, true); !strings.Contains(s, "失效") {
		t.Fatalf("a press on a forgotten card was told %q", s)
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

// nextN returns the next n things the hub said, in any order: the record
// of an ops request and the start of the upgrade it begins race.
func (b *bench) nextN(t *testing.T, n int) string {
	t.Helper()
	var said []string
	for range n {
		said = append(said, b.next(t))
	}
	return strings.Join(said, "\n")
}

// A client listed for an op gets it; one that is not is refused; every
// request is said through Alert, refused or not.
func TestOps(t *testing.T) {
	b := newBench(t)
	b.run(t)
	text, err := b.h.Op(t.Context(), "workstation", OpVersion)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"hub：v0.1.0", "最新 release：v0.2.0", "datamachine：v0.1.0 在线", "workstation：v0.1.0 在线"} {
		if !strings.Contains(text, want) {
			t.Errorf("version = %q, want %q in it", text, want)
		}
	}
	if s := b.next(t); s != "ops：workstation 请求 version，已回答" {
		t.Fatalf("the hub said %q", s)
	}
	for _, op := range []string{OpVersion, OpUpgrade} {
		if _, err := b.h.Op(t.Context(), "datamachine", op); !errors.Is(err, link.ErrDenied) {
			t.Fatalf("%s from datamachine = %v, want ErrDenied", op, err)
		}
		if s := b.next(t); s != "ops：datamachine 请求 "+op+"，被拒：hub 配置的 ops."+op+" 里没有它" {
			t.Fatalf("the hub said %q", s)
		}
	}
	if _, err := b.h.Op(t.Context(), "workstation", "<!channel> reboot"); !errors.Is(err, link.ErrBadRequest) {
		t.Fatalf("an unknown op = %v, want ErrBadRequest", err)
	}
	if s := b.next(t); s != "ops：workstation 请求 未知操作，被拒：没有这个操作" {
		t.Fatalf("the hub said %q", s)
	}
	b.quiet(t)
	if n := len(b.told("workstation")) + len(b.told("datamachine")); n != 0 {
		t.Fatalf("%d notices sent before any upgrade began", n)
	}

	// upgrade starts the same rollout a confirmed card does, and answers
	// without waiting for it.
	text, err = b.h.Op(t.Context(), "workstation", OpUpgrade)
	if err != nil || text != "开始从 v0.1.0 升到 v0.2.0，过程和结果发到报警 channel" {
		t.Fatalf("upgrade = %q, %v", text, err)
	}
	said := b.nextN(t, 2)
	for _, want := range []string{"ops：workstation 请求 upgrade，开始从 v0.1.0 升到 v0.2.0", "开始升级到 v0.2.0 (workstation 经 ops 发起)：先升 client datamachine、workstation"} {
		if !strings.Contains(said, want) {
			t.Errorf("the hub said %q, want %q in it", said, want)
		}
	}
	for _, c := range []string{"datamachine", "workstation"} {
		if got := b.told(c); len(got) != 1 || got[0] != "v0.2.0" {
			t.Fatalf("%s was told %q, want v0.2.0 once", c, got)
		}
	}
	if text, err := b.h.Op(t.Context(), "workstation", OpUpgrade); err != nil || text != "正在升级到 v0.2.0" {
		t.Fatalf("upgrade during an upgrade = %q, %v", text, err)
	}
	if s := b.next(t); s != "ops：workstation 请求 upgrade，正在升级到 v0.2.0" {
		t.Fatalf("the hub said %q", s)
	}
}

// On the latest release already, upgrade says so and starts nothing; a
// hub that cannot upgrade itself refuses, and says so.
func TestOpsUpgradeNothingToDo(t *testing.T) {
	b := newBench(t)
	b.run(t)
	b.f.publish("v0.1.0", nil)
	text, err := b.h.Op(t.Context(), "workstation", OpUpgrade)
	if err != nil || text != "hub 已经是最新的 release：v0.1.0 (最新 v0.1.0)" {
		t.Fatalf("upgrade on the latest release = %q, %v", text, err)
	}
	if s := b.next(t); s != "ops：workstation 请求 upgrade，hub 已经是最新的 release：v0.1.0 (最新 v0.1.0)" {
		t.Fatalf("the hub said %q", s)
	}
	b.quiet(t)
	if n := len(b.told("workstation")) + len(b.told("datamachine")); n != 0 {
		t.Fatalf("%d notices sent with nothing to upgrade to", n)
	}
	b.h.Request = ""
	if _, err := b.h.Op(t.Context(), "workstation", OpUpgrade); !errors.Is(err, link.ErrDenied) || !strings.Contains(err.Error(), "upgrade-request") {
		t.Fatalf("upgrade on a hub without a request path = %v, want ErrDenied", err)
	}
	if s := b.next(t); !strings.HasPrefix(s, "ops：workstation 请求 upgrade，被拒：hub 没有配升级请求的路径") {
		t.Fatalf("the hub said %q", s)
	}
}

// A slow alerts webhook does not hold the answer: upgrade answers that it
// has started at once, starts once, and the record follows when the
// webhook takes it.
func TestOpsAnswerDoesNotWaitForTheRecord(t *testing.T) {
	b := newBench(t)
	b.run(t)
	release := make(chan struct{})
	alert := b.h.Alert
	b.h.Alert = func(ctx context.Context, text string) error {
		if strings.HasPrefix(text, "ops：") {
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return alert(ctx, text)
	}
	answered := make(chan string, 1)
	go func() {
		text, _ := b.h.Op(t.Context(), "workstation", OpUpgrade)
		answered <- text
	}()
	select {
	case text := <-answered:
		if !strings.HasPrefix(text, "开始从 v0.1.0 升到 v0.2.0") {
			t.Fatalf("upgrade = %q", text)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("upgrade waited for the webhook")
	}
	if s := b.next(t); !strings.HasPrefix(s, "开始升级到 v0.2.0 (workstation 经 ops 发起)") {
		t.Fatalf("the hub said %q before the record", s)
	}
	close(release)
	if s := b.next(t); s != "ops：workstation 请求 upgrade，开始从 v0.1.0 升到 v0.2.0" {
		t.Fatalf("the hub said %q", s)
	}
	for _, c := range []string{"datamachine", "workstation"} {
		if got := b.told(c); len(got) != 1 {
			t.Fatalf("%s was told %q, want one notice", c, got)
		}
	}
}

// lockedBuffer is a bytes.Buffer the log handler and the test share.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// With the webhook stuck, ops requests are answered and logged until the
// records waiting for it fill every place; the next request is refused
// as busy and not carried out, and is logged too. Once the webhook takes
// them, every record of a request answered reaches it, and the hub
// carries out requests again.
func TestOpsBusyWhenRecordsBackUp(t *testing.T) {
	logs := &lockedBuffer{}
	defer slog.SetDefault(slog.Default())
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	b := newBench(t)
	b.run(t)
	release := make(chan struct{})
	alert := b.h.Alert
	b.h.Alert = func(ctx context.Context, text string) error {
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return alert(ctx, text)
	}
	for i := range maxResponses {
		client := []string{"workstation", "datamachine"}[i%2]
		_, err := b.h.Op(t.Context(), client, OpVersion)
		if (client == "workstation") != (err == nil) {
			t.Fatalf("request %d, version from %s = %v", i, client, err)
		}
	}
	if _, err := b.h.Op(t.Context(), "workstation", OpUpgrade); !errors.Is(err, link.ErrBusy) {
		t.Fatalf("upgrade with every place taken = %v, want ErrBusy", err)
	}
	if n := len(b.told("workstation")) + len(b.told("datamachine")); n != 0 {
		t.Fatalf("a busy hub sent %d upgrade notices", n)
	}
	log := logs.String()
	if n := strings.Count(log, "ops：workstation 请求 version，已回答"); n != maxResponses/2 {
		t.Errorf("%d answered requests from workstation logged, want %d", n, maxResponses/2)
	}
	if n := strings.Count(log, "ops：datamachine 请求 version，被拒"); n != maxResponses/2 {
		t.Errorf("%d refused requests from datamachine logged, want %d", n, maxResponses/2)
	}
	if !strings.Contains(log, "ops：workstation 请求 upgrade，没有执行") {
		t.Errorf("the busy refusal is not logged:\n%s", log)
	}

	close(release)
	var said []string
	for range maxResponses {
		said = append(said, b.next(t))
	}
	for i, s := range said {
		if !strings.HasPrefix(s, "ops：") || strings.Contains(s, "upgrade") {
			t.Errorf("record %d is %q, want one of the version requests", i, s)
		}
	}
	b.quiet(t)
	if text, err := b.h.Op(t.Context(), "workstation", OpUpgrade); err != nil || !strings.HasPrefix(text, "开始从 v0.1.0 升到 v0.2.0") {
		t.Fatalf("upgrade once the records are out = %q, %v", text, err)
	}
}

// logAtLatest is a transport that, on each request for the latest
// release, notes what the log holds by then.
type logAtLatest struct {
	logs *lockedBuffer
	mu   sync.Mutex
	seen []string
}

func (l *logAtLatest) RoundTrip(r *http.Request) (*http.Response, error) {
	if strings.HasSuffix(r.URL.Path, "/latest") {
		l.mu.Lock()
		l.seen = append(l.seen, l.logs.String())
		l.mu.Unlock()
	}
	return http.DefaultTransport.RoundTrip(r)
}

// An ops request is logged before the hub does anything for it: by the
// time upgrade asks for the latest release, which comes before the
// rollout starts, the request is in the log.
func TestOpsLoggedBeforeCarriedOut(t *testing.T) {
	logs := &lockedBuffer{}
	defer slog.SetDefault(slog.Default())
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	b := newBench(t)
	b.run(t)
	tr := &logAtLatest{logs: logs}
	b.h.Releases.HTTP = &http.Client{Transport: tr}
	if text, err := b.h.Op(t.Context(), "workstation", OpUpgrade); err != nil || !strings.HasPrefix(text, "开始从 v0.1.0 升到 v0.2.0") {
		t.Fatalf("upgrade = %q, %v", text, err)
	}
	tr.mu.Lock()
	seen := tr.seen
	tr.mu.Unlock()
	if len(seen) != 1 || !strings.Contains(seen[0], "ops：workstation 请求 upgrade，收到") || strings.Contains(seen[0], "开始从") {
		t.Fatalf("when the latest release was asked for, the log held %q; want the request, and not yet its outcome", seen)
	}
	if !strings.Contains(logs.String(), "ops：workstation 请求 upgrade，开始从 v0.1.0 升到 v0.2.0") {
		t.Fatalf("the outcome is not logged:\n%s", logs.String())
	}
}
