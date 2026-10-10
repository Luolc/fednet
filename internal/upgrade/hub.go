package upgrade

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Luolc/fednet/internal/link"
	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/release"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
)

// Defaults for the zero fields of Hub.
const (
	// DefaultInterval is how often the hub looks for a new release.
	DefaultInterval = time.Hour
	// DefaultWait is how long the clients, and then the hub, get to come
	// up on the new release.
	DefaultWait = 10 * time.Minute
	// DefaultPoll is how often the wait looks at the clients' versions.
	DefaultPoll = 5 * time.Second
	// DefaultFetch bounds the fetch of the latest release a command makes:
	// Slack wants a slash command answered within three seconds.
	DefaultFetch = 2500 * time.Millisecond
	// cardTTL is how long an upgrade card waits for its click.
	cardTTL = 10 * time.Minute
	// respondTimeout bounds the answer to a click, which goes out after
	// the click is acked.
	respondTimeout = 10 * time.Second
)

// The operations a client may ask the hub for through `fednet client ops`.
const (
	// OpVersion answers as `/fednet version` does.
	OpVersion = "version"
	// OpUpgrade starts the upgrade `/fednet upgrade` would.
	OpUpgrade = "upgrade"
)

// Hub decides the upgrades on the hub. Its Command and Click answer the
// `/fednet` slash command and the clicks on the card `/fednet upgrade`
// shows, and Op the same two from a client; Run looks for a new release
// every Interval, when Auto is set, and carries out each upgrade: it
// tells every online client, waits for them all to report the new
// release, and only then requests the hub's own upgrade; a client that
// fails or is late keeps the hub where it is.
// Connected tells a client that connects with an older release than the
// hub's to upgrade. Its exported fields are set before use and not
// changed after.
type Hub struct {
	Store *store.Hub
	// Version is the hub's version.
	Version string
	// Releases is where releases are published.
	Releases Releases
	// Send queues payload for client; it is link.Hub.Send.
	Send func(ctx context.Context, client string, payload []byte) error
	// Online reports whether a client is online; it is link.Hub.Online.
	Online func(client string) bool
	// Alert posts the progress of an upgrade and its summary where
	// everyone can see them; nil means they are only logged.
	Alert func(ctx context.Context, text string) error
	// Slack answers the clicks through their response URLs; nil means
	// the clicker is not answered.
	Slack slack.API
	// Request is the path the hub's own upgrade request is written to;
	// empty means the hub cannot upgrade itself, and `/fednet upgrade` is
	// refused.
	Request string
	// HandedOff is closed once this process has handed off to a
	// successor; it is handoff.Process.Exit. Run returns then, after the
	// summary of an upgrade waiting for just that.
	HandedOff <-chan struct{}
	// Users is the user list; only people on it get an answer. Admins are
	// the Slack user ids of those who may upgrade, each on Users.
	Users  map[string]string
	Admins []string
	// Ops maps each operation, OpVersion or OpUpgrade, to the clients
	// that may ask for it; any other client is refused.
	Ops map[string][]string
	// Auto makes Run look for a new release every Interval and upgrade
	// to it.
	Auto bool
	// Interval, Wait, Poll and Fetch are the timings; zero means the
	// package's default of each.
	Interval, Wait, Poll, Fetch time.Duration
	// Now returns the current time; nil means time.Now.
	Now func() time.Time

	mu sync.Mutex
	// cards holds the confirmations waiting for a click, by id.
	cards map[string]card
	// target is the release an upgrade in progress goes to; empty when
	// none is.
	target string
	// start carries the upgrades Click begins to Run.
	start     chan rollout
	startOnce sync.Once
	// responses are the answers to clicks and the records of ops on
	// their way out: Run cancels and waits for them when it returns; at
	// most maxResponses at once.
	responses     sync.WaitGroup
	responseCtx   context.Context
	stopResponses context.CancelFunc
	responseOnce  sync.Once
	inFlight      chan struct{}
}

// maxResponses is how many answers to clicks and records of ops may be
// on their way out at once; one more is dropped with a log line.
const maxResponses = 16

// responseContext returns the context the answers to clicks and the
// records of ops run under,
// made on first use.
func (h *Hub) responseContext() context.Context {
	h.responseOnce.Do(func() {
		h.responseCtx, h.stopResponses = context.WithCancel(context.Background())
		h.inFlight = make(chan struct{}, maxResponses)
	})
	return h.responseCtx
}

// card is a confirmation `/fednet upgrade` showed and nobody has clicked.
type card struct {
	user, from, to string
	at             time.Time
}

// rollout is one upgrade to carry out: to is the release, by who asked.
type rollout struct{ to, by string }

func or(d, def time.Duration) time.Duration {
	if d == 0 {
		return def
	}
	return d
}

func (h *Hub) now() time.Time {
	if h.Now != nil {
		return h.Now()
	}
	return time.Now()
}

func (h *Hub) startCh() chan rollout {
	h.startOnce.Do(func() { h.start = make(chan rollout, 1) })
	return h.start
}

// Command answers c: `version` says what runs where, `upgrade` shows the
// card that starts an upgrade. Only people on the user list are answered
// in kind, and only admins can upgrade.
func (h *Hub) Command(ctx context.Context, c slack.Command) (slack.CommandReply, error) {
	if _, ok := h.Users[c.User]; !ok {
		return slack.CommandReply{Text: "你不在 fednet 的用户名单上"}, nil
	}
	switch strings.TrimSpace(c.Text) {
	case "version":
		return h.version(ctx)
	case "upgrade":
		return h.upgrade(ctx, c)
	}
	return slack.CommandReply{Text: "用法：`" + c.Name + " version` 看版本，`" + c.Name + " upgrade` 升到最新的 release"}, nil
}

// version answers `/fednet version`: the hub's version, the latest
// release, and each client's version and whether it is online.
func (h *Hub) version(ctx context.Context) (slack.CommandReply, error) {
	text, err := h.versionText(ctx)
	return slack.CommandReply{Text: text}, err
}

func (h *Hub) versionText(ctx context.Context) (string, error) {
	lines := []string{"hub：" + h.Version}
	if latest, err := h.latest(ctx); err != nil {
		lines = append(lines, "最新 release：查不到 ("+err.Error()+")")
	} else {
		lines = append(lines, "最新 release："+latest)
	}
	h.mu.Lock()
	target := h.target
	h.mu.Unlock()
	if target != "" {
		lines = append(lines, "正在升级到 "+target)
	}
	clients, err := h.clients(ctx)
	if err != nil {
		return "", err
	}
	for _, c := range clients {
		lines = append(lines, c.line())
	}
	return strings.Join(lines, "\n"), nil
}

// latest fetches the latest release, within Fetch.
func (h *Hub) latest(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, or(h.Fetch, DefaultFetch))
	defer cancel()
	return h.Releases.LatestVersion(ctx)
}

// client is a client as an upgrade sees it.
type client struct {
	id, version string
	online      bool
}

func (c client) line() string {
	v := c.version
	if v == "" {
		v = "没连过"
	}
	if c.online {
		return c.id + "：" + v + " 在线"
	}
	return c.id + "：" + v + " 离线"
}

// clients returns every registered client with its last reported version
// and whether it is online, ordered by id.
func (h *Hub) clients(ctx context.Context) ([]client, error) {
	ids, err := h.Store.Clients(ctx)
	if err != nil {
		return nil, err
	}
	var cs []client
	for _, id := range ids {
		reg, err := h.Store.Registration(ctx, id)
		if err != nil {
			return nil, err
		}
		cs = append(cs, client{id, reg.Version, h.Online(id)})
	}
	return cs, nil
}

// upgrade answers `/fednet upgrade`: the card that asks to confirm an
// upgrade to the latest release, or why there is none to confirm.
func (h *Hub) upgrade(ctx context.Context, c slack.Command) (slack.CommandReply, error) {
	text := func(s string) (slack.CommandReply, error) { return slack.CommandReply{Text: s}, nil }
	if !slices.Contains(h.Admins, c.User) {
		return text("只有管理员能升级")
	}
	latest, why, err := h.next(ctx)
	if err != nil {
		return text(err.Error())
	}
	if why != "" {
		return text(why)
	}
	clients, err := h.clients(ctx)
	if err != nil {
		return slack.CommandReply{}, err
	}
	shown := &slack.UpgradeCard{ID: rand.Text(), From: h.Version, To: latest}
	for _, c := range clients {
		shown.Clients = append(shown.Clients, c.line())
	}
	now := h.now()
	h.mu.Lock()
	if h.cards == nil {
		h.cards = make(map[string]card)
	}
	// Cards nobody clicked are forgotten here, so they do not pile up.
	for id, old := range h.cards {
		if now.Sub(old.at) > cardTTL {
			delete(h.cards, id)
		}
	}
	h.cards[shown.ID] = card{user: c.User, from: h.Version, to: latest, at: now}
	h.mu.Unlock()
	return slack.CommandReply{Card: shown}, nil
}

// next returns the release a command may start an upgrade to: the
// latest, when it is newer than the hub and no upgrade is in progress.
// Otherwise it returns why there is none: when that is nothing to fix,
// the hub on the latest release or on its way there, as why; when the hub
// cannot upgrade from a command, or cannot find the latest release, as an
// error, a refusal for the former. Either is said as it is to whoever
// asked.
func (h *Hub) next(ctx context.Context) (latest, why string, err error) {
	switch {
	case !release.IsRelease(h.Version):
		return "", "", link.Refuse(link.ErrDenied, "hub 跑的是 %s，不是发布版，不能从这里升级", h.Version)
	case h.Request == "":
		return "", "", link.Refuse(link.ErrDenied, "hub 没有配升级请求的路径 (-upgrade-request)，升不了自己")
	}
	latest, err = h.latest(ctx)
	if err != nil {
		return "", "", fmt.Errorf("查不到最新的 release：%w", err)
	}
	if !release.Newer(latest, h.Version) {
		return "", "hub 已经是最新的 release：" + h.Version + " (最新 " + latest + ")", nil
	}
	h.mu.Lock()
	target := h.target
	h.mu.Unlock()
	if target != "" {
		return "", "正在升级到 " + target, nil
	}
	return latest, "", nil
}

// Op carries out op for client, the id the link authenticated the
// request with: OpVersion answers what `/fednet version` does, OpUpgrade
// starts the upgrade to the latest release that `/fednet upgrade` would,
// without a card, and answers once it has started; how it goes is said
// through Alert, as for any upgrade. An op Ops does not list client for
// is refused, and so is any other op. Every request is said through
// Alert with what came of it, after Op returns.
func (h *Hub) Op(ctx context.Context, client, op string) (string, error) {
	text, outcome, err := h.op(ctx, client, op)
	if op != OpVersion && op != OpUpgrade {
		// The op is the client's to write; only known ones are repeated.
		op = "未知操作"
	}
	// The record goes out after the answer, so that a slow webhook does
	// not hold the answer past the client's timeout.
	record := "ops：" + client + " 请求 " + op + "，" + outcome
	h.later("record of an ops request", record, func(ctx context.Context) { h.say(ctx, record) })
	return text, err
}

// op returns what to answer client, and what came of op for the record.
func (h *Hub) op(ctx context.Context, client, op string) (text, outcome string, err error) {
	if op != OpVersion && op != OpUpgrade {
		return "", "被拒：没有这个操作", link.Refuse(link.ErrBadRequest, "unknown op: want %s or %s", OpVersion, OpUpgrade)
	}
	if !slices.Contains(h.Ops[op], client) {
		return "", "被拒：hub 配置的 ops." + op + " 里没有它",
			link.Refuse(link.ErrDenied, "client %s may not ask for %s: the hub's config does not list it under ops.%s", client, op, op)
	}
	if op == OpVersion {
		if text, err = h.versionText(ctx); err != nil {
			return "", "失败：" + err.Error(), err
		}
		return text, "已回答", nil
	}
	latest, why, err := h.next(ctx)
	switch {
	case errors.Is(err, link.ErrDenied):
		return "", "被拒：" + err.Error(), err
	case err != nil:
		return "", "失败：" + err.Error(), err
	case why != "":
		return why, why, nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.target != "" {
		return "正在升级到 " + h.target, "正在升级到 " + h.target, nil
	}
	h.target = latest
	h.startCh() <- rollout{to: latest, by: client + " 经 ops"}
	started := "开始从 " + h.Version + " 升到 " + latest
	return started + "，过程和结果发到报警 channel", started, nil
}

// Click applies a press on an upgrade card's button: the confirm button,
// pressed by the admin the card was shown to while the card is still
// good, starts the upgrade; the cancel button, or any press that does not
// count, only tells the clicker. The clicker is told after Click returns,
// so the press is acked in time whatever Slack's response URL does; the
// answer is cancelled and waited for when Run returns.
func (h *Hub) Click(_ context.Context, c slack.Click) error {
	text := h.click(c)
	if h.Slack == nil || c.ResponseURL == "" {
		return nil
	}
	h.later("answer to a click", "user "+c.User, func(ctx context.Context) { h.respond(ctx, c, text) })
	return nil
}

// later runs f after the caller returns, under the context Run cancels
// and waits for f under when it returns; when maxResponses are on their
// way out already, f is dropped with a log line naming what and about.
func (h *Hub) later(what, about string, f func(ctx context.Context)) {
	ctx := h.responseContext()
	select {
	case h.inFlight <- struct{}{}:
	default:
		slog.Warn("upgrade: too many answers on their way out, dropping one", "what", what, "about", about)
		return
	}
	h.responses.Add(1)
	go func() {
		defer h.responses.Done()
		defer func() { <-h.inFlight }()
		f(ctx)
	}()
}

// click returns what to tell the clicker.
func (h *Hub) click(c slack.Click) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	card, ok := h.cards[c.ID]
	switch {
	case c.Bot:
		return "bot 不能点这张卡"
	case !ok:
		return "这张卡已经用过或失效了，要升级再发一次 /fednet upgrade"
	case card.user != c.User:
		return "只有发 /fednet upgrade 的人能点这张卡"
	}
	delete(h.cards, c.ID)
	switch {
	case !c.Approve:
		return "已取消"
	case h.now().Sub(card.at) > cardTTL:
		return "这张卡过期了，要升级再发一次 /fednet upgrade"
	case h.target != "":
		return "正在升级到 " + h.target
	case !release.Newer(card.to, h.Version):
		return "hub 已经是 " + h.Version
	}
	h.target = card.to
	h.startCh() <- rollout{to: card.to, by: "<@" + c.User + ">"}
	return "开始从 " + card.from + " 升到 " + card.to + "，过程和结果发到报警 channel"
}

// respond tells the clicker text through the click's response URL,
// which replaces the card, within respondTimeout; a response that fails
// is logged.
func (h *Hub) respond(ctx context.Context, c slack.Click, text string) {
	ctx, cancel := context.WithTimeout(ctx, respondTimeout)
	defer cancel()
	if err := h.Slack.Respond(ctx, c.ResponseURL, text); err != nil {
		slog.Warn("upgrade: answering the click", "user", c.User, "err", err)
	}
}

// UpgradeTo answers, for a client dialing with version, the release it
// should upgrade to: the hub's when that is newer, "" otherwise. It is
// link.Hub.UpgradeTo, so a client behind the hub is told on every dial,
// one the hub refuses to serve included.
func (h *Hub) UpgradeTo(version string) string {
	if !release.Newer(h.Version, version) {
		return ""
	}
	return h.Version
}

// tell queues the notice to upgrade to version for client.
func (h *Hub) tell(ctx context.Context, client, version string) error {
	b, err := json.Marshal(payload.Message{Type: payload.Upgrade, Version: version})
	if err != nil {
		return err
	}
	return h.Send(ctx, client, b)
}

// Run carries out the upgrades Click starts and, when Auto is set, looks
// for a new release every Interval and upgrades to it, until ctx is done
// or the hub has handed off; then it cancels the answers to clicks still
// on their way out and waits for them. It must be called once.
func (h *Hub) Run(ctx context.Context) {
	h.responseContext()
	defer func() {
		h.stopResponses()
		h.responses.Wait()
	}()
	var tick <-chan time.Time
	if h.Auto && release.IsRelease(h.Version) {
		t := time.NewTicker(or(h.Interval, DefaultInterval))
		defer t.Stop()
		tick = t.C
	} else if h.Auto {
		slog.Info("upgrade: not looking for new releases, this hub is not a release", "version", h.Version)
	}
	for {
		select {
		case r := <-h.startCh():
			h.rollout(ctx, r)
		case <-tick:
			h.check(ctx)
		case <-ctx.Done():
			return
		case <-h.HandedOff:
			return
		}
	}
}

// check upgrades to the latest release when it is newer than the hub and
// no upgrade is in progress.
func (h *Hub) check(ctx context.Context) {
	latest, err := h.Releases.LatestVersion(ctx)
	if err != nil {
		slog.Warn("upgrade: looking for a new release", "err", err)
		return
	}
	if !release.Newer(latest, h.Version) {
		return
	}
	h.mu.Lock()
	if h.target != "" {
		h.mu.Unlock()
		return
	}
	h.target = latest
	h.mu.Unlock()
	h.rollout(ctx, rollout{to: latest, by: "定时检查"})
}

// rollout carries out r: tells every online client that is not on the
// release yet, waits Wait for each to report it, and, when all have,
// requests the hub's own upgrade and waits Wait for the handoff. Any
// client that fails, or is still not there at the end of the wait,
// keeps the hub where it is. Each step is said through Alert.
func (h *Hub) rollout(ctx context.Context, r rollout) {
	defer func() {
		h.mu.Lock()
		h.target = ""
		h.mu.Unlock()
	}()
	clients, err := h.clients(ctx)
	if err != nil {
		h.say(ctx, "升级到 "+r.to+" 没开始：读不到 client 名单："+err.Error())
		return
	}
	var todo, offline, there []string
	for _, c := range clients {
		switch {
		case c.version == r.to:
			there = append(there, c.id)
		case !c.online:
			offline = append(offline, c.id+" ("+c.line()+")")
		default:
			todo = append(todo, c.id)
		}
	}
	failed := make(map[string]string)
	for _, c := range todo {
		if err := h.tell(ctx, c, r.to); err != nil {
			failed[c] = err.Error()
		}
	}
	h.say(ctx, fmt.Sprintf("开始升级到 %s (%s 发起)：先升 client %s，离线不等 %s，已是 %s 的 %s；client 都升好了再升 hub (%s)",
		r.to, r.by, list(todo), list(offline), r.to, list(there), h.Version))
	left := h.await(ctx, todo, failed, r.to)
	if left == nil {
		// Stopped or handed off while waiting: nothing to report.
		return
	}
	for _, c := range left {
		failed[c] = "超时"
	}
	if len(failed) > 0 {
		var fs []string
		for _, c := range todo {
			if failed[c] != "" {
				fs = append(fs, c+"："+failed[c])
			}
		}
		h.say(ctx, fmt.Sprintf("升级到 %s 没完成：%s；hub 不升，还是 %s。处理好了再发一次 /fednet upgrade", r.to, strings.Join(fs, "，"), h.Version))
		return
	}
	if h.Request == "" {
		h.say(ctx, "client 都已是 "+r.to+"，但 hub 没有配升级请求的路径，hub 不升，还是 "+h.Version)
		return
	}
	if err := WriteRequest(h.Request, r.to); err != nil {
		h.say(ctx, "client 都已是 "+r.to+"，但 hub 的升级请求写不了，hub 不升，还是 "+h.Version+"："+err.Error())
		return
	}
	h.say(ctx, "client 都已是 "+r.to+"，hub 开始从 "+h.Version+" 升级")
	t := time.NewTimer(or(h.Wait, DefaultWait))
	defer t.Stop()
	select {
	case <-h.HandedOff:
		h.say(ctx, "hub 已换成新进程，升级到 "+r.to+" 完成")
	case <-t.C:
		// The upgrader leaves what became of the request next to it: a
		// failed handoff says it rolled back.
		result := ReadResult(h.Request)
		if result == "" {
			result = "升级器没有留下结果，原因在 hub 机器上它的日志里"
		}
		h.say(ctx, fmt.Sprintf("hub 在 %v 内没有换成新进程，还是 %s；升级器说：%s", or(h.Wait, DefaultWait), h.Version, result))
	case <-ctx.Done():
	}
}

// await waits until every client in todo that did not fail reports
// version, at most Wait, and returns the ones that have not by then. It
// returns nil when ctx is done or the hub has handed off first.
func (h *Hub) await(ctx context.Context, todo []string, failed map[string]string, version string) []string {
	deadline := time.NewTimer(or(h.Wait, DefaultWait))
	defer deadline.Stop()
	for {
		var left []string
		for _, c := range todo {
			if failed[c] != "" {
				continue
			}
			reg, err := h.Store.Registration(ctx, c)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				slog.Warn("upgrade: reading a client's version", "client", c, "err", err)
			}
			if reg.Version != version {
				left = append(left, c)
			}
		}
		if len(left) == 0 {
			return []string{}
		}
		poll := time.NewTimer(or(h.Poll, DefaultPoll))
		select {
		case <-poll.C:
		case <-deadline.C:
			poll.Stop()
			return left
		case <-ctx.Done():
			poll.Stop()
			return nil
		case <-h.HandedOff:
			poll.Stop()
			return nil
		}
	}
}

// say posts text through Alert, and logs it.
func (h *Hub) say(ctx context.Context, text string) {
	slog.Info("upgrade: " + text)
	if h.Alert == nil {
		return
	}
	if err := h.Alert(ctx, text); err != nil {
		slog.Warn("upgrade: posting the progress", "err", err)
	}
}

// list joins names for a message; none reads as such.
func list(names []string) string {
	if len(names) == 0 {
		return "(无)"
	}
	return strings.Join(names, "、")
}
