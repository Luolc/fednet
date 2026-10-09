package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Luolc/fednet/internal/auth"
	"github.com/Luolc/fednet/internal/hook"
	"github.com/Luolc/fednet/internal/inbound"
	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
)

// The credentials the tests hand the hub; none may show up in any output.
const (
	testAppToken = "test-app-token-6f1d0c"
	testBotToken = "test-bot-token-93ab7e"
	testHookPath = "/services/test-hook-5c2e81"
)

// slackRun is what the hub gave Slack: the tokens, and the receiver Socket
// Mode would feed.
type slackRun struct {
	appToken, botToken string
	r                  *inbound.Receiver
}

// fakeSlack makes the hub use api as its Slack API, and a stand-in for the
// Socket Mode connection that sends what the hub gave Slack to the returned
// channel, then returns fail at once or, when fail is nil, waits for its
// context. It writes the two token files in dir, padded with white space,
// and returns the flags that name them.
func fakeSlack(t *testing.T, dir string, api slack.API, fail error) ([]string, <-chan slackRun) {
	t.Helper()
	app, bot := filepath.Join(dir, "slack-app-token"), filepath.Join(dir, "slack-bot-token")
	for path, token := range map[string]string{app: testAppToken, bot: testBotToken} {
		if err := os.WriteFile(path, []byte("  "+token+"\n\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	runs := make(chan slackRun, 1)
	var botToken string
	newSlack = func(token string) slack.API {
		botToken = token
		return api
	}
	runInbound = func(ctx context.Context, appToken string, r *inbound.Receiver, _ time.Duration) error {
		runs <- slackRun{appToken, botToken, r}
		if fail != nil {
			return fail
		}
		<-ctx.Done()
		return ctx.Err()
	}
	t.Cleanup(func() {
		newSlack = func(token string) slack.API { return slack.New(token) }
		runInbound = inbound.Run
	})
	return []string{"-slack-app-token-file", app, "-slack-bot-token-file", bot}, runs
}

// webhook is a stand-in for the alerts webhook: it refuses the first
// request, then records the text of each alert.
type webhook struct {
	mu      sync.Mutex
	refused bool
	alerts  []string
}

func (h *webhook) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.refused {
		h.refused = true
		http.Error(w, "try again", http.StatusInternalServerError)
		return
	}
	var m struct{ Text string }
	json.NewDecoder(r.Body).Decode(&m)
	h.alerts = append(h.alerts, m.Text)
}

func (h *webhook) got() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.alerts...)
}

// The hub with Slack and the webhook configured, from credential files: a
// message posted in Slack reaches the client that is already connected; an
// agent's post reaches the thread under its machine's name; a message the
// hook gives up on is alerted once, by the hub, as from the client. None
// of it waits for the outbound side's polling, and no credential shows up
// in any output.
func TestHubWithSlack(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	hubDB := filepath.Join(dir, "hub.db")
	clientDB := filepath.Join(dir, "client.db")
	credPath := filepath.Join(dir, "credential")
	socket := filepath.Join(dir, "fednet.sock")
	hookRetry = hook.Retry{Min: time.Millisecond, Max: time.Millisecond, Attempts: 2}
	// Only a nudge makes the outbound side look at the inbox.
	outboundInterval = time.Hour
	t.Cleanup(func() { hookRetry, outboundInterval = hook.Retry{}, 0 })

	f := &slack.Fake{}
	f.AddChannel("C1", "repo: fednet")
	slackFlags, runs := fakeSlack(t, dir, f, nil)
	wh := &webhook{}
	hookSrv := httptest.NewServer(wh)
	defer hookSrv.Close()
	webhookFile := filepath.Join(dir, "alert-webhook")
	if err := os.WriteFile(webhookFile, []byte(hookSrv.URL+testHookPath+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(dir, "hub.json")
	if err := os.WriteFile(config, []byte(`{"channels": {"C1": {"machine": "workstation"}}, "users": {"U1": "maintainer"}, "upgrade": {"admins": ["U1"], "auto": false}}`), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr syncBuffer
	var secret string
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		out := stdout.String() + "\n" + stderr.String()
		for _, s := range []string{secret, testAppToken, testBotToken, testHookPath} {
			if s != "" {
				out = strings.ReplaceAll(out, s, "[REDACTED]")
			}
		}
		t.Logf("output:\n%s", out)
	})
	if code := run(ctx, []string{"client", "init", "-id", "workstation", "-credential", credPath}, &stdout, &stderr); code != 0 {
		t.Fatalf("client init: exit %d", code)
	}
	cred, err := auth.Read(credPath)
	if err != nil {
		t.Fatal(err)
	}
	secret = cred.Secret
	fields := strings.Fields(stdout.String())
	if code := run(ctx, []string{"hub", "register", "-db", hubDB, fields[0], fields[1]}, &stdout, &stderr); code != 0 {
		t.Fatalf("hub register: exit %d", code)
	}

	stopHub := start(t, append([]string{"hub", "-listen", "127.0.0.1:0", "-db", hubDB, "-config", config, "-alert-webhook-file", webhookFile}, slackFlags...), &stdout, &stderr)
	var addr string
	waitFor(t, "the hub to listen", func() bool {
		m := listening.FindStringSubmatch(stdout.String())
		if m != nil {
			addr = m[1]
		}
		return m != nil
	})
	var sr slackRun
	select {
	case sr = <-runs:
	case <-time.After(5 * time.Second):
		t.Fatal("the hub did not start Socket Mode")
	}
	if sr.appToken != testAppToken || sr.botToken != testBotToken {
		t.Fatal("the hub did not get the tokens, trimmed, from their files")
	}

	// The hook keeps every event and fails, so each message ends up a dead
	// letter.
	script := filepath.Join(dir, "hook.sh")
	body := "#!/bin/sh\ncat \"$1\" >> \"$FEDNET_TEST_DIR/events\"\necho 'no agent here' >&2\nexit 1\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FEDNET_TEST_DIR", dir)
	stopClient := start(t, []string{"client", "-hub", "http://" + addr, "-db", clientDB, "-credential", credPath, "-socket", socket,
		"-hook-env", "FEDNET_TEST_DIR", "/bin/sh", script}, &stdout, &stderr)
	hs, err := store.OpenHub(ctx, hubDB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hs.Close() })
	waitFor(t, "the client to connect", func() bool {
		reg, err := hs.Registration(ctx, "workstation")
		return err == nil && reg.Version != ""
	})

	// The slash command is wired to the hub: the maintainer gets the
	// versions, with the client online.
	reply, err := sr.r.Commands.Command(ctx, slack.Command{Name: "/fednet", Text: "version", User: "U1"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the client to be online", func() bool {
		reply, err = sr.r.Commands.Command(ctx, slack.Command{Name: "/fednet", Text: "version", User: "U1"})
		return err == nil && strings.Contains(reply.Text, "workstation：dev 在线")
	})
	if !strings.Contains(reply.Text, "hub：dev") || !strings.Contains(reply.Text, "最新 release：查不到") {
		t.Fatalf("/fednet version = %q", reply.Text)
	}
	if reply, _ := sr.r.Commands.Command(ctx, slack.Command{Name: "/fednet", Text: "upgrade", User: "U1"}); !strings.Contains(reply.Text, "不是发布版") {
		t.Fatalf("/fednet upgrade on a dev hub = %q", reply.Text)
	}

	// The hub asked Slack which user its bot is, so that it knows a
	// mention. Someone mentions it in C1: the message reaches the
	// connected client, as a mention from the person's name.
	if sr.r.Bot != slack.FakeBot {
		t.Fatalf("the receiver's bot is %q, want %q from auth.test", sr.r.Bot, slack.FakeBot)
	}
	// The message comes with a screenshot and a log: the screenshot is
	// fetched through the hub before the hook runs, and the event names
	// its path; the log is listed with its id for the agent to fetch.
	shot := f.AddFile(slack.File{ID: "F1", Name: "shot.png", Mimetype: "image/png", URL: "https://example.invalid/F1"}, []byte("PNG..."))
	log := f.AddFile(slack.File{ID: "F2", Name: "build.log", Mimetype: "text/plain", URL: "https://example.invalid/F2"}, []byte("error: ..."))
	ts, err := f.Add("C1", slack.Message{User: "U1", Text: "<@" + slack.FakeBot + "> please fix the build", SubType: "file_share", Files: []slack.File{shot, log}})
	if err != nil {
		t.Fatal(err)
	}
	thread := slack.ThreadKey("C1", ts)
	ev := inbound.Event{ID: "Ev1", Channel: "C1", Message: slack.Message{TS: ts, User: "U1", Text: "<@" + slack.FakeBot + "> please fix the build", SubType: "file_share", Files: []slack.File{shot, log}}}
	if err := sr.r.Handle(ctx, ev); err != nil {
		t.Fatal(err)
	}
	var event hook.Event
	waitFor(t, "the message to reach the hook", func() bool {
		b, err := os.ReadFile(filepath.Join(dir, "events"))
		if err != nil {
			return false
		}
		return json.NewDecoder(strings.NewReader(string(b))).Decode(&event) == nil
	})
	for _, want := range []string{`"thread":"` + thread + `"`, "please fix the build", `"trigger":"mention"`, `"user_name":"maintainer"`} {
		if !strings.Contains(string(event.Payload), want) {
			t.Fatalf("hook got %s, want %s", event.Payload, want)
		}
	}
	var got payload.Message
	if err := json.Unmarshal(event.Payload, &got); err != nil {
		t.Fatal(err)
	}
	shotPath := filepath.Join(dir, "files", "F1", "shot.png")
	wantFiles := []payload.File{
		{ID: "F1", Name: "shot.png", Mimetype: "image/png", Size: 6, URL: "https://example.invalid/F1", Fetch: true, Path: shotPath},
		{ID: "F2", Name: "build.log", Mimetype: "text/plain", Size: 10, URL: "https://example.invalid/F2"},
	}
	if !reflect.DeepEqual(got.Files, wantFiles) {
		t.Fatalf("hook got files %+v, want %+v", got.Files, wantFiles)
	}
	if b, err := os.ReadFile(shotPath); err != nil || string(b) != "PNG..." {
		t.Fatalf("the screenshot at %s holds %q, %v; want the content", shotPath, b, err)
	}
	if fi, err := os.Stat(shotPath); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("the screenshot's mode is %v, %v; want 0600", fi.Mode(), err)
	}
	// The hook gives up on it. The dead letter keeps the payload as the
	// inbox stored it, as the hub sent it: no path.
	cs, err := store.OpenClient(ctx, clientDB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cs.Close() })
	var dead store.DeadLetter
	waitFor(t, "the message among the dead letters", func() bool {
		ds, err := cs.Inbox.DeadLetters(ctx)
		if err != nil || len(ds) != 1 {
			return false
		}
		dead = ds[0]
		return true
	})
	if p := string(dead.Payload); dead.MsgID != event.MsgID || strings.Contains(p, `"path"`) || !strings.Contains(p, `"fetch":true`) {
		t.Fatalf("dead letter %s holds %s, want %s with fetch and no path", dead.MsgID, p, event.MsgID)
	}

	// The client tells the hub, whose first try at the webhook is refused
	// and logged.
	waitFor(t, "the refused alert in the log", func() bool {
		return strings.Contains(stderr.String(), "outbound: alert from a client")
	})

	// An agent posts to the thread: it reaches Slack under the machine's
	// name, and the alert goes out on the same pass.
	if code := run(ctx, []string{"client", "post", "-socket", socket, "-thread", thread, "build is green"}, &stdout, &stderr); code != 0 {
		t.Fatalf("client post: exit %d", code)
	}
	waitFor(t, "the post in Slack", func() bool {
		ms, err := f.Replies(ctx, "C1", ts)
		return err == nil && len(ms) == 2
	})
	ms, _ := f.Replies(ctx, "C1", ts)
	if ms[1].Text != "build is green" || f.Machine(ms[1].TS) != "workstation" {
		t.Fatalf("Slack thread ends with %q from %q, want the post from workstation", ms[1].Text, f.Machine(ms[1].TS))
	}
	a := wh.got()
	if len(a) != 1 || !strings.HasPrefix(a[0], "[workstation] dead letter: msg_id "+event.MsgID) || !strings.Contains(a[0], "no agent here") {
		t.Fatalf("alerts = %q, want one dead letter for %s from workstation", a, event.MsgID)
	}

	if code := stopClient(); code != 0 {
		t.Errorf("client exited %d", code)
	}
	if code := stopHub(); code != 0 {
		t.Errorf("hub exited %d", code)
	}
	out := stdout.String() + stderr.String()
	for name, s := range map[string]string{"app token": testAppToken, "bot token": testBotToken, "webhook": testHookPath, "credential": secret} {
		if strings.Contains(out, s) {
			t.Errorf("output contains the %s", name)
		}
	}
	if strings.Contains(out, "Slack not configured") {
		t.Error("the hub says Slack is not configured")
	}
}

// When Slack rejects the app token, the hub stops, with exit 1 and an
// error that does not quote the token.
func TestHubStopsWhenSlackRejectsToken(t *testing.T) {
	dir := t.TempDir()
	slackFlags, _ := fakeSlack(t, dir, &slack.Fake{}, errors.New("invalid_auth"))
	var stdout, stderr syncBuffer
	code := run(t.Context(), append([]string{"hub", "-listen", "127.0.0.1:0", "-db", filepath.Join(dir, "hub.db")}, slackFlags...), &stdout, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "slack socket mode: invalid_auth") {
		t.Fatalf("exit %d, stderr %q; want 1 and the reason", code, stderr.String())
	}
	if strings.Contains(stderr.String(), testAppToken) || strings.Contains(stderr.String(), testBotToken) {
		t.Fatal("stderr contains a token")
	}
}

// noSelf is a Slack that cannot say which user the bot is.
type noSelf struct{ slack.API }

func (noSelf) Self(context.Context) (string, error) { return "", errors.New("auth.test: not_authed") }

// With Slack, a hub that cannot learn which user its bot is does not
// start: it could not tell a mention.
func TestHubNeedsItsBotUser(t *testing.T) {
	dir := t.TempDir()
	slackFlags, _ := fakeSlack(t, dir, noSelf{&slack.Fake{}}, nil)
	var stdout, stderr syncBuffer
	code := run(t.Context(), append([]string{"hub", "-listen", "127.0.0.1:0", "-db", filepath.Join(dir, "hub.db")}, slackFlags...), &stdout, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "which user the bot is: auth.test: not_authed") {
		t.Fatalf("exit %d, stderr %q; want 1 and the reason", code, stderr.String())
	}
	if strings.Contains(stderr.String(), testAppToken) || strings.Contains(stderr.String(), testBotToken) {
		t.Fatal("stderr contains a token")
	}
}

// With Slack, a webhook file that cannot be read stops the hub before it
// starts.
func TestHubWithSlackNeedsItsWebhookFile(t *testing.T) {
	dir := t.TempDir()
	slackFlags, _ := fakeSlack(t, dir, &slack.Fake{}, nil)
	var stdout, stderr syncBuffer
	args := append([]string{"hub", "-listen", "127.0.0.1:0", "-db", filepath.Join(dir, "hub.db"), "-alert-webhook-file", "/nonexistent/webhook"}, slackFlags...)
	if code := run(t.Context(), args, &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "no such file") {
		t.Fatalf("exit %d, stderr %q; want 1 and the missing file", code, stderr.String())
	}
}
