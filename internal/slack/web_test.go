package slack

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	slackgo "github.com/slack-go/slack"
)

const testToken = "xoxb-test-0000-token"

// request is one request the test server got.
type request struct {
	method string
	form   url.Values
	auth   string
}

// testSlack stands in for Slack's Web API. answer gets each request and
// returns the HTTP status and the JSON body; a 429 comes with retryAfter
// in Retry-After. answer runs with mu held, so a test changes what it
// answers under mu.
type testSlack struct {
	mu         sync.Mutex
	retryAfter string
	requests   []request
	waits      []time.Duration
}

// newTestWeb returns a Web that calls a test server answering with answer,
// and records the waits it is asked for instead of sleeping.
func newTestWeb(t *testing.T, answer func(r request) (int, string)) (*Web, *testSlack, *httptest.Server) {
	ts := &testSlack{retryAfter: "2"}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, hr *http.Request) {
		if err := hr.ParseForm(); err != nil {
			t.Error(err)
		}
		r := request{method: path.Base(hr.URL.Path), form: hr.PostForm, auth: hr.Header.Get("Authorization")}
		ts.mu.Lock()
		ts.requests = append(ts.requests, r)
		status, body := answer(r)
		if status == http.StatusTooManyRequests {
			w.Header().Set("Retry-After", ts.retryAfter)
		}
		ts.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	w := &Web{c: slackgo.New(testToken, slackgo.OptionAPIURL(srv.URL+"/"))}
	w.sleep = func(_ context.Context, d time.Duration) error {
		ts.mu.Lock()
		defer ts.mu.Unlock()
		ts.waits = append(ts.waits, d)
		return nil
	}
	return w, ts, srv
}

func (ts *testSlack) got() ([]request, []time.Duration) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return append([]request(nil), ts.requests...), append([]time.Duration(nil), ts.waits...)
}

func TestWeb(t *testing.T) {
	ctx := context.Background()
	w, ts, _ := newTestWeb(t, func(r request) (int, string) {
		switch r.method {
		case "conversations.replies":
			if r.form.Get("cursor") == "" {
				return 200, `{"ok":true,"messages":[{"ts":"1.1","user":"U1","text":"first"}],"has_more":true,"response_metadata":{"next_cursor":"page2"}}`
			}
			return 200, `{"ok":true,"messages":[{"ts":"1.2","user":"U2","text":"second"}],"has_more":false}`
		case "chat.postMessage":
			return 200, `{"ok":true,"channel":"` + r.form.Get("channel") + `","ts":"1.3"}`
		case "conversations.info":
			return 200, `{"ok":true,"channel":{"id":"C1","purpose":{"value":"what C1 is for"}}}`
		case "conversations.setPurpose":
			return 200, `{"ok":true,"channel":{"id":"C1"}}`
		case "conversations.open":
			return 200, `{"ok":true,"channel":{"id":"D1"}}`
		case "conversations.history":
			if r.form.Get("cursor") == "" {
				return 200, `{"ok":true,"messages":[{"ts":"2.3","user":"U2","text":"newest","thread_ts":"2.3","reply_count":1},{"ts":"2.2","bot_id":"B1","subtype":"bot_message","text":"from a bot"}],"has_more":true,"response_metadata":{"next_cursor":"page2"}}`
			}
			return 200, `{"ok":true,"messages":[{"ts":"2.1","user":"U1","text":"oldest","subtype":"file_share","files":[{"name":"a.txt","permalink":"https://example.invalid/a"}]}],"has_more":false}`
		case "users.conversations":
			if r.form.Get("cursor") == "" {
				return 200, `{"ok":true,"channels":[{"id":"C1","is_channel":true}],"response_metadata":{"next_cursor":"page2"}}`
			}
			return 200, `{"ok":true,"channels":[{"id":"D1","is_im":true}],"response_metadata":{"next_cursor":""}}`
		}
		return 200, `{"ok":false,"error":"unknown_method"}`
	})

	ms, err := w.Replies(ctx, "C1", "1.1")
	if err != nil {
		t.Fatal(err)
	}
	want := []Message{{TS: "1.1", User: "U1", Text: "first"}, {TS: "1.2", User: "U2", Text: "second"}}
	if !reflect.DeepEqual(ms, want) {
		t.Errorf("Replies = %v, want %v", ms, want)
	}
	ts1, err := w.Post(ctx, "C1", "hello")
	if err != nil || ts1 != "1.3" {
		t.Errorf("Post = %q, %v; want 1.3", ts1, err)
	}
	p, err := w.Purpose(ctx, "C1")
	if err != nil || p != "what C1 is for" {
		t.Errorf("Purpose = %q, %v", p, err)
	}
	if err := w.SetPurpose(ctx, "C1", "new purpose"); err != nil {
		t.Error(err)
	}
	if err := w.DM(ctx, "U1", "psst"); err != nil {
		t.Error(err)
	}
	// History comes from Slack newest first and is returned oldest first,
	// with the fields the inbound filter needs; a thread's first message
	// is not a reply, so its thread_ts is dropped.
	hs, err := w.History(ctx, "C1", "2.0")
	if err != nil {
		t.Fatal(err)
	}
	wantHistory := []Message{
		{TS: "2.1", User: "U1", Text: "oldest", SubType: "file_share", Files: []File{{Name: "a.txt", URL: "https://example.invalid/a"}}},
		{TS: "2.2", Text: "from a bot", BotID: "B1", SubType: "bot_message"},
		{TS: "2.3", User: "U2", Text: "newest"},
	}
	if !reflect.DeepEqual(hs, wantHistory) {
		t.Errorf("History = %+v, want %+v", hs, wantHistory)
	}
	cs, err := w.Conversations(ctx)
	if err != nil || !reflect.DeepEqual(cs, []Conversation{{ID: "C1"}, {ID: "D1", IM: true}}) {
		t.Errorf("Conversations = %+v, %v; want C1 and the IM D1", cs, err)
	}

	type sent struct{ method, args string }
	var got []sent
	rs, _ := ts.got()
	for _, r := range rs {
		f := r.form
		got = append(got, sent{r.method, strings.Join([]string{f.Get("channel"), f.Get("ts"), f.Get("cursor"), f.Get("text"), f.Get("purpose"), f.Get("users"), f.Get("oldest"), f.Get("types"), f.Get("thread_ts")}, "|")})
	}
	wantSent := []sent{
		{"conversations.replies", "C1|1.1|||||||"},
		{"conversations.replies", "C1|1.1|page2||||||"},
		{"chat.postMessage", "C1|||hello|||||"},
		{"conversations.info", "C1||||||||"},
		{"conversations.setPurpose", "C1||||new purpose||||"},
		{"conversations.open", "|||||U1|||"},
		{"chat.postMessage", "D1|||psst|||||"},
		{"conversations.history", "C1||||||2.0||"},
		{"conversations.history", "C1||page2||||2.0||"},
		{"users.conversations", "|||||||public_channel,private_channel,im|"},
		{"users.conversations", "||page2|||||public_channel,private_channel,im|"},
	}
	if !reflect.DeepEqual(got, wantSent) {
		t.Errorf("requests = %v, want %v", got, wantSent)
	}
}

func TestWebNotFound(t *testing.T) {
	ctx := context.Background()
	code := "channel_not_found"
	w, _, _ := newTestWeb(t, func(request) (int, string) {
		return 200, `{"ok":false,"error":"` + code + `"}`
	})
	calls := map[string]func() error{
		"Replies":    func() error { _, err := w.Replies(ctx, "C1", "1.1"); return err },
		"Post":       func() error { _, err := w.Post(ctx, "C1", "hi"); return err },
		"Purpose":    func() error { _, err := w.Purpose(ctx, "C1"); return err },
		"SetPurpose": func() error { return w.SetPurpose(ctx, "C1", "p") },
		"DM":         func() error { return w.DM(ctx, "U1", "hi") },
		"History":    func() error { _, err := w.History(ctx, "C1", "1.0"); return err },
	}
	for name, call := range calls {
		if err := call(); !errors.Is(err, ErrNotFound) {
			t.Errorf("%s with %s: err = %v, want ErrNotFound", name, code, err)
		}
	}
	code = "thread_not_found"
	if err := calls["Replies"](); !errors.Is(err, ErrNotFound) {
		t.Errorf("Replies with %s: err = %v, want ErrNotFound", code, err)
	}
	code = "not_in_channel"
	err := calls["Post"]()
	if err == nil || errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), code) {
		t.Errorf("Post with %s: err = %v, want an error naming it that is not ErrNotFound", code, err)
	}
}

func TestWebRateLimit(t *testing.T) {
	ctx := context.Background()
	limited := 2
	w, ts, _ := newTestWeb(t, func(request) (int, string) {
		if limited > 0 {
			limited--
			return http.StatusTooManyRequests, `{"ok":false,"error":"ratelimited"}`
		}
		return 200, `{"ok":true,"channel":"C1","ts":"1.1"}`
	})
	if ts1, err := w.Post(ctx, "C1", "hi"); err != nil || ts1 != "1.1" {
		t.Fatalf("Post = %q, %v; want it to succeed after waiting", ts1, err)
	}
	rs, waits := ts.got()
	if len(rs) != 3 || !reflect.DeepEqual(waits, []time.Duration{2 * time.Second, 2 * time.Second}) {
		t.Errorf("%d requests, waits %v; want 3 requests and two waits of Retry-After", len(rs), waits)
	}

	ts.mu.Lock()
	limited = maxRetries + 1
	ts.mu.Unlock()
	_, err := w.Post(ctx, "C1", "hi")
	var rl *slackgo.RateLimitedError
	if !errors.As(err, &rl) {
		t.Errorf("Post rate-limited past the limit: err = %v, want a rate limit error", err)
	}
	rs, waits = ts.got()
	if len(rs) != 3+maxRetries+1 || len(waits) != 2+maxRetries {
		t.Errorf("%d requests and %d waits in all, want %d and %d", len(rs), len(waits), 3+maxRetries+1, 2+maxRetries)
	}

	ts.mu.Lock()
	limited = 1
	ts.retryAfter = strconv.Itoa(int(maxWait/time.Second) + 1)
	ts.mu.Unlock()
	if _, err := w.Post(ctx, "C1", "hi"); !errors.As(err, &rl) {
		t.Errorf("Post asked to wait longer than maxWait: err = %v, want a rate limit error", err)
	}
	rs, waits = ts.got()
	if len(rs) != 3+maxRetries+2 || len(waits) != 2+maxRetries {
		t.Errorf("asked to wait longer than maxWait, it sent %d requests and waited %d times in all, want %d and %d", len(rs), len(waits), 3+maxRetries+2, 2+maxRetries)
	}
}

func TestWebKeepsTokenOutOfErrors(t *testing.T) {
	ctx := context.Background()
	var status int
	var body string
	w, ts, srv := newTestWeb(t, func(request) (int, string) { return status, body })

	var errs []error
	for _, a := range []struct {
		status int
		body   string
	}{
		{200, `{"ok":false,"error":"invalid_auth"}`},
		{200, `{"ok":false,"error":"channel_not_found"}`},
		{http.StatusTooManyRequests, `{"ok":false,"error":"ratelimited"}`},
		{http.StatusInternalServerError, `oops`},
		{200, `not json`},
	} {
		ts.mu.Lock()
		status, body = a.status, a.body
		ts.mu.Unlock()
		_, err := w.Post(ctx, "C1", "hi")
		errs = append(errs, err)
	}
	rs, _ := ts.got()
	if r := rs[0]; r.form.Get("token") != testToken && r.auth != "Bearer "+testToken {
		t.Fatalf("Slack got no token: form %v, Authorization %q", r.form, r.auth)
	}
	srv.Close()
	_, err := w.Post(ctx, "C1", "hi")
	errs = append(errs, err)

	for _, err := range errs {
		if err == nil {
			t.Error("a failed call returned no error")
		} else if strings.Contains(err.Error(), testToken) {
			t.Errorf("error %q contains the token", err)
		}
	}
}

func TestWebPostReplyAndDelete(t *testing.T) {
	ctx := context.Background()
	w, ts, _ := newTestWeb(t, func(r request) (int, string) {
		switch r.method {
		case "chat.postMessage":
			return 200, `{"ok":true,"channel":"C1","ts":"1.5"}`
		case "chat.delete":
			if r.form.Get("ts") == "9.9" {
				return 200, `{"ok":false,"error":"message_not_found"}`
			}
			return 200, `{"ok":true,"channel":"C1","ts":"` + r.form.Get("ts") + `"}`
		}
		return 200, `{"ok":false,"error":"unknown_method"}`
	})
	if got, err := w.PostReply(ctx, "C1", "1.1", "workstation", "the build is fixed"); err != nil || got != "1.5" {
		t.Fatalf("PostReply = %q, %v; want 1.5", got, err)
	}
	if err := w.Delete(ctx, "C1", "1.5"); err != nil {
		t.Fatal(err)
	}
	if err := w.Delete(ctx, "C1", "9.9"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Delete of a message Slack does not know = %v, want ErrNotFound", err)
	}

	rs, _ := ts.got()
	post := rs[0].form
	if rs[0].method != "chat.postMessage" || post.Get("channel") != "C1" || post.Get("thread_ts") != "1.1" || post.Get("text") != "the build is fixed" {
		t.Fatalf("PostReply sent %s %v", rs[0].method, post)
	}
	var blocks []struct {
		Type     string `json:"type"`
		Text     any    `json:"text"`
		Elements []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"elements"`
	}
	if err := json.Unmarshal([]byte(post.Get("blocks")), &blocks); err != nil {
		t.Fatalf("blocks %q: %v", post.Get("blocks"), err)
	}
	if len(blocks) != 2 || blocks[0].Type != "context" || len(blocks[0].Elements) != 1 || blocks[0].Elements[0].Text != "workstation" ||
		blocks[1].Type != "markdown" || blocks[1].Text != "the build is fixed" {
		t.Fatalf("blocks = %s, want a context block naming the machine, then the text", post.Get("blocks"))
	}
	if rs[1].method != "chat.delete" || rs[1].form.Get("channel") != "C1" || rs[1].form.Get("ts") != "1.5" {
		t.Fatalf("Delete sent %s %v", rs[1].method, rs[1].form)
	}
}

// blocksJSON writes bs as JSON without escaping < and >, which Slack's
// date and user tokens are made of.
func blocksJSON(t *testing.T, bs []slackgo.Block) string {
	t.Helper()
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(bs); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

// A pending card has the two buttons, each carrying the approval id; a
// decided card has none and says how it ended; long parameters are cut.
func TestCardBlocks(t *testing.T) {
	c := Card{ID: "apr-1", Summary: "delete b", Params: `{"b":1}`, Machine: "workstation", Agent: "ops-exec", Expires: time.Unix(1_760_000_000, 0)}
	bs := blocks(c)
	actions, ok := bs[len(bs)-1].(*slackgo.ActionBlock)
	if !ok || len(actions.Elements.ElementSet) != 2 {
		t.Fatalf("pending card ends with %T, want an action block with two buttons", bs[len(bs)-1])
	}
	for i, want := range []string{ApproveAction, RejectAction} {
		b, ok := actions.Elements.ElementSet[i].(*slackgo.ButtonBlockElement)
		if !ok || b.ActionID != want || b.Value != "apr-1" {
			t.Fatalf("button %d = %+v, want %s carrying apr-1", i, actions.Elements.ElementSet[i], want)
		}
	}
	text := blocksJSON(t, bs)
	for _, want := range []string{"delete b", `{\"b\":1}`, "workstation", "ops-exec", "<!date^1760000000^"} {
		if !strings.Contains(text, want) {
			t.Errorf("pending card %s lacks %q", text, want)
		}
	}

	c.Outcome, c.Approver, c.DecidedAt = "approved", "U1", time.Unix(1_760_000_100, 0)
	bs = blocks(c)
	if _, ok := bs[len(bs)-1].(*slackgo.ActionBlock); ok {
		t.Fatal("decided card still has buttons")
	}
	text = blocksJSON(t, bs)
	if !strings.Contains(text, "已批准") || !strings.Contains(text, "<@U1>") || !strings.Contains(text, "<!date^1760000100^") {
		t.Errorf("approved card %s does not say approved by U1 at the time", text)
	}
	c.Outcome, c.Approver = "rejected", "U2"
	if text = blocksJSON(t, blocks(c)); !strings.Contains(text, "已拒绝") || !strings.Contains(text, "<@U2>") {
		t.Errorf("rejected card %s does not say rejected by U2", text)
	}
	c.Outcome, c.Approver = "expired", ""
	if text = blocksJSON(t, blocks(c)); !strings.Contains(text, "已过期") {
		t.Errorf("expired card %s does not say expired", text)
	}

	c.Params = strings.Repeat("x", maxParams+1)
	text = blocksJSON(t, blocks(c))
	if strings.Contains(text, c.Params) || !strings.Contains(text, "truncated") {
		t.Error("long parameters are not cut")
	}
}

// PostCard, UpdateCard and Whisper call chat.postMessage, chat.update and
// chat.postEphemeral with the card's blocks and the summary as the text.
func TestWebCards(t *testing.T) {
	ctx := context.Background()
	w, ts, _ := newTestWeb(t, func(r request) (int, string) {
		switch r.method {
		case "chat.postMessage", "chat.update":
			return 200, `{"ok":true,"channel":"C9","ts":"1.5"}`
		case "chat.postEphemeral":
			return 200, `{"ok":true,"message_ts":"1.6"}`
		}
		return 200, `{"ok":false,"error":"unknown_method"}`
	})
	c := Card{ID: "apr-1", Summary: "delete b", Params: `{"b":1}`, Machine: "workstation", Agent: "ops-exec", Expires: time.Unix(1_760_000_000, 0)}
	if got, err := w.PostCard(ctx, "C9", c); err != nil || got != "1.5" {
		t.Fatalf("PostCard = %q, %v; want 1.5", got, err)
	}
	c.Outcome, c.Approver, c.DecidedAt = "approved", "U1", time.Unix(1_760_000_100, 0)
	if err := w.UpdateCard(ctx, "C9", "1.5", c); err != nil {
		t.Fatal(err)
	}
	if err := w.Whisper(ctx, "C9", "U2", "你不在审批人名单上"); err != nil {
		t.Fatal(err)
	}
	rs, _ := ts.got()
	if len(rs) != 3 {
		t.Fatalf("%d requests, want 3", len(rs))
	}
	post, update, whisper := rs[0], rs[1], rs[2]
	if post.method != "chat.postMessage" || post.form.Get("channel") != "C9" || post.form.Get("text") != "delete b" || !strings.Contains(post.form.Get("blocks"), `"action_id":"approve"`) {
		t.Errorf("PostCard sent %s %v, want a message in C9 with the buttons", post.method, post.form)
	}
	if update.method != "chat.update" || update.form.Get("channel") != "C9" || update.form.Get("ts") != "1.5" || strings.Contains(update.form.Get("blocks"), `"action_id"`) || !strings.Contains(update.form.Get("blocks"), "已批准") {
		t.Errorf("UpdateCard sent %s %v, want an update of 1.5 in C9 without buttons", update.method, update.form)
	}
	if whisper.method != "chat.postEphemeral" || whisper.form.Get("channel") != "C9" || whisper.form.Get("user") != "U2" || whisper.form.Get("text") != "你不在审批人名单上" {
		t.Errorf("Whisper sent %s %v, want an ephemeral message to U2 in C9", whisper.method, whisper.form)
	}
}
