package slack

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

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
	w := &Web{c: slackgo.New(testToken, slackgo.OptionAPIURL(srv.URL+"/")), token: testToken, base: srv.URL + "/", files: &http.Client{}}
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
			return 200, `{"ok":true,"messages":[{"ts":"1.2","user":"U2","text":"second"},{"ts":"1.3","user":"UBOT","bot_id":"B2","text":"fixed","blocks":[{"type":"context","elements":[{"type":"plain_text","text":"workstation"}]},{"type":"markdown","text":"fixed"}]},{"ts":"1.4","user":"U1","text":"a heading","blocks":[{"type":"header","text":{"type":"plain_text","text":"a heading"}},{"type":"section","text":{"type":"plain_text","text":"x"}}]}],"has_more":false}`
		case "auth.test":
			return 200, `{"ok":true,"user":"fednet","user_id":"UBOT","bot_id":"B2"}`
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
				return 200, `{"ok":true,"messages":[{"ts":"2.3","user":"U2","text":"newest","thread_ts":"2.3","reply_count":1,"latest_reply":"2.4"},{"ts":"2.2","bot_id":"B1","subtype":"bot_message","text":"from a bot"}],"has_more":true,"response_metadata":{"next_cursor":"page2"}}`
			}
			return 200, `{"ok":true,"messages":[{"ts":"2.1","user":"U1","text":"oldest","subtype":"file_share","files":[{"name":"a.txt","mimetype":"text/plain","size":12,"permalink":"https://example.invalid/a"}]}],"has_more":false}`
		case "users.conversations":
			if r.form.Get("cursor") == "" {
				return 200, `{"ok":true,"channels":[{"id":"C1","is_channel":true}],"response_metadata":{"next_cursor":"page2"}}`
			}
			return 200, `{"ok":true,"channels":[{"id":"D1","is_im":true}],"response_metadata":{"next_cursor":""}}`
		}
		return 200, `{"ok":false,"error":"unknown_method"}`
	})

	if self, err := w.Self(ctx); err != nil || self != "UBOT" {
		t.Errorf("Self = %q, %v; want UBOT", self, err)
	}
	// A reply a machine posted carries the machine's name, read back from
	// the context block PostReply puts before the text; a message whose
	// first block is something else names no machine.
	ms, err := w.Replies(ctx, "C1", "1.1")
	if err != nil {
		t.Fatal(err)
	}
	want := []Message{{TS: "1.1", User: "U1", Text: "first"}, {TS: "1.2", User: "U2", Text: "second"}, {TS: "1.3", User: "UBOT", Text: "fixed", BotID: "B2", Machine: "workstation"}, {TS: "1.4", User: "U1", Text: "a heading"}}
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
		{TS: "2.1", User: "U1", Text: "oldest", SubType: "file_share", Files: []File{{Name: "a.txt", Mimetype: "text/plain", Size: 12, URL: "https://example.invalid/a"}}},
		{TS: "2.2", Text: "from a bot", BotID: "B1", SubType: "bot_message"},
		{TS: "2.3", User: "U2", Text: "newest", LatestReply: "2.4"},
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
		{"auth.test", "||||||||"},
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

// paramTexts returns the texts of the card's parameter blocks, in order.
func paramTexts(t *testing.T, bs []slackgo.Block) []string {
	t.Helper()
	var texts []string
	for _, b := range bs {
		rt, ok := b.(*slackgo.RichTextBlock)
		if !ok {
			continue
		}
		pre, ok := rt.Elements[0].(*slackgo.RichTextPreformatted)
		if !ok || len(pre.Elements) != 1 {
			t.Fatalf("rich text block %+v, want one preformatted element with one text", rt)
		}
		texts = append(texts, pre.Elements[0].(*slackgo.RichTextSectionTextElement).Text)
	}
	return texts
}

// A pending card has the two buttons, each carrying the approval id; a
// decided card has none and says how it ended. What the agent wrote goes
// in plain text and preformatted blocks, as it is: backticks, stars and
// angle brackets included, long parameters whole over several blocks.
func TestCardBlocks(t *testing.T) {
	params := "{\n  \"note\": \"```not a fence```\",\n  \"bucket\": \"*example-critical* <https://example.invalid|x>\"\n}"
	c := Card{ID: "apr-1", Summary: "delete *b* <!channel>", Params: params, Machine: "workstation", Agent: "ops-*exec*", Requester: "<@U7>", Expires: time.Unix(1_760_000_000, 0)}
	bs := blocks(c)
	if len(bs) != fixedBlocks+1 {
		t.Fatalf("%d blocks with one parameter block, want the %d fixed ones and it", len(bs), fixedBlocks)
	}
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
	if actions.BlockID != CardBlockID("apr-1") {
		t.Fatalf("buttons are in block %q, want %q", actions.BlockID, CardBlockID("apr-1"))
	}
	if got := paramTexts(t, bs); len(got) != 1 || got[0] != params {
		t.Fatalf("parameter blocks = %q, want the parameters as they are", got)
	}
	// Everything the agent wrote is in a plain_text object, never in a
	// mrkdwn one.
	var plain, mrkdwn []string
	for _, b := range bs {
		sec, ok := b.(*slackgo.SectionBlock)
		if !ok {
			continue
		}
		objs := sec.Fields
		if sec.Text != nil {
			objs = append(objs, sec.Text)
		}
		for _, o := range objs {
			if o.Type == slackgo.PlainTextType {
				plain = append(plain, o.Text)
			} else {
				mrkdwn = append(mrkdwn, o.Text)
			}
		}
	}
	for _, want := range []string{c.Summary, c.Machine, c.Agent, c.Requester} {
		if !slices.Contains(plain, want) {
			t.Errorf("plain text objects %q lack %q", plain, want)
		}
		for _, m := range mrkdwn {
			if strings.Contains(m, want) {
				t.Errorf("mrkdwn object %q carries what the agent wrote, %q", m, want)
			}
		}
	}
	text := blocksJSON(t, bs)
	if !strings.Contains(text, "<!date^1760000000^") || !strings.Contains(text, "apr-1") {
		t.Errorf("pending card %s lacks the expiry token or the id", text)
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

	c.Params = strings.Repeat("é", ParamChunk) + "<&>"
	got := paramTexts(t, blocks(c))
	if len(got) != 2 || strings.Join(got, "") != c.Params || utf8.RuneCountInString(got[0]) != ParamChunk {
		t.Fatalf("parameter blocks = %d, joined %q; want two that join to the parameters, the first %d characters", len(got), strings.Join(got, ""), ParamChunk)
	}
	// The largest parameters that fit make exactly the largest message
	// Slack takes, pending or decided, with or without a requester; the
	// Fake refuses one block more, as Slack would.
	c.Params = strings.Repeat("x", MaxParamChunks*ParamChunk)
	for _, outcome := range []string{"", "approved"} {
		c.Outcome = outcome
		if n := len(blocks(c)); n != maxBlocks {
			t.Fatalf("largest card (outcome %q) has %d blocks, want %d", outcome, n, maxBlocks)
		}
	}
	f := &Fake{}
	f.AddChannel("C9", "")
	if _, err := f.PostCard(context.Background(), "C9", c); err != nil {
		t.Fatalf("the Fake refused the largest card: %v", err)
	}
	c.Params += "x"
	if _, err := f.PostCard(context.Background(), "C9", c); err == nil {
		t.Fatal("the Fake took a card of more blocks than Slack does")
	}
}

// ParamBlocks fits at most MaxParamChunks blocks of ParamChunk characters
// each; one character more does not fit; a character is never split.
func TestParamBlocks(t *testing.T) {
	exact := strings.Repeat("x", MaxParamChunks*ParamChunk)
	if chunks, ok := ParamBlocks(exact); !ok || len(chunks) != MaxParamChunks {
		t.Fatalf("ParamBlocks(exact) = %d blocks, %v; want %d, true", len(chunks), ok, MaxParamChunks)
	}
	if _, ok := ParamBlocks(exact + "x"); ok {
		t.Fatal("ParamBlocks(exact + 1) fits")
	}
	chunks, _ := ParamBlocks(strings.Repeat("x", ParamChunk-1) + "éy")
	if len(chunks) != 2 || chunks[1] != "y" || !utf8.ValidString(chunks[0]) {
		t.Fatalf("ParamBlocks around a two-byte character = %q, want it whole in the first block", chunks)
	}
	if chunks, ok := ParamBlocks(""); !ok || len(chunks) != 1 || chunks[0] != "" {
		t.Fatalf("ParamBlocks(\"\") = %q, %v; want one empty block", chunks, ok)
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

// The upgrade card has the question, the clients and the two buttons in
// a block named after the id; the ack of a command carries it, or the
// text, as an ephemeral message.
func TestUpgradeCardBlocks(t *testing.T) {
	c := UpgradeCard{ID: "up-1", From: "v0.1.0", To: "v0.2.0", Clients: []string{"workstation：v0.1.0 在线", "datamachine：v0.1.0 离线"}}
	ack := CommandAck(CommandReply{Card: &c})
	if ack["response_type"] != "ephemeral" || ack["text"] != "从 v0.1.0 升到 v0.2.0？" {
		t.Fatalf("ack = %+v, want an ephemeral message asking about the upgrade", ack)
	}
	bs, ok := ack["blocks"].([]slackgo.Block)
	if !ok || len(bs) != 3 {
		t.Fatalf("ack blocks = %#v, want the question, the clients and the buttons", ack["blocks"])
	}
	actions, ok := bs[2].(*slackgo.ActionBlock)
	if !ok || actions.BlockID != UpgradeBlockID("up-1") || len(actions.Elements.ElementSet) != 2 {
		t.Fatalf("card ends with %+v, want two buttons in block %q", bs[2], UpgradeBlockID("up-1"))
	}
	for i, want := range []string{UpgradeConfirmAction, UpgradeCancelAction} {
		b, ok := actions.Elements.ElementSet[i].(*slackgo.ButtonBlockElement)
		if !ok || b.ActionID != want || b.Value != "up-1" {
			t.Fatalf("button %d = %+v, want %s carrying up-1", i, actions.Elements.ElementSet[i], want)
		}
	}
	text := blocksJSON(t, bs)
	for _, want := range []string{"v0.1.0", "v0.2.0", "workstation", "datamachine"} {
		if !strings.Contains(text, want) {
			t.Errorf("card %s lacks %q", text, want)
		}
	}
	plain := CommandAck(CommandReply{Text: "hub：v0.1.0"})
	if plain["text"] != "hub：v0.1.0" || plain["blocks"] != nil {
		t.Fatalf("ack of a text reply = %+v", plain)
	}
}

// Respond posts to the response URL, replacing the message there, as an
// ephemeral message.
func TestRespond(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&got)
		fmt.Fprint(w, "ok")
	}))
	defer srv.Close()
	w := New("test-token")
	if err := w.Respond(t.Context(), srv.URL+"/respond", "已取消"); err != nil {
		t.Fatal(err)
	}
	if got["text"] != "已取消" || got["response_type"] != "ephemeral" || got["replace_original"] != true {
		t.Fatalf("the response URL got %+v", got)
	}
	// A failure's error carries neither the URL nor its path: the URL
	// lets whoever has it post in the sender's place.
	srv.Close()
	responseURL := srv.URL + "/actions/T0/secret-response-3f9a1c"
	err := w.Respond(t.Context(), responseURL, "已取消")
	if err == nil {
		t.Fatal("Respond to a closed server did not fail")
	}
	for _, s := range []string{responseURL, "secret-response-3f9a1c", srv.URL} {
		if strings.Contains(err.Error(), s) {
			t.Fatalf("the error %q carries %q", err, s)
		}
	}
	if !strings.Contains(err.Error(), "response_url") {
		t.Fatalf("the error %q does not say what failed", err)
	}
}

func TestMentions(t *testing.T) {
	for _, tt := range []struct {
		text, user string
		want       bool
	}{
		{"<@UBOT> look at this", "UBOT", true},
		{"look at this <@UBOT|fednet>", "UBOT", true},
		{"<@UBOTX> is someone else", "UBOT", false},
		{"@fednet typed, not picked", "UBOT", false},
		{"<@UBOT>", "", false},
	} {
		if got := Mentions(tt.text, tt.user); got != tt.want {
			t.Errorf("Mentions(%q, %q) = %v, want %v", tt.text, tt.user, got, tt.want)
		}
	}
}

// files.info and the download of a file: the download goes with the token
// to Slack's host (here the test server's), not elsewhere; a file Slack has
// deleted is ErrNotFound; one hosted outside Slack has nothing to download.
func TestWebFiles(t *testing.T) {
	ctx := context.Background()
	var srv *httptest.Server
	var downloads []string
	w, _, srv := newTestWeb(t, func(r request) (int, string) {
		switch r.method {
		case "files.info":
			switch r.form.Get("file") {
			case "F1":
				return 200, `{"ok":true,"file":{"id":"F1","name":"shot.png","mimetype":"image/png","size":6,"permalink":"https://example.invalid/F1","url_private_download":"` + srv.URL + `/files-pri/F1/download/shot.png"}}`
			case "F2":
				return 200, `{"ok":true,"file":{"id":"F2","name":"doc","is_external":true,"permalink":"https://example.invalid/F2","url_private":"https://docs.example.invalid/doc"}}`
			case "F3":
				return 200, `{"ok":true,"file":{"id":"F3","name":"shot.png","size":6,"url_private_download":"https://files.example.invalid/F3"}}`
			}
			return 200, `{"ok":false,"error":"file_not_found"}`
		case "shot.png":
			downloads = append(downloads, r.auth)
			return 200, `PNG...`
		}
		return 200, `{"ok":false,"error":"unknown_method"}`
	})
	f, err := w.FileInfo(ctx, "F1")
	if err != nil {
		t.Fatal(err)
	}
	want := File{ID: "F1", Name: "shot.png", Mimetype: "image/png", Size: 6, URL: "https://example.invalid/F1", DownloadURL: srv.URL + "/files-pri/F1/download/shot.png"}
	if f != want {
		t.Fatalf("FileInfo = %+v, want %+v", f, want)
	}
	body, err := w.Download(ctx, f)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(body)
	body.Close()
	if err != nil || string(b) != "PNG..." {
		t.Fatalf("Download read %q, %v; want the content", b, err)
	}
	if len(downloads) != 1 || downloads[0] != "Bearer "+testToken {
		t.Fatalf("the download went with %q, want the bot token as a bearer", downloads)
	}
	if _, err := w.FileInfo(ctx, "F9"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("FileInfo of a missing file = %v, want ErrNotFound", err)
	}
	if _, err := w.FileInfo(ctx, "F2"); err == nil || !strings.Contains(err.Error(), "outside Slack") {
		t.Fatalf("FileInfo of an external file = %v, want an error saying so", err)
	}
	// The token goes only to Slack or to the API's host.
	f3, err := w.FileInfo(ctx, "F3")
	if err != nil {
		t.Fatal(err)
	}
	_, err = w.Download(ctx, f3)
	if err == nil || !strings.Contains(err.Error(), "refusing to send the token") || len(downloads) != 1 {
		t.Fatalf("Download from another host = %v, %d downloads; want it refused before any request", err, len(downloads))
	}
	if strings.Contains(err.Error(), testToken) {
		t.Fatal("the error quotes the token")
	}
	// The file's own name is not needed for the download URL; a sign-in
	// redirect (without access, Slack sends the browser to log in) is
	// reported, not served as the file.
	redirecting := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/signin" {
			rw.Write([]byte("<html>sign in</html>"))
			return
		}
		http.Redirect(rw, r, "/signin?redir="+r.URL.Path, http.StatusFound)
	}))
	defer redirecting.Close()
	w.base = redirecting.URL + "/"
	if _, err := w.Download(ctx, File{ID: "F4", DownloadURL: redirecting.URL + "/files-pri/F4"}); err == nil || !strings.Contains(err.Error(), "sign-in page") {
		t.Fatalf("Download redirected to the sign-in page = %v, want an error saying so", err)
	}
}
