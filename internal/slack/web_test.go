package slack

import (
	"context"
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
	if ts2, err := w.PostReply(ctx, "C1", "1.1", "in the thread"); err != nil || ts2 != "1.3" {
		t.Errorf("PostReply = %q, %v; want 1.3", ts2, err)
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
		{"chat.postMessage", "C1|||in the thread|||||1.1"},
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
		"PostReply":  func() error { _, err := w.PostReply(ctx, "C1", "1.1", "hi"); return err },
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
