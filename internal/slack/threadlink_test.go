package slack

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	slackgo "github.com/slack-go/slack"

	"github.com/Luolc/fednet/internal/question"
)

// The ids and ts in these tests are made up.
const (
	keyA = "C0FAKE0001/1700000000.000100"
	keyB = "G0FAKE0002/1700000001.000200"
	keyC = "D0FAKE0003/1700000002.000300"
)

// threadKeys finds the keys a person reads as text, and none in code, in
// links or URLs, or in something longer such as a path.
func TestThreadKeys(t *testing.T) {
	for _, tt := range []struct {
		text string
		want []string
	}{
		{"see " + keyA, []string{keyA}},
		{keyA + " and " + keyB + ", then " + keyC + ".", []string{keyA, keyB, keyC}},
		{"线程" + keyA + "里", []string{keyA}},
		{"(" + keyA + ") **" + keyB + "**", []string{keyA, keyB}},
		{"```\n" + keyA + "\n```\n" + keyB, []string{keyB}},
		{"~~~go\n" + keyA + "\n~~~~\n" + keyB, []string{keyB}},
		{"````\n```\n" + keyA + "\n````\n" + keyB, []string{keyB}},
		{"```\n" + keyA, nil},
		{"`" + keyA + "` " + keyB, []string{keyB}},
		{"``a ` " + keyA + "`` " + keyB, []string{keyB}},
		{"` " + keyA, []string{keyA}},
		{"> ```\n> " + keyA + "\n> ```\n" + keyB, []string{keyB}},
		{"[" + keyA + "](https://example.invalid/x) " + keyB, []string{keyB}},
		{"[thread](https://example.invalid/?t=" + keyA + ") " + keyB, []string{keyB}},
		{"<https://example.invalid/" + keyA + "|t> " + keyB, []string{keyB}},
		{"https://example.invalid/?t=" + keyA + " " + keyB, []string{keyB}},
		{"a/" + keyA, nil},
		{"<" + keyA + ">", nil},
		{keyA + "/b", nil},
		{"x" + keyA, nil},
		{keyA + "x", nil},
		{keyA + "_1", nil},
		{"v1." + keyA, nil},
		{keyA + ".5", nil},
		{"X0FAKE0001/1700000000.000100", nil},
		{"c0fake0001/1700000000.000100", nil},
		{"C0FAKE01/1700000000.000100", nil},
		{"C0FAKE0001/170000000.000100", nil},
		{"C0FAKE0001/1700000000.00010", nil},
		{"C0FAKE0001/1700000000.0001000", nil},
		{"C0FAKE0001/1700000000", nil},
	} {
		var got []string
		for _, s := range threadKeys(tt.text) {
			got = append(got, tt.text[s[0]:s[1]])
		}
		if !slices.Equal(got, tt.want) {
			t.Errorf("threadKeys(%q) = %q, want %q", tt.text, got, tt.want)
		}
	}
}

// blockText returns the text of the only markdown block in blocks, the
// JSON of a message's blocks.
func blockText(t *testing.T, blocks string) string {
	t.Helper()
	var bs []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal([]byte(blocks), &bs); err != nil {
		t.Fatalf("blocks %q: %v", blocks, err)
	}
	for _, b := range bs {
		if b.Type == "markdown" {
			return b.Text
		}
	}
	t.Fatalf("blocks %s have no markdown block", blocks)
	return ""
}

// A thread key in a post shows as a link to its thread, asked of Slack
// once for each key; a key Slack gives no link for, or one that is not
// an https URL, stays as it is, as do keys in code. The plain text is the
// post as given.
func TestWebLinksThreadKeys(t *testing.T) {
	ctx := context.Background()
	linkA := "https://example.invalid/archives/C0FAKE0001/p1700000000000100"
	w, ts, _ := newTestWeb(t, func(r request) (int, string) {
		switch r.method {
		case "chat.getPermalink":
			switch r.query.Get("channel") + "/" + r.query.Get("message_ts") {
			case keyA:
				return 200, `{"ok":true,"channel":"C0FAKE0001","permalink":"` + linkA + `"}`
			case keyC:
				return 200, `{"ok":true,"channel":"D0FAKE0003","permalink":"javascript:alert(1)"}`
			}
			return 200, `{"ok":false,"error":"channel_not_found"}`
		case "chat.postMessage":
			return 200, `{"ok":true,"channel":"C1","ts":"1.5"}`
		}
		return 200, `{"ok":false,"error":"unknown_method"}`
	})
	text := "see " + keyA + ", " + keyB + " and " + keyC + "; again " + keyA + "\n`" + keyA + "`"
	if _, err := w.PostReply(ctx, "C1", "1.1", text); err != nil {
		t.Fatal(err)
	}
	// A key linked before is not asked of Slack again.
	if _, err := w.PostReply(ctx, "C1", "1.1", "and "+keyA); err != nil {
		t.Fatal(err)
	}
	if _, err := w.PostFooter(ctx, "C1", "1.1", "from "+keyA); err != nil {
		t.Fatal(err)
	}

	rs, _ := ts.got()
	var asked []string
	var posts []request
	for _, r := range rs {
		switch r.method {
		case "chat.getPermalink":
			asked = append(asked, r.query.Get("channel")+"/"+r.query.Get("message_ts"))
		case "chat.postMessage":
			posts = append(posts, r)
		}
	}
	if want := []string{keyA, keyB, keyC}; !slices.Equal(asked, want) {
		t.Fatalf("asked Slack for the permalinks of %q, want %q", asked, want)
	}
	if len(posts) != 3 {
		t.Fatalf("%d posts, want 3", len(posts))
	}
	linked := "[" + keyA + "](" + linkA + ")"
	want := "see " + linked + ", " + keyB + " and " + keyC + "; again " + linked + "\n`" + keyA + "`"
	if got := blockText(t, posts[0].form.Get("blocks")); got != want {
		t.Fatalf("markdown block = %q\nwant             %q", got, want)
	}
	if got := posts[0].form.Get("text"); got != text {
		t.Fatalf("plain text = %q, want the post as given, %q", got, text)
	}
	if got, want := blockText(t, posts[1].form.Get("blocks")), "and "+linked; got != want {
		t.Fatalf("second markdown block = %q, want %q", got, want)
	}
	var footer []struct {
		Elements []struct {
			Text string `json:"text"`
		} `json:"elements"`
	}
	if err := json.Unmarshal([]byte(posts[2].form.Get("blocks")), &footer); err != nil || len(footer) != 1 || len(footer[0].Elements) != 1 {
		t.Fatalf("footer blocks = %s, %v; want one context block of one element", posts[2].form.Get("blocks"), err)
	}
	if got, want := footer[0].Elements[0].Text, "from <"+linkA+"|"+keyA+">"; got != want {
		t.Fatalf("footer = %q, want %q", got, want)
	}
}

// When Slack gives no permalinks, the post goes out as given; so it does
// when the links would take it over what a markdown block takes.
func TestWebPostsKeysAsTheyAreWithoutLinks(t *testing.T) {
	ctx := context.Background()
	fail := true
	w, ts, _ := newTestWeb(t, func(r request) (int, string) {
		switch r.method {
		case "chat.getPermalink":
			if fail {
				return 500, `{"ok":false,"error":"internal_error"}`
			}
			return 200, `{"ok":true,"permalink":"https://example.invalid/archives/p1"}`
		case "chat.postMessage":
			return 200, `{"ok":true,"channel":"C1","ts":"1.5"}`
		}
		return 200, `{"ok":false,"error":"unknown_method"}`
	})
	text := "see " + keyA
	if got, err := w.PostReply(ctx, "C1", "1.1", text); err != nil || got != "1.5" {
		t.Fatalf("PostReply = %q, %v; want 1.5", got, err)
	}
	ts.mu.Lock()
	fail = false
	ts.mu.Unlock()
	long := strings.Repeat("x", maxMarkdownChars-len(keyB)-1) + " " + keyB
	if _, err := w.PostReply(ctx, "C1", "1.1", long); err != nil {
		t.Fatal(err)
	}

	rs, _ := ts.got()
	var posts []request
	for _, r := range rs {
		if r.method == "chat.postMessage" {
			posts = append(posts, r)
		}
	}
	if len(posts) != 2 {
		t.Fatalf("%d posts, want 2", len(posts))
	}
	if got := blockText(t, posts[0].form.Get("blocks")); got != text {
		t.Fatalf("markdown block = %q, want the post as given, %q", got, text)
	}
	if got := blockText(t, posts[1].form.Get("blocks")); got != long {
		t.Fatalf("markdown block of %d characters, want the %d of the post as given", len(got), len(long))
	}
}

// At most maxLinks keys of one text are asked of Slack; the rest stay as
// they are.
func TestWebLinksAtMostMaxLinks(t *testing.T) {
	w, ts, _ := newTestWeb(t, func(r request) (int, string) {
		switch r.method {
		case "chat.getPermalink":
			return 200, `{"ok":true,"permalink":"https://example.invalid/archives/` + r.query.Get("channel") + `/p1"}`
		case "chat.postMessage":
			return 200, `{"ok":true,"channel":"C1","ts":"1.5"}`
		}
		return 200, `{"ok":false,"error":"unknown_method"}`
	})
	var keys []string
	for i := range maxLinks + 1 {
		keys = append(keys, fmt.Sprintf("C0FAKE%04d/1700000000.000100", i))
	}
	if _, err := w.PostReply(context.Background(), "C1", "1.1", strings.Join(keys, " ")); err != nil {
		t.Fatal(err)
	}
	rs, _ := ts.got()
	var asked int
	var blocks string
	for _, r := range rs {
		switch r.method {
		case "chat.getPermalink":
			asked++
		case "chat.postMessage":
			blocks = r.form.Get("blocks")
		}
	}
	if asked != maxLinks {
		t.Fatalf("asked Slack %d times, want %d", asked, maxLinks)
	}
	last := keys[maxLinks]
	if got := blockText(t, blocks); !strings.HasSuffix(got, " "+last) || strings.Count(got, "](") != maxLinks {
		t.Fatalf("markdown block = %q, want %d keys linked and %s as it is", got, maxLinks, last)
	}
}

// A post goes out as given, within linkWait, when Slack does not answer
// for the permalinks.
func TestWebDoesNotWaitLongForLinks(t *testing.T) {
	defer func(d time.Duration) { linkWait = d }(linkWait)
	linkWait = 50 * time.Millisecond
	var mu sync.Mutex
	var blocks string
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/chat.getPermalink") {
			select {
			case <-r.Context().Done():
			case <-time.After(10 * time.Second):
			}
			return
		}
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		mu.Lock()
		blocks = r.PostForm.Get("blocks")
		mu.Unlock()
		rw.Header().Set("Content-Type", "application/json")
		rw.Write([]byte(`{"ok":true,"channel":"C1","ts":"1.5"}`))
	}))
	t.Cleanup(srv.Close)
	w := &Web{c: newClient(testToken, slackgo.OptionAPIURL(srv.URL+"/")), token: testToken, base: srv.URL + "/", files: &http.Client{}, sleep: sleep}
	text := "see " + keyA + " and " + keyB
	start := time.Now()
	got, err := w.PostReply(context.Background(), "C1", "1.1", text)
	if took := time.Since(start); err != nil || got != "1.5" || took > 2*time.Second {
		t.Fatalf("PostReply = %q, %v after %v; want 1.5 well within 2s", got, err, took)
	}
	mu.Lock()
	defer mu.Unlock()
	if got := blockText(t, blocks); got != text {
		t.Fatalf("markdown block = %q, want the post as given, %q", got, text)
	}
}

// cardMarkdown returns the texts of the markdown blocks in blocks, the
// JSON of a message's blocks.
func cardMarkdown(t *testing.T, blocks string) []string {
	t.Helper()
	var bs []struct {
		Type string          `json:"type"`
		Text json.RawMessage `json:"text"`
	}
	if err := json.Unmarshal([]byte(blocks), &bs); err != nil {
		t.Fatalf("blocks %q: %v", blocks, err)
	}
	var md []string
	for _, b := range bs {
		if b.Type == "markdown" {
			var s string
			if err := json.Unmarshal(b.Text, &s); err != nil {
				t.Fatalf("markdown block %s: %v", b.Text, err)
			}
			md = append(md, s)
		}
	}
	return md
}

// cardSlack answers for the permalinks of keyA and keyB, or fails while
// fail is set, and takes every post and update; posts returns the blocks
// of each post and update and how many times the permalinks were asked.
func cardSlack(t *testing.T, linkA, linkB string, fail *bool) (*Web, func() ([]string, int), *testSlack) {
	w, ts, _ := newTestWeb(t, func(r request) (int, string) {
		switch r.method {
		case "chat.getPermalink":
			if *fail {
				return 500, `{"ok":false,"error":"internal_error"}`
			}
			switch r.query.Get("channel") + "/" + r.query.Get("message_ts") {
			case keyA:
				return 200, `{"ok":true,"permalink":"` + linkA + `"}`
			case keyB:
				return 200, `{"ok":true,"permalink":"` + linkB + `"}`
			}
			return 200, `{"ok":false,"error":"channel_not_found"}`
		case "chat.postMessage", "chat.update":
			return 200, `{"ok":true,"channel":"C1","ts":"1.5"}`
		}
		return 200, `{"ok":false,"error":"unknown_method"}`
	})
	return w, func() ([]string, int) {
		rs, _ := ts.got()
		var blocks []string
		asked := 0
		for _, r := range rs {
			switch r.method {
			case "chat.getPermalink":
				asked++
			case "chat.postMessage", "chat.update":
				blocks = append(blocks, r.form.Get("blocks"))
			}
		}
		return blocks, asked
	}, ts
}

// The thread keys in a question card's context and options show as
// links to their threads, but not those in code; the card drawn again,
// answered, shows the same links without asking Slack again, even when
// the permalinks by key are forgotten and Slack would give none by then.
// The plain text is the card's as it is.
func TestWebLinksThreadKeysOnCards(t *testing.T) {
	ctx := context.Background()
	linkA := "https://example.invalid/archives/C0FAKE0001/p1700000000000100"
	linkB := "https://example.invalid/archives/G0FAKE0002/p1700000001000200"
	fail := false
	w, got, ts := cardSlack(t, linkA, linkB, &fail)
	q := question.Question{
		Question: "合并哪一个？",
		Context:  "见 " + keyA + "，`" + keyA + "`\n```\n" + keyB + "\n```",
		Options: []question.Option{
			{Short: "先合 " + keyB},
			{Short: "都不合", Long: "等 " + keyA + " 和 " + keyB + " 都定了"},
		},
	}
	c := QuestionCard{ID: "QFAKE1", Question: q}
	if _, err := w.PostQuestion(ctx, "C1", "1.1", c); err != nil {
		t.Fatal(err)
	}
	ts.mu.Lock()
	fail = true
	ts.mu.Unlock()
	// The permalinks by key are forgotten, as when that cache is full.
	w.links.mu.Lock()
	w.links.m = nil
	w.links.mu.Unlock()
	c.State, c.Answer = question.Answered, question.Answer{By: "U0FAKE0001", Choice: &question.Choice{Index: 1}}
	if err := w.UpdateQuestion(ctx, "C1", "1.5", c); err != nil {
		t.Fatal(err)
	}

	blocks, asked := got()
	if asked != 2 {
		t.Fatalf("asked Slack for permalinks %d times, want 2, once for each key on the first draw", asked)
	}
	la, lb := "["+keyA+"]("+linkA+")", "["+keyB+"]("+linkB+")"
	want := []string{
		"见 " + la + "，`" + keyA + "`\n```\n" + keyB + "\n```",
		"**(a) 先合 " + lb + "**",
		"**(b) 都不合**\n等 " + la + " 和 " + lb + " 都定了",
	}
	if len(blocks) != 2 {
		t.Fatalf("%d draws, want 2", len(blocks))
	}
	for i, b := range blocks {
		if md := cardMarkdown(t, b); !slices.Equal(md, want) {
			t.Fatalf("markdown blocks of draw %d = %q\nwant %q", i+1, md, want)
		}
	}
	rs, _ := ts.got()
	for _, r := range rs {
		if r.method == "chat.postMessage" && r.form.Get("text") != questionText(c) {
			t.Fatalf("plain text = %q, want %q", r.form.Get("text"), questionText(c))
		}
	}
}

// A card goes out as written when Slack gives no permalinks, and is drawn
// again as written once Slack gives them; so it does when the links would
// take its markdown blocks over what a message takes.
func TestWebPostsCardsAsTheyAreWithoutLinks(t *testing.T) {
	ctx := context.Background()
	fail := true
	w, got, ts := cardSlack(t, "https://example.invalid/archives/p1", "https://example.invalid/archives/p2", &fail)
	q := question.Question{Question: "要不要？", Context: "见 " + keyA, Options: []question.Option{{Short: "要"}}}
	c := QuestionCard{ID: "QFAKE1", Question: q}
	if posted, err := w.PostQuestion(ctx, "C1", "1.1", c); err != nil || posted != "1.5" {
		t.Fatalf("PostQuestion = %q, %v; want 1.5", posted, err)
	}
	ts.mu.Lock()
	fail = false
	ts.mu.Unlock()
	c.State = QuestionWithdrawn
	if err := w.UpdateQuestion(ctx, "C1", "1.5", c); err != nil {
		t.Fatal(err)
	}

	long := question.Question{Question: "要不要？", Context: "见 " + keyB}
	for range question.MaxOptions {
		long.Options = append(long.Options, question.Option{Short: "s", Long: strings.Repeat("x", question.MaxLong)})
	}
	n := 0
	for _, s := range questionMarkdown(long) {
		n += utf8.RuneCountInString(s)
	}
	long.Options[4].Long = long.Options[4].Long[:question.MaxLong-(n-maxMarkdownChars)]
	if err := long.Check(); err != nil {
		t.Fatalf("the long card is not one an agent may ask: %v", err)
	}
	if _, err := w.PostQuestion(ctx, "C1", "1.1", QuestionCard{ID: "QFAKE2", Question: long}); err != nil {
		t.Fatal(err)
	}

	blocks, _ := got()
	if len(blocks) != 3 {
		t.Fatalf("%d draws, want 3", len(blocks))
	}
	for i, b := range blocks[:2] {
		if md := cardMarkdown(t, b); !slices.Equal(md, questionMarkdown(q)) {
			t.Fatalf("markdown blocks of draw %d = %q, want the card as written, %q", i+1, md, questionMarkdown(q))
		}
	}
	if md := cardMarkdown(t, blocks[2]); !slices.Equal(md, questionMarkdown(long)) {
		t.Fatalf("markdown blocks of the long card start %q, want the card as written", md[0])
	}
}
