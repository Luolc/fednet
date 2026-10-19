package slack

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
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
		{"[" + keyA + "](https://example.invalid/x) " + keyB, []string{keyB}},
		{"[thread](https://example.invalid/?t=" + keyA + ") " + keyB, []string{keyB}},
		{"<https://example.invalid/" + keyA + "|t> " + keyB, []string{keyB}},
		{"https://example.invalid/?t=" + keyA + " " + keyB, []string{keyB}},
		{"a/" + keyA, nil},
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
