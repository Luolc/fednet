package slack

import (
	"context"
	"encoding/json"
	"testing"
)

// A progress card is one plan block: the title, then each item as a task
// in Slack's status for its state; every layout has a new block id, and
// the plain text names the title. An update replaces the message at ts.
func TestWebProgress(t *testing.T) {
	ctx := context.Background()
	w, ts, _ := newTestWeb(t, func(r request) (int, string) {
		return 200, `{"ok":true,"channel":"C1","ts":"1.5"}`
	})
	p := Progress{Title: "running <tests>", Items: []ProgressItem{{"build", Done}, {"tests", Doing}, {"lint", Failed}}}
	if got, err := w.PostProgress(ctx, "C1", "1.1", p); err != nil || got != "1.5" {
		t.Fatalf("PostProgress = %q, %v; want 1.5", got, err)
	}
	if err := w.UpdateProgress(ctx, "C1", "1.5", Progress{Title: "empty"}); err != nil {
		t.Fatal(err)
	}
	rs, _ := ts.got()
	post, update := rs[0], rs[1]
	if post.method != "chat.postMessage" || post.form.Get("thread_ts") != "1.1" || post.form.Get("text") != "进度：running &lt;tests&gt;" {
		t.Fatalf("PostProgress sent %s %v", post.method, post.form)
	}
	if update.method != "chat.update" || update.form.Get("ts") != "1.5" || update.form.Get("text") != "进度：empty" {
		t.Fatalf("UpdateProgress sent %s %v", update.method, update.form)
	}
	type task struct {
		Type   string `json:"type"`
		TaskID string `json:"task_id"`
		Title  string `json:"title"`
		Status string `json:"status"`
	}
	type plan struct {
		Type    string  `json:"type"`
		BlockID string  `json:"block_id"`
		Title   string  `json:"title"`
		Tasks   []*task `json:"tasks"`
	}
	read := func(form string) plan {
		var bs []plan
		if err := json.Unmarshal([]byte(form), &bs); err != nil || len(bs) != 1 {
			t.Fatalf("blocks %s: %v, want one", form, err)
		}
		return bs[0]
	}
	got := read(post.form.Get("blocks"))
	want := []task{{"task_card", "t1", "build", "complete"}, {"task_card", "t2", "tests", "in_progress"}, {"task_card", "t3", "lint", "error"}}
	if got.Type != "plan" || got.Title != "running <tests>" || len(got.Tasks) != 3 {
		t.Fatalf("card = %s", post.form.Get("blocks"))
	}
	for i, tk := range got.Tasks {
		if *tk != want[i] {
			t.Fatalf("task %d = %+v, want %+v", i, *tk, want[i])
		}
	}
	empty := read(update.form.Get("blocks"))
	if empty.Tasks == nil || len(empty.Tasks) != 0 || empty.BlockID == got.BlockID || empty.BlockID == "" {
		t.Fatalf("updated card = %s, want an empty task list and a new block id (was %s)", update.form.Get("blocks"), got.BlockID)
	}
}

// A footer is one context block holding the text as mrkdwn, its links
// written <url|text> and the rest escaped; the plain text has each link's
// text alone.
func TestWebFooter(t *testing.T) {
	ctx := context.Background()
	w, ts, _ := newTestWeb(t, func(r request) (int, string) {
		return 200, `{"ok":true,"channel":"C1","ts":"1.7"}`
	})
	if _, err := w.PostFooter(ctx, "C1", "1.1", "会话已结束 · [T-1](https://example.invalid/t?a=1&b=2) <x>"); err != nil {
		t.Fatal(err)
	}
	rs, _ := ts.got()
	r := rs[0]
	if r.method != "chat.postMessage" || r.form.Get("thread_ts") != "1.1" || r.form.Get("text") != "会话已结束 · T-1 &lt;x&gt;" {
		t.Fatalf("PostFooter sent %s %v", r.method, r.form)
	}
	var bs []struct {
		Type     string `json:"type"`
		Elements []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"elements"`
	}
	if err := json.Unmarshal([]byte(r.form.Get("blocks")), &bs); err != nil {
		t.Fatal(err)
	}
	want := "会话已结束 · <https://example.invalid/t?a=1&amp;b=2|T-1> &lt;x&gt;"
	if len(bs) != 1 || bs[0].Type != "context" || len(bs[0].Elements) != 1 || bs[0].Elements[0].Type != "mrkdwn" || bs[0].Elements[0].Text != want {
		t.Fatalf("blocks = %s, want one context block with %q", r.form.Get("blocks"), want)
	}
}
