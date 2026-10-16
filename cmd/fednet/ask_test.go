package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Luolc/fednet/internal/ask"
	"github.com/Luolc/fednet/internal/hook"
	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/question"
	"github.com/Luolc/fednet/internal/slack"
)

// answers returns the answers the hook has received so far.
func (rig *approvalRig) answers(t *testing.T) []question.Answer {
	t.Helper()
	b, err := os.ReadFile(rig.events)
	if err != nil {
		return nil
	}
	var as []question.Answer
	for line := range strings.SplitSeq(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		var ev hook.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatal(err)
		}
		var m payload.Message
		if err := json.Unmarshal(ev.Payload, &m); err != nil {
			t.Fatal(err)
		}
		if m.Type != payload.Answer {
			continue
		}
		var a question.Answer
		if err := json.Unmarshal(m.Answer, &a); err != nil {
			t.Fatal(err)
		}
		as = append(as, a)
	}
	return as
}

// End to end: an agent asks through the client, from standard input, and
// the card appears in the thread; a person's press comes to the hook as a
// message of type answer, through the same hook as every message; a card
// over the limits is refused before the socket, and the agent takes back
// a card that still waits, but not one answered.
func TestQuestionFlow(t *testing.T) {
	ctx := t.Context()
	rig := startApprovalRig(t, false)
	root, err := rig.f.Start("C9", "U1", "一个线程")
	if err != nil {
		t.Fatal(err)
	}
	thread := slack.ThreadKey("C9", root)
	askArgs := func(ref string) []string {
		return []string{"client", "ask", "-socket", rig.socket, "-thread", thread, "-ref", ref, "-card", "-", "-mention", "U1", "-json"}
	}
	stdin = strings.NewReader(`{"question": "放宽第 5 条吗？", "options": [{"short": "照原样"}, {"short": "放宽", "recommended": true}]}`)
	t.Cleanup(func() { stdin = os.Stdin })
	var out syncBuffer
	if code := run(ctx, askArgs("q-1"), &out, rig.stderr); code != 0 {
		t.Fatalf("client ask: exit %d", code)
	}
	var p ask.Posted
	if err := json.Unmarshal([]byte(out.String()), &p); err != nil || p.ID == "" || p.Channel != "C9" || p.TS == "" || p.Permalink == "" {
		t.Fatalf("client ask printed %q (%v), want the card's id, channel, ts and link", out.String(), err)
	}
	if c, ok := rig.f.Question("C9", p.TS); !ok || c.ID != p.ID || len(c.Mentions) != 1 {
		t.Fatalf("card at %s = %+v, %v; want it, mentioning U1", p.TS, c, ok)
	}

	if err := rig.sr.r.Questions.Click(ctx, slack.QuestionClick{ID: p.ID, Action: slack.QuestionSubmitAction, Choice: "1", Note: "可以", User: "U2", Channel: "C9", TS: p.TS}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the answer to reach the hook", func() bool { return len(rig.answers(t)) == 1 })
	if a := rig.answers(t)[0]; a.QuestionID != "q-1" || a.CardID != p.ID || a.Outcome != question.Answered || a.Choice == nil || a.Choice.Short != "放宽" || a.Note != "可以" || a.By != "U2" {
		t.Fatalf("answer = %+v, want (b) answered by U2 with the note", a)
	}
	waitFor(t, "the card to show the answer", func() bool { c, _ := rig.f.Question("C9", p.TS); return c.State == question.Answered })
	if code := run(ctx, []string{"client", "ask-close", "-socket", rig.socket, p.ID}, &out, rig.stderr); code != exitBadRequest {
		t.Fatalf("ask-close of an answered card: exit %d, want %d", code, exitBadRequest)
	}

	stdin = strings.NewReader(`{"question": "` + strings.Repeat("问", question.MaxQuestion+1) + `"}`)
	var errOut syncBuffer
	if code := run(ctx, askArgs("q-2"), &out, &errOut); code != exitBadRequest || !strings.Contains(errOut.String(), "question is 141 characters, at most 140") {
		t.Fatalf("client ask with a long question: exit %d, stderr %q; want %d and the field, length and limit", code, errOut.String(), exitBadRequest)
	}

	cardPath := filepath.Join(rig.dir, "card.json")
	if err := os.WriteFile(cardPath, []byte(`{"question": "还要吗？"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	out = syncBuffer{}
	if code := run(ctx, []string{"client", "ask", "-socket", rig.socket, "-thread", thread, "-ref", "q-3", "-card", cardPath}, &out, rig.stderr); code != 0 {
		t.Fatalf("client ask -card FILE: exit %d", code)
	}
	id := strings.TrimSpace(out.String())
	if code := run(ctx, []string{"client", "ask-close", "-socket", rig.socket, "-note", "不用问了", id}, &out, rig.stderr); code != 0 {
		t.Fatalf("ask-close: exit %d", code)
	}
	tss, cards := rig.f.Questions("C9")
	waitFor(t, "the card to show it was taken back", func() bool {
		c, _ := rig.f.Question("C9", tss[len(tss)-1])
		return c.ID == id && c.State == slack.QuestionWithdrawn && c.Note == "不用问了"
	})
	if len(cards) != 2 || len(rig.answers(t)) != 1 {
		t.Fatalf("%d cards and %d answers, want 2 and 1", len(cards), len(rig.answers(t)))
	}
	rig.stop(t)
}
