package slack

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	slackgo "github.com/slack-go/slack"

	"github.com/Luolc/fednet/internal/question"
)

var testQuestion = question.Question{
	Question: "要不要放宽第 5 条？",
	Context:  "背景 **加粗**",
	Options: []question.Option{
		{Short: "照原样"},
		{Short: "放宽", Long: "放宽的说明", Recommended: true},
	},
}

// A card that waits mentions the people, writes out the question, the
// context and every option, has the radio buttons with "other" last and
// the recommended option selected, the note box, submit, the snooze
// buttons and close, each in a block named after the card; one that ended
// has no buttons and shows the answer the agent got as it is.
func TestQuestionBlocks(t *testing.T) {
	c := QuestionCard{ID: "Q1", Question: testQuestion, Mentions: []string{"U1"}}
	bs := questionBlocks(c)
	var md []string
	actions := map[string]*slackgo.ActionBlock{}
	var note *slackgo.InputBlock
	for _, b := range bs {
		switch b := b.(type) {
		case *slackgo.MarkdownBlock:
			md = append(md, b.Text)
		case *slackgo.ActionBlock:
			actions[b.BlockID] = b
		case *slackgo.InputBlock:
			note = b
		}
	}
	if rt, ok := bs[0].(*slackgo.RichTextBlock); !ok || !strings.Contains(mustJSON(t, rt), `"user_id":"U1"`) {
		t.Fatalf("first block = %s, want the mention of U1", mustJSON(t, bs[0]))
	}
	if h, ok := bs[1].(*slackgo.HeaderBlock); !ok || h.Text.Text != "❓ 要不要放宽第 5 条？" {
		t.Fatalf("second block = %s, want the question as the header", mustJSON(t, bs[1]))
	}
	if want := []string{"背景 **加粗**", "**(a) 照原样**", "**(b) 放宽**　★ 推荐\n放宽的说明"}; strings.Join(md, "|") != strings.Join(want, "|") {
		t.Fatalf("markdown blocks = %q, want %q", md, want)
	}
	pick := actions[QuestionBlockID("Q1", QuestionPickPart)]
	if pick == nil {
		t.Fatalf("no radio buttons in %s", QuestionBlockID("Q1", QuestionPickPart))
	}
	radio := pick.Elements.ElementSet[0].(*slackgo.RadioButtonsBlockElement)
	var values []string
	for _, o := range radio.Options {
		values = append(values, o.Value+"="+o.Text.Text)
	}
	if strings.Join(values, ",") != "0=(a) 照原样,1=(b) 放宽 ★推荐,other=其他" || radio.InitialOption == nil || radio.InitialOption.Value != "1" {
		t.Fatalf("radio buttons %q, initial %+v; want the options, other last, (b) selected", values, radio.InitialOption)
	}
	if note == nil || note.BlockID != QuestionBlockID("Q1", QuestionNotePart) || !note.Optional {
		t.Fatalf("note box = %+v, want an optional input block", note)
	}
	var buttons []string
	for _, part := range []string{QuestionSubmitPart, QuestionLaterPart} {
		a := actions[QuestionBlockID("Q1", part)]
		if a == nil {
			t.Fatalf("no block %s", QuestionBlockID("Q1", part))
		}
		for _, e := range a.Elements.ElementSet {
			b := e.(*slackgo.ButtonBlockElement)
			if b.Value != "Q1" {
				t.Errorf("button %s carries %q, want Q1", b.ActionID, b.Value)
			}
			buttons = append(buttons, b.ActionID)
			if b.ActionID == QuestionCloseAction && b.Confirm == nil {
				t.Errorf("close has no confirmation")
			}
		}
	}
	if want := "ask-submit,ask-snooze-30m,ask-snooze-2h,ask-snooze-24h,ask-snooze-pick,ask-close"; strings.Join(buttons, ",") != want {
		t.Fatalf("buttons = %s, want %s", strings.Join(buttons, ","), want)
	}

	// The option chosen goes in plain text: what the agent wrote is never
	// mrkdwn.
	chosen := c
	chosen.Question.Options = []question.Option{{Short: "*粗* <!channel>"}}
	chosen.Mentions, chosen.State = nil, question.Answered
	chosen.Answer = question.Answer{Outcome: question.Answered, Choice: &question.Choice{Index: 0, Short: "*粗* <!channel>"}, By: "U1"}
	var plainTexts []string
	for _, b := range questionBlocks(chosen) {
		if sec, ok := b.(*slackgo.SectionBlock); ok && sec.Text != nil {
			if sec.Text.Type == slackgo.PlainTextType {
				plainTexts = append(plainTexts, sec.Text.Text)
			} else if strings.Contains(sec.Text.Text, "粗") {
				t.Fatalf("the option chosen is in mrkdwn: %q", sec.Text.Text)
			}
		}
	}
	if !strings.Contains(strings.Join(plainTexts, "|"), "(a) *粗* <!channel>") {
		t.Fatalf("plain text sections = %q, want the option chosen as it is", plainTexts)
	}

	c.Mentions, c.State = nil, question.Answered
	c.Answer = question.Answer{QuestionID: "r1", CardID: "Q1", Outcome: question.Answered, Note: "`*x*` <y>", By: "U1", At: "t"}
	got := mustJSON(t, questionBlocks(c))
	want, _ := c.Answer.MarshalJSON()
	// The answer as a JSON string inside the blocks, without its quotes.
	inner := mustJSON(t, string(want))
	inner = inner[1 : len(inner)-1]
	if strings.Contains(got, `"action_id"`) || !strings.Contains(got, `"type":"rich_text_preformatted"`) || !strings.Contains(got, inner) || !strings.Contains(got, "选了「其他」") {
		t.Fatalf("answered card = %s, want no buttons and the answer %s as it is", got, want)
	}
	c.State = QuestionMoved
	if got := mustJSON(t, questionBlocks(c)); strings.Contains(got, `"action_id"`) || !strings.Contains(got, "卡片重发在下方") {
		t.Fatalf("moved card = %s, want no buttons and where it went", got)
	}
}

// A warning goes above submit; the other blocks keep their ids, so that
// what a person picked and typed stays on their screen.
func TestQuestionWarning(t *testing.T) {
	ids := func(bs []slackgo.Block) (ids []string) {
		for _, b := range bs {
			if sec, ok := b.(*slackgo.SectionBlock); ok {
				ids = append(ids, "section:"+sec.Text.Text)
				continue
			}
			ids = append(ids, string(b.BlockType())+":"+b.ID())
		}
		return ids
	}
	c := QuestionCard{ID: "Q1", Question: testQuestion}
	plain := ids(questionBlocks(c))
	c.Warning = "⚠️ 请先选一项"
	warned := ids(questionBlocks(c))
	submit := slices.Index(plain, "actions:"+QuestionBlockID("Q1", QuestionSubmitPart))
	want := slices.Insert(slices.Clone(plain), submit, "section:*⚠️ 请先选一项*")
	if strings.Join(warned, ",") != strings.Join(want, ",") {
		t.Fatalf("blocks with a warning = %q, want %q", warned, want)
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// The time picker carries the card to the submission, and the time
// picked comes back out of the submitted state.
func TestPicker(t *testing.T) {
	st := PickerState{ID: "Q1", Channel: "C1", TS: "1.2", Note: "晚点"}
	v, err := pickerView(st, time.Unix(1_760_000_000, 0))
	if err != nil {
		t.Fatal(err)
	}
	var back PickerState
	if v.CallbackID != QuestionTimePicker || json.Unmarshal([]byte(v.PrivateMetadata), &back) != nil || back != st {
		t.Fatalf("view = %+v, want callback %s carrying %+v", v, QuestionTimePicker, st)
	}
	values := map[string]map[string]slackgo.BlockAction{questionAtAction: {questionAtAction: {SelectedDateTime: 1_760_003_600}}}
	if at, ok := PickedTime(values); !ok || at.Unix() != 1_760_003_600 {
		t.Fatalf("PickedTime = %v, %v; want the time picked", at, ok)
	}
	if _, ok := PickedTime(nil); ok {
		t.Fatal("PickedTime with nothing picked: ok")
	}
}

func TestWebQuestions(t *testing.T) {
	ctx := context.Background()
	w, ts, _ := newTestWeb(t, func(r request) (int, string) {
		switch r.method {
		case "chat.postMessage", "chat.update":
			return 200, `{"ok":true,"channel":"C1","ts":"1.5"}`
		case "chat.getPermalink":
			return 200, `{"ok":true,"channel":"C1","permalink":"https://slack.example.invalid/archives/C1/p15"}`
		case "views.open":
			return 200, `{"ok":true,"view":{"id":"V1"}}`
		case "chat.postEphemeral":
			return 200, `{"ok":true,"message_ts":"1.6"}`
		}
		return 200, `{"ok":false,"error":"unknown_method"}`
	})
	c := QuestionCard{ID: "Q1", Question: testQuestion, Mentions: []string{"U1"}}
	if got, err := w.PostQuestion(ctx, "C1", "1.1", c); err != nil || got != "1.5" {
		t.Fatalf("PostQuestion = %q, %v; want 1.5", got, err)
	}
	c.State, c.Answer = question.Closed, question.Answer{Outcome: question.Closed, By: "U1"}
	if err := w.UpdateQuestion(ctx, "C1", "1.5", c); err != nil {
		t.Fatal(err)
	}
	if got, err := w.Permalink(ctx, "C1", "1.5"); err != nil || got != "https://slack.example.invalid/archives/C1/p15" {
		t.Fatalf("Permalink = %q, %v", got, err)
	}
	if err := w.PickTime(ctx, "T1", PickerState{ID: "Q1", Channel: "C1", TS: "1.5"}); err != nil {
		t.Fatal(err)
	}
	if err := w.WhisperInThread(ctx, "C1", "1.1", "U2", "请先写补充"); err != nil {
		t.Fatal(err)
	}
	rs, _ := ts.got()
	if len(rs) != 5 {
		t.Fatalf("%d requests, want 5", len(rs))
	}
	post, update, link, pick, whisper := rs[0], rs[1], rs[2], rs[3], rs[4]
	if post.method != "chat.postMessage" || post.form.Get("thread_ts") != "1.1" || post.form.Get("text") != "<@U1> ❓ 要不要放宽第 5 条？" || !strings.Contains(post.form.Get("blocks"), `"action_id":"ask-submit"`) {
		t.Errorf("PostQuestion sent %s %v, want a reply in 1.1 with the buttons", post.method, post.form)
	}
	if update.method != "chat.update" || update.form.Get("ts") != "1.5" || strings.Contains(update.form.Get("blocks"), `"action_id"`) || !strings.Contains(update.form.Get("blocks"), "关闭了这个问题") {
		t.Errorf("UpdateQuestion sent %s %v, want an update of 1.5 without buttons", update.method, update.form)
	}
	if link.method != "chat.getPermalink" || link.query.Get("message_ts") != "1.5" {
		t.Errorf("Permalink sent %s %v", link.method, link.form)
	}
	if pick.method != "views.open" || !strings.Contains(pick.body, `"trigger_id":"T1"`) || !strings.Contains(pick.body, QuestionTimePicker) {
		t.Errorf("PickTime sent %s %v, want views.open with the picker", pick.method, pick.form)
	}
	if whisper.method != "chat.postEphemeral" || whisper.form.Get("thread_ts") != "1.1" || whisper.form.Get("user") != "U2" {
		t.Errorf("WhisperInThread sent %s %v, want an ephemeral message to U2 in thread 1.1", whisper.method, whisper.form)
	}
}
