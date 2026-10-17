package question

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// sample is the example question of the schema, as an agent would write
// it.
const sample = `{
  "question": "线程里直接打字回复，算不算答了卡片上的问题？",
  "context": "问题卡片上线以后，你既可以点卡片作答，也可以直接在线程里打字。",
  "options": [
    {"recommended": true, "short": "不自动算，由 agent 判断", "long": "卡片留着不动。"},
    {"short": "自动算，有人说话就算答了", "long": "跟现在的纯文本问题一样。"},
    {"short": "只有 @ 了 bot 的回复才算"}
  ]
}`

func TestParse(t *testing.T) {
	q, err := Parse([]byte(sample))
	if err != nil {
		t.Fatalf("Parse(sample) = %v, want nil", err)
	}
	if len(q.Options) != 3 || q.Recommended() != 0 || q.OptionLabel(0) != "(a) 不自动算，由 agent 判断 ★推荐" || q.OptionLabel(2) != "(c) 只有 @ 了 bot 的回复才算" {
		t.Fatalf("Parse(sample) = %+v, want three options, (a) recommended", q)
	}
	if _, err := Parse([]byte(`{"question": "开放问题？"}`)); err != nil {
		t.Fatalf("a question with no options: %v, want nil", err)
	}

	// Each limit holds at n characters and fails at n+1, the error naming
	// the field, the limit and the length; a CJK character counts one.
	cards := map[string]func(n int) string{
		"question": func(n int) string { return fmt.Sprintf(`{"question": %q}`, strings.Repeat("问", n)) },
		"context":  func(n int) string { return fmt.Sprintf(`{"question": "q", "context": %q}`, strings.Repeat("问", n)) },
		"options[1].short": func(n int) string {
			return fmt.Sprintf(`{"question": "q", "options": [{"short": "a"}, {"short": %q}]}`, strings.Repeat("问", n))
		},
		"options[0].long": func(n int) string {
			return fmt.Sprintf(`{"question": "q", "options": [{"short": "a", "long": %q}]}`, strings.Repeat("问", n))
		},
	}
	limits := map[string]int{"question": MaxQuestion, "context": MaxContext, "options[1].short": MaxShort, "options[0].long": MaxLong}
	for field, card := range cards {
		n := limits[field]
		if _, err := Parse([]byte(card(n))); err != nil {
			t.Errorf("%s at %d characters: %v, want nil", field, n, err)
		}
		_, err := Parse([]byte(card(n + 1)))
		if want := fmt.Sprintf("%s is %d characters, at most %d", field, n+1, n); err == nil || err.Error() != want {
			t.Errorf("%s at %d characters: %v, want %q", field, n+1, err, want)
		}
	}

	// The total of the markdown blocks holds at 12000 and fails at 12001:
	// the context, and four options of "**(a) x**", a newline and Long.
	total := func(extra int) string {
		var opts []string
		for i := range 4 {
			n := 2240
			if i == 0 {
				n += extra
			}
			opts = append(opts, fmt.Sprintf(`{"short": "x", "long": %q}`, strings.Repeat("问", n)))
		}
		return fmt.Sprintf(`{"question": "q", "context": %q, "options": [%s]}`, strings.Repeat("问", MaxContext), strings.Join(opts, ","))
	}
	if _, err := Parse([]byte(total(0))); err != nil {
		t.Errorf("markdown at %d characters: %v, want nil", MaxMarkdown, err)
	}
	if _, err := Parse([]byte(total(1))); err == nil || err.Error() != "context and the options' long take 12001 characters on the card, Slack shows at most 12000 in one message" {
		t.Errorf("markdown at %d characters: %v, want it refused", MaxMarkdown+1, err)
	}

	var six []string
	for range 6 {
		six = append(six, `{"short": "x"}`)
	}
	long := strings.Repeat("问", MaxLong)
	var fat []string
	for range 5 {
		fat = append(fat, fmt.Sprintf(`{"short": "x", "long": %q}`, long))
	}
	for card, want := range map[string]string{
		`{"question": ""}`: "question is empty",
		`{"question": "q", "options": [` + strings.Join(six, ",") + `]}`:                                           "options has 6 options, at most 5",
		`{"question": "q", "options": [{"short": ""}]}`:                                                            "options[0].short is empty",
		`{"question": "q", "options": [{"short": "a", "recommended": true}, {"short": "b", "recommended": true}]}`: "options has 2 recommended options, at most 1",
		`{"question": "q", "options": [` + strings.Join(fat, ",") + `]}`:                                           "context and the options' long take 14550 characters on the card, Slack shows at most 12000 in one message",
		`{"question": "q", "answer": "a"}`:                                                                         "the card is not a question: json: unknown field \"answer\"",
		`{"question": "q"} {}`:                                                                                     "the card is not a question: more after the JSON object",
	} {
		if _, err := Parse([]byte(card)); err == nil || err.Error() != want {
			t.Errorf("Parse(%.60s) = %v, want %q", card, err, want)
		}
	}
}

// A short that starts with a label of its own is refused, naming the
// field and the label: the card puts (a), (b) before it, and "(a) B：..."
// reads as two labels that disagree. Text that merely starts with a
// letter or a number passes.
func TestOwnLabel(t *testing.T) {
	card := func(short string) string {
		return fmt.Sprintf(`{"question": "q", "options": [{"short": "x"}, {"short": %q}]}`, short)
	}
	for short, label := range map[string]string{
		"B：加 thresholds 命令": "B：",
		"A: hand-edit once": "A:",
		"b. 手工改表一次":         "b.",
		"1) 先回滚":            "1)",
		" B：开头带空白":          "B：",
		"二、保持现状":            "二、",
		"Ｃ）全角":              "Ｃ）",
		"(a) 重复编号":          "(a)",
		"【A】加命令":            "【A】",
		"方案一：加命令":           "方案一：",
		"选项A 手工改":           "选项A",
		"Option B: keep it": "Option B:",
	} {
		_, err := Parse([]byte(card(short)))
		want := fmt.Sprintf("options[1].short starts with its own label %q; the card labels the options (a), (b), (c) itself", label)
		if err == nil || err.Error() != want {
			t.Errorf("short %q: %v, want %q", short, err, want)
		}
	}
	for _, short := range []string{"A 股", "B 端先上", "API 改造", "2 小时后再看", "1.5 倍超时", "A/B 测试", "3:00 之前", "方案十分可行", "一次性迁移", "Optional cleanup", "Options 都保留", "Options: keep all", "A、B 两端都改"} {
		if _, err := Parse([]byte(card(short))); err != nil {
			t.Errorf("short %q: %v, want nil", short, err)
		}
	}
}

// The JSON the agent gets: choice is null for "other", and absent with
// note and by on a resurfaced card; what a person wrote is not escaped.
func TestAnswerJSON(t *testing.T) {
	for _, c := range []struct {
		a    Answer
		want string
	}{
		{Answer{QuestionID: "q-12", CardID: "C1", Outcome: Answered, Choice: &Choice{Index: 0, Short: "a"}, Note: "<b> & c", By: "U1", At: "2026-10-16T01:01:04-07:00"},
			`{"question_id":"q-12","card_id":"C1","outcome":"answered","choice":{"index":0,"short":"a"},"note":"<b> & c","by":"U1","at":"2026-10-16T01:01:04-07:00"}`},
		{Answer{QuestionID: "q-12", CardID: "C1", Outcome: Answered, Note: "都不对", By: "U1", At: "t"},
			`{"question_id":"q-12","card_id":"C1","outcome":"answered","choice":null,"note":"都不对","by":"U1","at":"t"}`},
		{Answer{QuestionID: "q-12", CardID: "C1", Outcome: Snoozed, SnoozeUntil: "u", By: "U1", At: "t"},
			`{"question_id":"q-12","card_id":"C1","outcome":"snoozed","choice":null,"snooze_until":"u","by":"U1","at":"t"}`},
		{Answer{QuestionID: "q-12", CardID: "C1", Outcome: Resurfaced, At: "t"},
			`{"question_id":"q-12","card_id":"C1","outcome":"resurfaced","at":"t"}`},
	} {
		b, err := c.a.MarshalJSON()
		if err != nil || string(b) != c.want {
			t.Errorf("MarshalJSON = %s, %v; want %s", b, err, c.want)
		}
		var back Answer
		if err := json.Unmarshal(b, &back); err != nil || back.Outcome != c.a.Outcome {
			t.Errorf("Unmarshal(%s) = %+v, %v; want it back", b, back, err)
		}
	}
}
