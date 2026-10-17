// Package question is the question card an agent asks a person in a
// Slack thread: the JSON the agent writes, checked against Slack's limits
// here and nowhere else, and the JSON the agent gets back each time a
// person acts on the card.
package question

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"unicode/utf8"
)

// Question is what the agent writes: the question, the context and the
// options. The card adds the "other" choice, the snooze buttons and the
// close button itself.
type Question struct {
	// Question is one sentence, the card's header and what a
	// notification shows.
	Question string `json:"question"`
	// Context is standard Markdown, shown under the question.
	Context string   `json:"context,omitempty"`
	Options []Option `json:"options,omitempty"`
}

// Option is one choice: Short on the radio button, which also comes back
// to the agent, Long, standard Markdown, written out above the buttons.
type Option struct {
	Recommended bool   `json:"recommended,omitempty"`
	Short       string `json:"short"`
	Long        string `json:"long,omitempty"`
}

// Limits of a Question, in characters. The header has 150 and the card
// puts "❓ " before the question; a radio button's text has 75 and the
// card puts "(a) " before Short and " ★推荐" after it; a section has 3000
// and the card puts a line with Short above Long. MaxMarkdown is what
// Slack takes in all the markdown blocks of one message, which hold
// Context and each option's Long with its line.
const (
	MaxQuestion = 140
	MaxContext  = 3000
	MaxOptions  = 5
	MaxShort    = 60
	MaxLong     = 2900
	MaxMarkdown = 12000
)

// Parse reads a Question from b, a JSON object with no fields but the
// Question's, and checks it: a field over its limit is an error that
// names the field, the limit and its length, and nothing is cut.
func Parse(b []byte) (Question, error) {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	var q Question
	if err := d.Decode(&q); err != nil {
		return Question{}, fmt.Errorf("the card is not a question: %w", err)
	}
	if _, err := d.Token(); err != io.EOF {
		return Question{}, errors.New("the card is not a question: more after the JSON object")
	}
	return q, q.Check()
}

// Check reports the first way q breaks the limits.
func (q Question) Check() error {
	over := func(field, s string, max int) error {
		if n := utf8.RuneCountInString(s); n > max {
			return fmt.Errorf("%s is %d characters, at most %d", field, n, max)
		}
		return nil
	}
	if q.Question == "" {
		return errors.New("question is empty")
	}
	if err := over("question", q.Question, MaxQuestion); err != nil {
		return err
	}
	if err := over("context", q.Context, MaxContext); err != nil {
		return err
	}
	if n := len(q.Options); n > MaxOptions {
		return fmt.Errorf("options has %d options, at most %d", n, MaxOptions)
	}
	recommended := 0
	for i, o := range q.Options {
		if o.Short == "" {
			return fmt.Errorf("options[%d].short is empty", i)
		}
		if err := over(fmt.Sprintf("options[%d].short", i), o.Short, MaxShort); err != nil {
			return err
		}
		if m := ownLabel.FindStringSubmatch(o.Short); m != nil {
			return fmt.Errorf("options[%d].short starts with its own label %q; the card labels the options (a), (b), (c) itself", i, strings.Join(m[1:], ""))
		}
		if err := over(fmt.Sprintf("options[%d].long", i), o.Long, MaxLong); err != nil {
			return err
		}
		if o.Recommended {
			recommended++
		}
	}
	if recommended > 1 {
		return fmt.Errorf("options has %d recommended options, at most 1", recommended)
	}
	n := utf8.RuneCountInString(q.Context)
	for i := range q.Options {
		n += utf8.RuneCountInString(q.OptionMarkdown(i))
	}
	if n > MaxMarkdown {
		return fmt.Errorf("context and the options' long take %d characters on the card, Slack shows at most %d in one message", n, MaxMarkdown)
	}
	return nil
}

// ownLabel matches a label an agent put before an option's text, which
// the card's own (a), (b) would then contradict: one letter, digit or
// Chinese numeral, bracketed, followed by a separator, or after 方案, 选项
// or Option. It stays narrow so that "A 股", "B 端", "API 改造", "2 小时后"
// and "1.5 倍" pass: a separator followed by a letter or digit, as in
// "2.5" or "A:B", is not a label. Of its three groups, the one that
// matched holds the label.
var ownLabel = func() *regexp.Regexp {
	const (
		label  = `[A-Za-zＡ-Ｚａ-ｚ0-9０-９一二三四五六七八九十]`
		prefix = `(?:方案|选项|(?i:option))\s*`
	)
	return regexp.MustCompile(`^(?:([(（\[【]` + label + `[)）\]】])` +
		`|((?:` + prefix + `)?` + label + `[：:.．、)）])(?:$|[^A-Za-z0-9])` +
		`|(` + prefix + label + `)(?:$|\s))`)
}()

// Letter is the label of option i: a, b, c and so on.
func Letter(i int) string { return string(rune('a' + i)) }

// OptionLabel is option i's text on its radio button.
func (q Question) OptionLabel(i int) string {
	s := "(" + Letter(i) + ") " + q.Options[i].Short
	if q.Options[i].Recommended {
		s += " ★推荐"
	}
	return s
}

// OptionMarkdown is option i written out above the radio buttons: a line
// with its label, then Long.
func (q Question) OptionMarkdown(i int) string {
	o := q.Options[i]
	s := "**(" + Letter(i) + ") " + o.Short + "**"
	if o.Recommended {
		s += "　★ 推荐"
	}
	if o.Long != "" {
		s += "\n" + o.Long
	}
	return s
}

// Recommended returns the index of the recommended option, or -1.
func (q Question) Recommended() int {
	for i, o := range q.Options {
		if o.Recommended {
			return i
		}
	}
	return -1
}

// Outcomes of an Answer.
const (
	// Answered is a press on submit: Choice is the option chosen, nil for
	// "other", which then comes with a Note.
	Answered = "answered"
	// Snoozed is a press on a snooze button, or a time picked: the card
	// comes back at SnoozeUntil.
	Snoozed = "snoozed"
	// Closed is a press on close, confirmed: the person thinks the
	// question needs no answer.
	Closed = "closed"
	// Resurfaced is a snoozed card posted again at the bottom of its
	// thread, waiting for a press again.
	Resurfaced = "resurfaced"
)

// Answer is what the agent gets each time a person acts on a card, or a
// snoozed card comes back, and what the card shows at its bottom once
// acted on. QuestionID is the ref the agent asked with; CardID the id the
// hub gave the card.
type Answer struct {
	QuestionID  string  `json:"question_id"`
	CardID      string  `json:"card_id"`
	Outcome     string  `json:"outcome"`
	Choice      *Choice `json:"choice"`
	Note        string  `json:"note,omitempty"`
	SnoozeUntil string  `json:"snooze_until,omitempty"`
	By          string  `json:"by,omitempty"`
	At          string  `json:"at"`
}

// Choice is the option chosen: its index, from 0, and its Short.
type Choice struct {
	Index int    `json:"index"`
	Short string `json:"short"`
}

// MarshalJSON writes a, without choice on a Resurfaced, which has none to
// tell. HTML is not escaped: the JSON is shown on the card as it is.
func (a Answer) MarshalJSON() ([]byte, error) {
	type plain Answer
	var v any = plain(a)
	if a.Outcome == Resurfaced {
		v = struct {
			QuestionID string `json:"question_id"`
			CardID     string `json:"card_id"`
			Outcome    string `json:"outcome"`
			At         string `json:"at"`
		}{a.QuestionID, a.CardID, a.Outcome, a.At}
	}
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	if err := e.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(b.Bytes(), []byte("\n")), nil
}
