package slack

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// threadKeyPattern is a thread key as Slack's ids make one: a channel,
// group or DM id, upper-case letters and digits after its C, G or D, and
// a ts of ten digits, a dot and six digits.
var threadKeyPattern = regexp.MustCompile(`^[CGD][A-Z0-9]{8,14}/[0-9]{10}\.[0-9]{6}`)

var (
	// linkAt matches a Markdown link, [text](url), at the start.
	linkAt = regexp.MustCompile(`^\[[^\[\]\n]*\]\([^()\s]*\)`)
	// angleAt matches an autolink or a mrkdwn link, <url> or <url|text>,
	// at the start.
	angleAt = regexp.MustCompile(`^<(?:https?://|mailto:)[^<>\n]*>`)
	// urlAt matches a bare URL at the start.
	urlAt = regexp.MustCompile(`^https?://\S*`)
)

// maxLinks is the most thread keys looked up for one text.
const maxLinks = 10

// linkWait bounds the lookups for one text, so a post is not held up long
// when Slack is slow or rate-limits; tests shorten it.
var linkWait = 5 * time.Second

// maxMarkdownChars is the most a markdown block takes, in characters.
const maxMarkdownChars = 12000

// maxCachedLinks bounds the permalinks a Web keeps; the cache is emptied
// when it is full.
const maxCachedLinks = 4096

// threadKeys returns where text has thread keys a person reads as text,
// each as its start and end byte offset, in order. Keys in a fenced code
// block, in a code span, in a link, its text or its URL, or in a bare URL
// are not, nor are keys right after or before a letter, a digit, "_",
// "/", or after "." or "-", which are parts of something longer, such
// as a path, nor keys right after "<", which Slack's mrkdwn reads as
// the start of a link or mention. Indented code blocks are not told
// apart from text.
func threadKeys(text string) [][2]int {
	var spans [][2]int
	var fence string
	at := 0
	for _, line := range strings.SplitAfter(text, "\n") {
		start := at
		at += len(line)
		if f := fenceOf(line); fence == "" && f != "" {
			fence = f
			continue
		} else if fence != "" {
			if f != "" && f[0] == fence[0] && len(f) >= len(fence) && strings.TrimSpace(fenceInfo(line, f)) == "" {
				fence = ""
			}
			continue
		}
		for i := 0; i < len(line); {
			rest := line[i:]
			switch c := line[i]; {
			case c == '`':
				i += codeSpan(rest)
				continue
			case c == '[':
				if m := linkAt.FindString(rest); m != "" {
					i += len(m)
					continue
				}
			case c == '<':
				if m := angleAt.FindString(rest); m != "" {
					i += len(m)
					continue
				}
			case c == 'h' && (i == 0 || !wordByte(line[i-1])):
				if m := urlAt.FindString(rest); m != "" {
					i += len(m)
					continue
				}
			case c == 'C' || c == 'G' || c == 'D':
				if m := threadKeyPattern.FindString(rest); m != "" && keyBefore(line, i) && keyAfter(line[i+len(m):]) {
					spans = append(spans, [2]int{start + i, start + i + len(m)})
					i += len(m)
					continue
				}
			}
			i++
		}
	}
	return spans
}

// fenceOf returns the run of backticks or tildes that opens or closes a
// fenced code block when line is such a fence, in a block quote or not,
// and "" when it is not.
func fenceOf(line string) string {
	u := unquote(line)
	t := strings.TrimLeft(u, " ")
	if len(u)-len(t) > 3 || len(t) < 3 || (t[0] != '`' && t[0] != '~') {
		return ""
	}
	n := len(t) - len(strings.TrimLeft(t, t[:1]))
	if n < 3 {
		return ""
	}
	return t[:n]
}

// unquote returns line without the block quote markers it starts with.
func unquote(line string) string {
	for {
		t := strings.TrimLeft(line, " ")
		if !strings.HasPrefix(t, ">") {
			return line
		}
		line = t[1:]
	}
}

// fenceInfo returns what follows f, the fence line opens or closes with.
func fenceInfo(line, f string) string {
	return strings.TrimLeft(unquote(line), " ")[len(f):]
}

// codeSpan returns how many bytes of s, which starts with a backtick, a
// code span takes, or the length of the run of backticks when it opens
// none: a span ends at the next run of exactly as many backticks.
func codeSpan(s string) int {
	n := len(s) - len(strings.TrimLeft(s, "`"))
	open := s[:n]
	for i := n; ; {
		j := strings.Index(s[i:], open)
		if j < 0 {
			break
		}
		j += i
		run := len(s[j:]) - len(strings.TrimLeft(s[j:], "`"))
		if run == n {
			return j + n
		}
		i = j + run
	}
	return n
}

func wordByte(b byte) bool {
	return b == '_' || b >= '0' && b <= '9' || b >= 'A' && b <= 'Z' || b >= 'a' && b <= 'z'
}

// keyBefore reports whether a key may start at line[i].
func keyBefore(line string, i int) bool {
	if i == 0 {
		return true
	}
	b := line[i-1]
	return !wordByte(b) && b != '/' && b != '.' && b != '-' && b != '<'
}

// keyAfter reports whether a key may end where rest starts.
func keyAfter(rest string) bool {
	if rest == "" {
		return true
	}
	b := rest[0]
	if wordByte(b) || b == '/' {
		return false
	}
	return !(b == '.' || b == '-') || len(rest) == 1 || !wordByte(rest[1])
}

// linkThreadKeys returns text with each thread key a person reads as text
// written as a Markdown link to the thread, [key](permalink), where Slack
// gives its permalink in time; each key it gives none for stays as it is,
// and so does all of text when the links would take it over what a
// markdown block takes.
func (w *Web) linkThreadKeys(ctx context.Context, text string) string {
	spans := threadKeys(text)
	if len(spans) == 0 {
		return text
	}
	ctx, cancel := context.WithTimeout(ctx, linkWait)
	defer cancel()
	found := make(map[string]string)
	for _, s := range spans {
		key := text[s[0]:s[1]]
		if _, tried := found[key]; tried || len(found) == maxLinks {
			continue
		}
		found[key] = w.threadLink(ctx, key)
	}
	var b strings.Builder
	last := 0
	for _, s := range spans {
		key := text[s[0]:s[1]]
		if found[key] == "" {
			continue
		}
		b.WriteString(text[last:s[0]])
		b.WriteString("[" + key + "](" + found[key] + ")")
		last = s[1]
	}
	b.WriteString(text[last:])
	if utf8.RuneCountInString(b.String()) > maxMarkdownChars {
		return text
	}
	return b.String()
}

// linkCache keeps the permalinks Web has had from Slack, by thread key.
type linkCache struct {
	mu sync.Mutex
	m  map[string]string
}

// threadLink returns the permalink of the thread key, from the cache or
// else from Slack, or "" when Slack gives none, or one that is not an
// https URL that fits in a Markdown link.
func (w *Web) threadLink(ctx context.Context, key string) string {
	w.links.mu.Lock()
	link, ok := w.links.m[key]
	w.links.mu.Unlock()
	if ok {
		return link
	}
	channel, ts, _ := ParseThreadKey(key)
	link, err := w.Permalink(ctx, channel, ts)
	if err != nil || !strings.HasPrefix(link, "https://") || strings.ContainsAny(link, "()<> \t\n") {
		return ""
	}
	w.links.mu.Lock()
	defer w.links.mu.Unlock()
	if w.links.m == nil || len(w.links.m) >= maxCachedLinks {
		w.links.m = make(map[string]string)
	}
	w.links.m[key] = link
	return link
}
