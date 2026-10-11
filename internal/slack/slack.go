// Package slack is the part of Slack's Web API the hub uses on behalf of the
// agents. Only the hub holds a Slack token: clients ask the hub, and the hub
// calls an API. Web calls Slack's Web API; Fake is kept in memory, for
// tests.
package slack

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrNotFound is returned for a channel, thread or message Slack does not
// know.
var ErrNotFound = errors.New("slack: not found")

// Message is one message in a channel or a thread.
type Message struct {
	// TS is the message's Slack timestamp, which identifies it in its channel.
	TS   string `json:"ts"`
	User string `json:"user"`
	Text string `json:"text"`
	// ThreadTS is the ts of the first message of the thread the message is
	// a reply in; empty for a message that is not a reply.
	ThreadTS string `json:"thread_ts,omitempty"`
	// BotID is set on a message a bot posted, this one included.
	BotID string `json:"bot_id,omitempty"`
	// SubType is Slack's subtype of the message; empty for a plain message
	// a person typed.
	SubType string `json:"subtype,omitempty"`
	// Files are the files uploaded with the message.
	Files []File `json:"files,omitempty"`
	// LatestReply is the ts of the latest reply in the thread the message
	// starts, as History reports it; empty when it has none, and on a
	// message Replies returns.
	LatestReply string `json:"latest_reply,omitempty"`
}

// File is a file uploaded with a message.
type File struct {
	// ID is Slack's id of the file, which FileInfo and Download take.
	ID   string `json:"id,omitempty"`
	Name string `json:"name"`
	// Mimetype and Size are what Slack reports of the file; Size is in
	// bytes.
	Mimetype string `json:"mimetype,omitempty"`
	Size     int    `json:"size,omitempty"`
	// URL is the file's permalink, which opens it in Slack.
	URL string `json:"url"`
	// DownloadURL is where Download gets the content, with the token; it
	// is set by FileInfo and goes to no client.
	DownloadURL string `json:"-"`
}

// Mentions reports whether text mentions user, as Slack writes a mention
// in a message's text: <@ID> or <@ID|name>.
func Mentions(text, user string) bool {
	if user == "" {
		return false
	}
	return strings.Contains(text, "<@"+user+">") || strings.Contains(text, "<@"+user+"|")
}

// Conversation is a channel or a direct message conversation.
type Conversation struct {
	ID string
	// IM is set for a direct message conversation.
	IM bool
}

// ChannelInfo is what Slack says about a channel.
type ChannelInfo struct {
	// Name is the channel's current name, without the #.
	Name string
	// Purpose is the description shown with the channel.
	Purpose string
}

// API is what the hub needs from Slack.
type API interface {
	// Self returns the Slack user id of the bot the token belongs to.
	Self(ctx context.Context) (string, error)
	// Replies returns the messages of the thread that starts at ts in
	// channel, the first message included, oldest first.
	Replies(ctx context.Context, channel, ts string) ([]Message, error)
	// History returns the messages posted in channel after oldest, a ts,
	// that are not replies in a thread, oldest first.
	History(ctx context.Context, channel, oldest string) ([]Message, error)
	// Conversations returns the channels and direct message conversations
	// the bot is a member of.
	Conversations(ctx context.Context) ([]Conversation, error)
	// Post posts text in channel as a new message, which starts a thread,
	// and returns its ts.
	Post(ctx context.Context, channel, text string) (string, error)
	// ChannelInfo returns channel's name and purpose.
	ChannelInfo(ctx context.Context, channel string) (ChannelInfo, error)
	// SetPurpose replaces channel's purpose.
	SetPurpose(ctx context.Context, channel, purpose string) error
	// PostReply posts text in the thread that starts at ts in channel and
	// returns the new message's ts.
	PostReply(ctx context.Context, channel, ts, text string) (string, error)
	// PostReplyMentioning is PostReply with each of users, Slack user
	// ids, mentioned at the start of the message.
	PostReplyMentioning(ctx context.Context, channel, ts, text string, users []string) (string, error)
	// Delete deletes the message at ts in channel.
	Delete(ctx context.Context, channel, ts string) error
	// DM sends text to user as a direct message, which belongs to no
	// thread.
	DM(ctx context.Context, user, text string) error
	// PostCard posts c in channel as a new message and returns its ts.
	PostCard(ctx context.Context, channel string, c Card) (string, error)
	// UpdateCard replaces the message at ts in channel with c.
	UpdateCard(ctx context.Context, channel, ts string, c Card) error
	// Whisper shows text in channel to user alone, as an ephemeral message.
	Whisper(ctx context.Context, channel, user, text string) error
	// Respond posts text through the response URL of a slash command or
	// of a click on what one showed, in place of the message there, which
	// is seen by that person alone.
	Respond(ctx context.Context, responseURL, text string) error
	// FileInfo returns what Slack knows of the file with id, DownloadURL
	// included; a file Slack does not have, or has deleted, is
	// ErrNotFound.
	FileInfo(ctx context.Context, id string) (File, error)
	// Download returns the content of f, which FileInfo returned, as a
	// stream the caller closes. The content is not checked against
	// f.Size: the caller counts.
	Download(ctx context.Context, f File) (io.ReadCloser, error)
	// Upload posts files, with text, in the thread that starts at ts in
	// channel, as one message; each file's content is read from its Body,
	// Size bytes.
	Upload(ctx context.Context, channel, ts, text string, files []Upload) error
	// PostProgress posts p in the thread that starts at ts in channel and
	// returns the new message's ts.
	PostProgress(ctx context.Context, channel, ts string, p Progress) (string, error)
	// UpdateProgress replaces the progress card at ts in channel with p.
	UpdateProgress(ctx context.Context, channel, ts string, p Progress) error
	// PostFooter posts text, standard Markdown whose links are kept and
	// nothing else, as one line of small grey text in the thread that
	// starts at ts in channel, and returns the new message's ts.
	PostFooter(ctx context.Context, channel, ts, text string) (string, error)
}

// Upload is one file to upload: its name, its size in bytes and its
// content.
type Upload struct {
	Name string
	Size int64
	Body io.Reader
}

// Command is a slash command someone sent, as Socket Mode delivers it.
type Command struct {
	// Name is the command with its slash, such as "/fednet"; Text is what
	// followed it.
	Name string
	Text string
	// User is the Slack user id of the sender; Channel is where it was
	// sent, which the bot need not be in.
	User    string
	Channel string
	// ResponseURL takes a later reply to the sender alone.
	ResponseURL string
}

// CommandReply is the answer to a Command, shown to the sender alone:
// Text, or an UpgradeCard when Card is set.
type CommandReply struct {
	Text string
	Card *UpgradeCard
}

// UpgradeCard asks the sender of `/fednet upgrade` to confirm: it shows
// the upgrade and has a confirm and a cancel button, each carrying ID.
type UpgradeCard struct {
	// ID identifies the confirmation; a click reports it.
	ID string
	// From and To are the hub's version and the release to upgrade to.
	From, To string
	// Clients lists each client with its version and whether it is
	// online, one a line.
	Clients []string
}

// Action ids and block id of an UpgradeCard's buttons.
const (
	UpgradeConfirmAction = "upgrade-confirm"
	UpgradeCancelAction  = "upgrade-cancel"
)

// UpgradeBlockID is the id of the block the buttons of the upgrade card
// with id are in.
func UpgradeBlockID(id string) string { return "upgrade:" + id }

// Card is an approval card: what an agent asks leave to do, and, once
// decided, how it ended. While Outcome is empty the card has an approve
// and a reject button, each carrying ID; a decided card has no buttons.
type Card struct {
	// ID is the approval id; a click on a button reports it.
	ID string
	// Summary says what the action does; Params is the action's
	// parameters, the JSON the agent handed in.
	Summary string
	Params  string
	// Machine is who asked, as the hub authenticated it: the client id.
	// Agent is the agent on it, and Requester the person it says it asks
	// on behalf of; both are what the agent reported, shown as such and
	// used by no check.
	Machine   string
	Agent     string
	Requester string
	// Expires is when the approval expires unless decided.
	Expires time.Time
	// Outcome is empty while the card waits, and otherwise one of the
	// payload package's outcomes.
	Outcome string
	// Approver is the Slack user id of who approved or rejected.
	Approver string
	// DecidedAt is when Outcome was reached.
	DecidedAt time.Time
}

// Click is a press on one of a card's buttons, as the interactive callback
// reports it.
type Click struct {
	// ID is the approval id the button carried.
	ID string
	// Approve is set for the approve button, clear for the reject button.
	Approve bool
	// User is the Slack user id of who clicked; Bot is set when the user is
	// a bot.
	User string
	Bot  bool
	// Channel and TS locate the card.
	Channel string
	TS      string
	// ResponseURL takes a reply to the clicker alone, in place of the
	// message the button was on; a click on an approval card does not
	// use it.
	ResponseURL string
}

// Limits of a card: Slack takes at most maxBlocks blocks in one message
// and about 3000 characters in one text. The card has fixedBlocks blocks
// besides the parameters (the header, the two labels, the summary, who
// asks, the expiry with the id, and the buttons or the outcome), so the
// parameters get the rest, ParamChunk characters each.
const (
	maxBlocks      = 50
	fixedBlocks    = 7
	ParamChunk     = 2900
	MaxParamChunks = maxBlocks - fixedBlocks
)

// ParamBlocks splits params into the texts of the card's parameter
// blocks, each at most ParamChunk characters, split between characters.
// The blocks show the text as it is, so nothing is escaped. ok is false
// when they would take more blocks than fit in one message, so that a
// card either shows the whole parameters or is not posted. Empty params
// take one empty block.
func ParamBlocks(params string) (chunks []string, ok bool) {
	var b strings.Builder
	n := 0 // characters in b
	for _, r := range params {
		if n == ParamChunk {
			chunks = append(chunks, b.String())
			b.Reset()
			n = 0
		}
		b.WriteRune(r)
		n++
	}
	chunks = append(chunks, b.String())
	return chunks, len(chunks) <= MaxParamChunks
}

// Action ids of a card's buttons.
const (
	ApproveAction = "approve"
	RejectAction  = "reject"
)

// CardBlockID is the id of the block the buttons of the card for the
// approval with id are in; a click reports it, so a button from
// elsewhere carrying the same value is told apart.
func CardBlockID(id string) string { return "approval:" + id }

// ThreadKey is how fednet names a thread: its channel and the ts of its
// first message, joined by a slash.
func ThreadKey(channel, ts string) string { return channel + "/" + ts }

// ParseThreadKey splits a thread key into its channel and ts.
func ParseThreadKey(key string) (channel, ts string, ok bool) {
	channel, ts, ok = strings.Cut(key, "/")
	return channel, ts, ok && channel != "" && ts != "" && !strings.Contains(ts, "/")
}

// CompareTS orders two Slack timestamps, which are "SECONDS.FRACTION"
// strings; it returns -1, 0 or 1 as a is before, at or after b. A ts that
// does not parse sorts first.
func CompareTS(a, b string) int {
	as, af := splitTS(a)
	bs, bf := splitTS(b)
	if as != bs {
		return cmp.Compare(as, bs)
	}
	return cmp.Compare(af, bf)
}

// splitTS parses ts into its seconds and its fraction scaled to
// microseconds; both are -1 for a ts that does not parse.
func splitTS(ts string) (sec, usec int64) {
	s, f, _ := strings.Cut(ts, ".")
	sec, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return -1, -1
	}
	if f == "" {
		return sec, 0
	}
	if len(f) < 6 {
		f += strings.Repeat("0", 6-len(f))
	}
	usec, err = strconv.ParseInt(f[:6], 10, 64)
	if err != nil {
		return -1, -1
	}
	return sec, usec
}

// FakeBot is the user id of the Fake's bot: Self returns it, and Post,
// PostReply and PostCard post as it.
const FakeBot = "fednet"

// Fake is an API kept in memory, for tests. Its zero value has no
// channels; AddChannel adds one.
type Fake struct {
	mu       sync.Mutex
	channels map[string]*fakeChannel
	dms      map[string][]string
	// cards maps channel and ts, as a thread key, to the card there.
	cards map[string]Card
	// progress maps channel and ts, as a thread key, to the progress
	// card there; footers maps the ts of each footer to its mrkdwn.
	progress map[string]Progress
	footers  map[string]string
	// whispers maps a user to the ephemeral texts shown to them.
	whispers map[string][]string
	// responses maps a response URL to the texts posted through it.
	responses map[string][]string
	// files maps a file id to the file and its content.
	files map[string]fakeFile
	clock int
}

type fakeFile struct {
	File
	content []byte
}

type fakeChannel struct {
	name    string
	purpose string
	im      bool
	threads map[string][]Message
}

// AddChannel adds an empty channel with purpose.
func (f *Fake) AddChannel(channel, purpose string) { f.add(channel, purpose, false) }

// AddIM adds an empty direct message conversation.
func (f *Fake) AddIM(channel string) { f.add(channel, "", true) }

func (f *Fake) add(channel, purpose string, im bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.channels == nil {
		f.channels = make(map[string]*fakeChannel)
	}
	f.channels[channel] = &fakeChannel{purpose: purpose, im: im, threads: make(map[string][]Message)}
}

// Reply adds a message from user to the thread at ts in channel and returns
// the new message's ts.
func (f *Fake) Reply(channel, ts, user, text string) (string, error) {
	return f.Add(channel, Message{ThreadTS: ts, User: user, Text: text})
}

// Add adds m to channel: as a reply in the thread at m.ThreadTS when that
// is set, as a new message that starts a thread otherwise. A m.TS that is
// empty gets a ts later than any before. It returns m's ts.
func (f *Fake) Add(channel string, m Message) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.channels[channel]
	if !ok {
		return "", ErrNotFound
	}
	if m.TS == "" {
		m.TS = f.next()
	}
	if m.ThreadTS == "" {
		c.threads[m.TS] = []Message{m}
		return m.TS, nil
	}
	if c.threads[m.ThreadTS] == nil {
		return "", ErrNotFound
	}
	c.threads[m.ThreadTS] = append(c.threads[m.ThreadTS], m)
	return m.TS, nil
}

// next returns a ts later than any before; f.mu is held.
func (f *Fake) next() string {
	f.clock++
	return "1700000000." + strconv.Itoa(100000+f.clock)
}

func (f *Fake) Replies(_ context.Context, channel, ts string) ([]Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.channels[channel]
	if !ok || c.threads[ts] == nil {
		return nil, ErrNotFound
	}
	return append([]Message(nil), c.threads[ts]...), nil
}

func (f *Fake) History(_ context.Context, channel, oldest string) ([]Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.channels[channel]
	if !ok {
		return nil, ErrNotFound
	}
	var ms []Message
	for _, ts := range slices.SortedFunc(maps.Keys(c.threads), CompareTS) {
		if CompareTS(ts, oldest) > 0 {
			m := c.threads[ts][0]
			for _, reply := range c.threads[ts][1:] {
				if CompareTS(reply.TS, m.LatestReply) > 0 {
					m.LatestReply = reply.TS
				}
			}
			ms = append(ms, m)
		}
	}
	return ms, nil
}

// Conversations returns every channel and direct message conversation
// added, ordered by id.
func (f *Fake) Conversations(context.Context) ([]Conversation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var cs []Conversation
	for _, id := range slices.Sorted(maps.Keys(f.channels)) {
		cs = append(cs, Conversation{ID: id, IM: f.channels[id].im})
	}
	return cs, nil
}

func (f *Fake) Self(context.Context) (string, error) { return FakeBot, nil }

// Post posts as the bot.
func (f *Fake) Post(_ context.Context, channel, text string) (string, error) {
	return f.Start(channel, FakeBot, text)
}

// Start posts text from user in channel as a new message, which starts a
// thread, and returns its ts.
func (f *Fake) Start(channel, user, text string) (string, error) {
	return f.Add(channel, Message{User: user, Text: text})
}

// PostReply posts as the bot.
func (f *Fake) PostReply(_ context.Context, channel, ts, text string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.channels[channel]
	if !ok || c.threads[ts] == nil {
		return "", ErrNotFound
	}
	m := Message{TS: f.next(), User: FakeBot, Text: text}
	c.threads[ts] = append(c.threads[ts], m)
	return m.TS, nil
}

// PostReplyMentioning posts as the bot; the message's Text starts with
// the mentions, as Slack's plain text does.
func (f *Fake) PostReplyMentioning(ctx context.Context, channel, ts, text string, users []string) (string, error) {
	var b strings.Builder
	for _, u := range users {
		b.WriteString("<@" + u + "> ")
	}
	return f.PostReply(ctx, channel, ts, b.String()+text)
}

// Delete deletes the message at ts in channel; deleting the first message
// of a thread deletes the thread.
func (f *Fake) Delete(_ context.Context, channel, ts string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.channels[channel]
	if !ok {
		return ErrNotFound
	}
	delete(f.cards, ThreadKey(channel, ts))
	if c.threads[ts] != nil {
		delete(c.threads, ts)
		return nil
	}
	for root, ms := range c.threads {
		for i, m := range ms {
			if m.TS == ts {
				c.threads[root] = append(ms[:i:i], ms[i+1:]...)
				return nil
			}
		}
	}
	return ErrNotFound
}

func (f *Fake) ChannelInfo(_ context.Context, channel string) (ChannelInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.channels[channel]
	if !ok {
		return ChannelInfo{}, ErrNotFound
	}
	return ChannelInfo{Name: c.name, Purpose: c.purpose}, nil
}

// RenameChannel gives channel a new name; a channel added has none.
func (f *Fake) RenameChannel(channel, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.channels[channel].name = name
}

func (f *Fake) SetPurpose(_ context.Context, channel, purpose string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.channels[channel]
	if !ok {
		return ErrNotFound
	}
	c.purpose = purpose
	return nil
}

func (f *Fake) DM(_ context.Context, user, text string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dms == nil {
		f.dms = make(map[string][]string)
	}
	f.dms[user] = append(f.dms[user], text)
	return nil
}

// DMs returns the texts sent to user as direct messages, oldest first.
func (f *Fake) DMs(user string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.dms[user]...)
}

// PostCard posts the card as a message from the bot whose text is the
// card's summary, and keeps the card for Card. Like Slack, it refuses a
// card of more than maxBlocks blocks, as Web would lay it out.
func (f *Fake) PostCard(_ context.Context, channel string, c Card) (string, error) {
	if err := fits(c); err != nil {
		return "", err
	}
	ts, err := f.Add(channel, Message{User: FakeBot, Text: c.Summary})
	if err != nil {
		return "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cards == nil {
		f.cards = make(map[string]Card)
	}
	f.cards[ThreadKey(channel, ts)] = c
	return ts, nil
}

func (f *Fake) UpdateCard(_ context.Context, channel, ts string, c Card) error {
	if err := fits(c); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.cards[ThreadKey(channel, ts)]; !ok {
		return ErrNotFound
	}
	f.cards[ThreadKey(channel, ts)] = c
	return nil
}

// fits is the check Slack would make on the card's blocks.
func fits(c Card) error {
	if n := len(blocks(c)); n > maxBlocks {
		return fmt.Errorf("slack: the card has %d blocks, Slack takes at most %d", n, maxBlocks)
	}
	return nil
}

// Card returns the card at ts in channel as it is now, and whether there
// is one.
func (f *Fake) Card(channel, ts string) (Card, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.cards[ThreadKey(channel, ts)]
	return c, ok
}

// Cards returns the cards in channel, oldest first, each with its ts.
func (f *Fake) Cards(channel string) (ts []string, cards []Card) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, key := range slices.SortedFunc(maps.Keys(f.cards), func(a, b string) int {
		_, ats, _ := ParseThreadKey(a)
		_, bts, _ := ParseThreadKey(b)
		return CompareTS(ats, bts)
	}) {
		ch, t, _ := ParseThreadKey(key)
		if ch == channel {
			ts = append(ts, t)
			cards = append(cards, f.cards[key])
		}
	}
	return ts, cards
}

func (f *Fake) Whisper(_ context.Context, channel, user, text string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.channels[channel]; !ok {
		return ErrNotFound
	}
	if f.whispers == nil {
		f.whispers = make(map[string][]string)
	}
	f.whispers[user] = append(f.whispers[user], text)
	return nil
}

// Whispers returns the ephemeral texts shown to user, oldest first.
func (f *Fake) Whispers(user string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.whispers[user]...)
}

func (f *Fake) Respond(_ context.Context, responseURL, text string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if responseURL == "" {
		return errors.New("slack: no response URL")
	}
	if f.responses == nil {
		f.responses = make(map[string][]string)
	}
	f.responses[responseURL] = append(f.responses[responseURL], text)
	return nil
}

// Responses returns the texts posted through responseURL, oldest first.
func (f *Fake) Responses(responseURL string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.responses[responseURL]...)
}

// AddFile makes file, with content, one FileInfo and Download find;
// file.Size is set from content. It returns the file as a message would
// carry it.
func (f *Fake) AddFile(file File, content []byte) File {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.files == nil {
		f.files = make(map[string]fakeFile)
	}
	file.Size = len(content)
	f.files[file.ID] = fakeFile{file, append([]byte(nil), content...)}
	return file
}

// RemoveFile deletes the file with id, as a person deleting it in Slack
// would.
func (f *Fake) RemoveFile(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.files, id)
}

func (f *Fake) FileInfo(_ context.Context, id string) (File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ff, ok := f.files[id]
	if !ok {
		return File{}, ErrNotFound
	}
	ff.DownloadURL = "fake://" + id
	return ff.File, nil
}

// Upload adds a message from the bot to the thread, with the files; each
// gets an id FileInfo and Download then find, and its content read whole
// from Body.
func (f *Fake) Upload(_ context.Context, channel, ts, text string, files []Upload) error {
	var fs []File
	var contents [][]byte
	for _, u := range files {
		b, err := io.ReadAll(u.Body)
		if err != nil {
			return err
		}
		if int64(len(b)) != u.Size {
			return fmt.Errorf("slack: %s: got %d bytes, said %d", u.Name, len(b), u.Size)
		}
		fs = append(fs, File{Name: u.Name, URL: "https://files.example.invalid/" + u.Name})
		contents = append(contents, b)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.channels[channel]
	if !ok || c.threads[ts] == nil {
		return ErrNotFound
	}
	if f.files == nil {
		f.files = make(map[string]fakeFile)
	}
	for i := range fs {
		f.clock++
		fs[i].ID = "F" + strconv.Itoa(f.clock)
		fs[i].Size = len(contents[i])
		f.files[fs[i].ID] = fakeFile{fs[i], contents[i]}
	}
	m := Message{TS: f.next(), User: FakeBot, Text: text, Files: fs, SubType: "file_share"}
	c.threads[ts] = append(c.threads[ts], m)
	return nil
}

func (f *Fake) Download(_ context.Context, file File) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ff, ok := f.files[file.ID]
	if !ok {
		return nil, ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(ff.content)), nil
}
