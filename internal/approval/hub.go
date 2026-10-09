package approval

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
)

// DefaultInterval is how often Run looks for approvals that have expired
// and for cards that do not show their outcome yet.
const DefaultInterval = time.Minute

// DefaultSlackTimeout bounds each Slack call a sweep makes.
const DefaultSlackTimeout = 30 * time.Second

// maxHints is how many hints wait for a sweep at most; past it a hint is
// dropped, with a log line, rather than kept without bound while Slack
// is away.
const maxHints = 64

// ErrOff is returned by Flow.Request when the hub cannot run approvals:
// it has no key, no Slack, no card channel or no approvers. Nothing is
// recorded or posted then, so no approval can be given.
var ErrOff = errors.New("approval: approvals are not set up on the hub")

// ErrTooBig is returned by Flow.Request for an action the card cannot
// show whole: what the approver sees must be what is signed, so such an
// action is refused rather than shown cut.
var ErrTooBig = errors.New("approval: the action does not fit in one card")

// Flow runs approvals on the hub: Request records what an agent asks and
// posts the card; Click applies a press on a card's button; Run expires
// what nobody decided, finishes cards and delivers the hints clicks left. Every outcome goes down to the
// client that asked, signed when approved. Its exported fields are set
// before use and not changed after.
type Flow struct {
	Store *store.Hub
	Slack slack.API
	// Key signs approvals. Nil means the hub cannot sign: Request fails
	// with ErrOff.
	Key ed25519.PrivateKey
	// Channel is where the cards go.
	Channel string
	// Approvers are the Slack user ids of the people who may decide.
	Approvers []string
	// TTL is how long a request waits to be decided, and how long an
	// approval may be used once given; zero means the package's TTL.
	TTL time.Duration
	// Interval is how often Run sweeps; zero means DefaultInterval.
	Interval time.Duration
	// SlackTimeout bounds each Slack call a sweep makes, so that a Slack
	// that does not answer holds up one card or hint, not the sweep; zero
	// means DefaultSlackTimeout.
	SlackTimeout time.Duration
	// Now returns the current time; nil means time.Now.
	Now func() time.Time
	// Stored, if set, is called each time an outcome has been queued for
	// a client. It must not block.
	Stored func()

	nudge     chan struct{}
	nudgeOnce sync.Once
	// mu guards hints, the hints clicks left for Pass to deliver.
	mu    sync.Mutex
	hints []hintFor
}

// hintFor is a hint to show user in channel.
type hintFor struct{ channel, user, text string }

// Request records an approval client's agent asks for, with summary saying
// what the action does and action its parameters, and posts the card. It
// returns the approval id. client is who asks, as the hub authenticated
// it; agent and requester (the Slack user id of the person the agent says
// it asks on behalf of, or empty) are what the agent reported: the card
// shows them as such, and no check reads them. The hash the signature
// will cover is of action byte for byte.
func (f *Flow) Request(ctx context.Context, client, agent, requester, summary string, action []byte) (string, error) {
	if f.Key == nil || f.Slack == nil || f.Channel == "" || len(f.Approvers) == 0 {
		return "", ErrOff
	}
	if _, ok := slack.ParamBlocks(string(action)); !ok {
		return "", ErrTooBig
	}
	now := f.now()
	a := store.Approval{
		ID: rand.Text(), Client: client, Agent: agent, Requester: requester, Summary: summary, Action: action,
		Nonce: make([]byte, 32), RequestedAt: now, ExpiresAt: now.Add(f.ttl()), Channel: f.Channel, Status: store.Pending,
	}
	rand.Read(a.Nonce)
	ts, err := f.Slack.PostCard(ctx, f.Channel, card(a))
	if err != nil {
		return "", fmt.Errorf("approval: posting the card: %w", err)
	}
	a.TS = ts
	if err := f.Store.PutApproval(ctx, a); err != nil {
		// The card is up without a record behind it; a click on it would
		// find no approval. Take it down, within deleteTimeout even if the
		// caller has given up.
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), deleteTimeout)
		derr := f.Slack.Delete(dctx, f.Channel, ts)
		cancel()
		if derr != nil {
			return "", fmt.Errorf("approval: recording %s failed: %w; deleting its card failed too, so a card no one can approve stays in Slack: %v", a.ID, err, derr)
		}
		return "", fmt.Errorf("approval: recording %s failed, so its card was deleted: %w", a.ID, err)
	}
	return a.ID, nil
}

// sign is Sign; a test counts through it.
var sign = Sign

// deleteTimeout bounds the delete that undoes a request whose approval
// could not be recorded; tests shorten it.
var deleteTimeout = 30 * time.Second

// Click applies c. A click counts only if the clicker is an approver and
// not a bot, c names the card as recorded, and the approval is still
// pending and not expired, all judged under the database's write lock; a
// click that does not count changes nothing, and the clicker is told why
// in a hint the next Pass delivers. A click that counts decides the
// approval and queues the outcome for the client in the same
// transaction; the card is brought up to date by the next Pass. Click
// calls no Slack API itself, so the caller may ack as soon as it returns.
// An error means nothing was decided and the click should not be acked.
func (f *Flow) Click(ctx context.Context, c slack.Click) error {
	hint, err := f.click(ctx, c)
	if err != nil {
		return err
	}
	if hint != "" {
		f.mu.Lock()
		if len(f.hints) < maxHints {
			f.hints = append(f.hints, hintFor{c.Channel, c.User, hint})
		} else {
			slog.Warn("approval: hints are piling up, dropping one", "approval", c.ID, "user", c.User, "hint", hint)
		}
		f.mu.Unlock()
	}
	f.Nudge()
	return nil
}

// click returns the hint for a click that did not count, "" for one that
// did.
func (f *Flow) click(ctx context.Context, c slack.Click) (string, error) {
	switch {
	case c.Bot:
		return "bot 不能处理审批", nil
	case !slices.Contains(f.Approvers, c.User):
		return "你不在审批人名单上", nil
	}
	a, err := f.Store.Approval(ctx, c.ID)
	if errors.Is(err, store.ErrNotFound) {
		return "没有这张审批卡 (编号 " + c.ID + ")", nil
	}
	if err != nil {
		return "", err
	}
	if c.Channel != a.Channel || c.TS != a.TS {
		return "这个按钮不在编号 " + c.ID + " 的审批卡上", nil
	}
	if a.Status != store.Pending {
		return "这张卡已经处理过了：" + outcomeName(a.Status), nil
	}
	outcome := payload.Rejected
	if c.Approve {
		outcome = payload.Approved
	}
	d, decided, err := f.decide(ctx, a.ID, outcome, c.User)
	switch {
	case err != nil:
		return "", err
	case !decided:
		return "这张卡已经处理过了", nil
	case d.Status == payload.Expired:
		return "这张卡已经过期了", nil
	}
	return "", nil
}

// outcomeName says an outcome in the card's words.
func outcomeName(outcome string) string {
	switch outcome {
	case payload.Approved:
		return "已批准"
	case payload.Rejected:
		return "已拒绝"
	default:
		return "已过期"
	}
}

// decide moves the approval with id from pending to outcome, by `by`, and
// queues the outcome for the client in the same transaction. The state
// and the clock are read under the write lock: an approval found expired
// there expires instead, whatever was asked, and the signature, on an
// approval, is made only in the transaction that takes it, so two
// approvers clicking at once make one signature. It returns what was
// decided and whether the approval was still pending.
func (f *Flow) decide(ctx context.Context, id, outcome, by string) (d store.Decision, decided bool, err error) {
	decided, err = f.Store.DecideApproval(ctx, id, func(tx *store.Hub, a store.Approval) (store.Decision, error) {
		now := f.now()
		if expired := !now.Before(a.ExpiresAt); expired {
			outcome, by = payload.Expired, ""
		} else if outcome == payload.Expired {
			return store.Decision{}, nil
		}
		d = store.Decision{Status: outcome, By: by, At: now}
		m := payload.Message{Type: payload.Approval, ApprovalID: a.ID, Agent: a.Agent, Outcome: outcome, Approver: by, Text: a.Summary}
		if outcome == payload.Approved {
			c := Content{
				ApprovalID: a.ID, ParamsSHA256: sha256.Sum256(a.Action), TargetMachine: a.Client, TargetAgent: a.Agent,
				Approver: by, ApprovedAt: now, ExpiresAt: now.Add(f.ttl()), Nonce: a.Nonce,
			}
			doc, err := json.Marshal(Approval{Content: c, Signature: sign(f.Key, c)})
			if err != nil {
				return store.Decision{}, err
			}
			m.Approval = doc
		}
		b, err := json.Marshal(m)
		if err != nil {
			return store.Decision{}, err
		}
		_, err = tx.Outbox.Enqueue(ctx, a.Client, b)
		return d, err
	})
	if err != nil || !decided {
		return store.Decision{}, false, err
	}
	if f.Stored != nil {
		f.Stored()
	}
	return d, true, nil
}

// finishCard makes a's card show its outcome and records that it does,
// giving Slack SlackTimeout. A card Slack does not update now is left for
// the next Pass; a card that is gone from Slack has nothing to show.
func (f *Flow) finishCard(ctx context.Context, a store.Approval) {
	sctx, cancel := context.WithTimeout(ctx, f.slackTimeout())
	defer cancel()
	err := f.Slack.UpdateCard(sctx, a.Channel, a.TS, card(a))
	if errors.Is(err, slack.ErrNotFound) {
		slog.Warn("approval: the card is gone from Slack", "approval", a.ID)
	} else if err != nil {
		slog.Warn("approval: updating the card, will retry", "approval", a.ID, "err", err)
		return
	}
	if err := f.Store.MarkCardFinal(ctx, a.ID); err != nil {
		slog.Warn("approval: recording that the card is final", "approval", a.ID, "err", err)
	}
}

// card is a's card as it should look now.
func card(a store.Approval) slack.Card {
	c := slack.Card{ID: a.ID, Summary: a.Summary, Params: string(a.Action), Machine: a.Client, Agent: a.Agent, Requester: a.Requester, Expires: a.ExpiresAt}
	if a.Status != store.Pending {
		c.Outcome, c.Approver, c.DecidedAt = a.Status, a.DecidedBy, a.DecidedAt
	}
	return c
}

// Nudge tells Run that there is a card to finish or a hint to deliver.
// It never blocks.
func (f *Flow) Nudge() {
	select {
	case f.nudgeCh() <- struct{}{}:
	default:
	}
}

func (f *Flow) nudgeCh() chan struct{} {
	f.nudgeOnce.Do(func() { f.nudge = make(chan struct{}, 1) })
	return f.nudge
}

// Run sweeps every Interval, and when nudged, until ctx is done. It must
// be called once.
func (f *Flow) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if err := f.Pass(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("approval: sweep", "err", err)
		}
		t := time.NewTimer(f.interval())
		select {
		case <-f.nudgeCh():
		case <-t.C:
		case <-ctx.Done():
		}
		t.Stop()
	}
}

// Pass expires every pending approval past its expiry, sending the
// outcome down, updates every decided card that does not show its
// outcome yet, and delivers the hints clicks left. Each Slack call gets
// SlackTimeout, so one Pass is bounded; a hint Slack does not take is
// logged and dropped.
func (f *Flow) Pass(ctx context.Context) error {
	pending, err := f.Store.PendingApprovals(ctx)
	if err != nil {
		return err
	}
	now := f.now()
	for _, a := range pending {
		if !now.Before(a.ExpiresAt) {
			if _, _, err := f.decide(ctx, a.ID, payload.Expired, ""); err != nil {
				return err
			}
		}
	}
	unfinished, err := f.Store.UnfinishedCards(ctx)
	if err != nil {
		return err
	}
	for _, a := range unfinished {
		f.finishCard(ctx, a)
	}
	f.mu.Lock()
	hints := f.hints
	f.hints = nil
	f.mu.Unlock()
	for _, h := range hints {
		sctx, cancel := context.WithTimeout(ctx, f.slackTimeout())
		err := f.Slack.Whisper(sctx, h.channel, h.user, h.text)
		cancel()
		if err != nil {
			slog.Warn("approval: telling the clicker why the click did not count", "user", h.user, "err", err)
		}
	}
	return nil
}

func (f *Flow) now() time.Time {
	if f.Now != nil {
		return f.Now()
	}
	return time.Now()
}

func (f *Flow) ttl() time.Duration {
	if f.TTL != 0 {
		return f.TTL
	}
	return TTL
}

func (f *Flow) slackTimeout() time.Duration {
	if f.SlackTimeout != 0 {
		return f.SlackTimeout
	}
	return DefaultSlackTimeout
}

func (f *Flow) interval() time.Duration {
	if f.Interval != 0 {
		return f.Interval
	}
	return DefaultInterval
}
