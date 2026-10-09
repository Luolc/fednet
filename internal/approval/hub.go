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
	"time"

	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
)

// MaxAction is the largest action, in bytes, a request may carry.
const MaxAction = 64 << 10

// DefaultInterval is how often Run looks for approvals that have expired
// and for cards that do not show their outcome yet.
const DefaultInterval = time.Minute

// ErrOff is returned by Flow.Request when the hub cannot run approvals:
// it has no key, no Slack, no card channel or no approvers. Nothing is
// recorded or posted then, so no approval can be given.
var ErrOff = errors.New("approval: approvals are not set up on the hub")

// Flow runs approvals on the hub: Request records what an agent asks and
// posts the card; Click applies a press on a card's button; Run expires
// what nobody decided and finishes cards. Every outcome goes down to the
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
	// Now returns the current time; nil means time.Now.
	Now func() time.Time
	// Stored, if set, is called each time an outcome has been queued for
	// a client. It must not block.
	Stored func()
}

// Request records an approval client's agent asks for, with summary saying
// what the action does and action its parameters, and posts the card. It
// returns the approval id. requester, if not empty, is the Slack user id
// of the person the agent asks on behalf of; they may not decide it. The
// hash the signature will cover is of action byte for byte.
func (f *Flow) Request(ctx context.Context, client, agent, requester, summary string, action []byte) (string, error) {
	if f.Key == nil || f.Slack == nil || f.Channel == "" || len(f.Approvers) == 0 {
		return "", ErrOff
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

// deleteTimeout bounds the delete that undoes a request whose approval
// could not be recorded; tests shorten it.
var deleteTimeout = 30 * time.Second

// Click applies c. A click counts only if the clicker is an approver, not
// a bot and not the requester, and the approval is still pending and not
// expired; a click that does not count changes nothing, and the clicker
// is told why in an ephemeral message. A click that counts decides the
// approval, queues the outcome for the client, then updates the card. An
// error means nothing was decided and the click should not be acked.
func (f *Flow) Click(ctx context.Context, c slack.Click) error {
	hint, err := f.click(ctx, c)
	if err != nil {
		return err
	}
	if hint == "" {
		return nil
	}
	if err := f.Slack.Whisper(ctx, c.Channel, c.User, hint); err != nil {
		slog.Warn("approval: telling the clicker why the click did not count", "approval", c.ID, "user", c.User, "err", err)
	}
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
	if a.Requester == c.User {
		return "请求方不能处理自己发起的审批", nil
	}
	if a.Status != store.Pending {
		return "这张卡已经处理过了：" + outcomeName(a.Status), nil
	}
	now := f.now()
	if !now.Before(a.ExpiresAt) {
		if _, err := f.decide(ctx, a, payload.Expired, "", now); err != nil {
			return "", err
		}
		return "这张卡已经过期了", nil
	}
	outcome := payload.Rejected
	if c.Approve {
		outcome = payload.Approved
	}
	decided, err := f.decide(ctx, a, outcome, c.User, now)
	if err != nil {
		return "", err
	}
	if !decided {
		return "这张卡已经处理过了", nil
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

// decide moves a from pending to outcome, by `by` at now, and queues the
// outcome for a's client in the same transaction, signed when approved;
// then it updates the card. It reports whether a was still pending.
func (f *Flow) decide(ctx context.Context, a store.Approval, outcome, by string, now time.Time) (bool, error) {
	m := payload.Message{Type: payload.Approval, ApprovalID: a.ID, Agent: a.Agent, Outcome: outcome, Approver: by, Text: a.Summary}
	if outcome == payload.Approved {
		c := Content{
			ApprovalID: a.ID, ParamsSHA256: sha256.Sum256(a.Action), TargetMachine: a.Client, TargetAgent: a.Agent,
			Approver: by, ApprovedAt: now, ExpiresAt: now.Add(f.ttl()), Nonce: a.Nonce,
		}
		doc, err := json.Marshal(Approval{Content: c, Signature: Sign(f.Key, c)})
		if err != nil {
			return false, err
		}
		m.Approval = doc
	}
	b, err := json.Marshal(m)
	if err != nil {
		return false, err
	}
	decided, err := f.Store.DecideApproval(ctx, a.ID, outcome, by, now, func(tx *store.Hub) error {
		_, err := tx.Outbox.Enqueue(ctx, a.Client, b)
		return err
	})
	if err != nil || !decided {
		return false, err
	}
	if f.Stored != nil {
		f.Stored()
	}
	a.Status, a.DecidedBy, a.DecidedAt = outcome, by, now
	f.finishCard(ctx, a)
	return true, nil
}

// finishCard makes a's card show its outcome and records that it does. A
// card Slack does not update now is left for Run to try again; a card
// that is gone from Slack has nothing to show.
func (f *Flow) finishCard(ctx context.Context, a store.Approval) {
	err := f.Slack.UpdateCard(ctx, a.Channel, a.TS, card(a))
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
	c := slack.Card{ID: a.ID, Summary: a.Summary, Params: string(a.Action), Machine: a.Client, Agent: a.Agent, Expires: a.ExpiresAt}
	if a.Status != store.Pending {
		c.Outcome, c.Approver, c.DecidedAt = a.Status, a.DecidedBy, a.DecidedAt
	}
	return c
}

// Run sweeps every Interval until ctx is done. It must be called once.
func (f *Flow) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if err := f.Pass(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("approval: sweep", "err", err)
		}
		t := time.NewTimer(f.interval())
		select {
		case <-t.C:
		case <-ctx.Done():
		}
		t.Stop()
	}
}

// Pass expires every pending approval past its expiry, sending the
// outcome down, and updates every decided card that does not show its
// outcome yet.
func (f *Flow) Pass(ctx context.Context) error {
	pending, err := f.Store.PendingApprovals(ctx)
	if err != nil {
		return err
	}
	now := f.now()
	for _, a := range pending {
		if !now.Before(a.ExpiresAt) {
			if _, err := f.decide(ctx, a, payload.Expired, "", now); err != nil {
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

func (f *Flow) interval() time.Duration {
	if f.Interval != 0 {
		return f.Interval
	}
	return DefaultInterval
}
