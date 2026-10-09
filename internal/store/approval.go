package store

import (
	"context"
	"errors"
	"time"
)

// Statuses of an Approval.
const (
	Pending = "pending"
)

// Approval is one approval an agent requested, as the hub keeps it. Times
// read back are in UTC.
type Approval struct {
	// ID is the approval id.
	ID string
	// Client and Agent are who requested: the agent on the client.
	Client string
	Agent  string
	// Requester is the Slack user id of the person the agent said it asks
	// on behalf of, if the request named one: a note for the card, read by
	// no check.
	Requester string
	// Summary says what the action does; Action is its parameters, the
	// JSON the agent handed in, byte for byte.
	Summary string
	Action  []byte
	// Nonce goes into the signature.
	Nonce []byte
	// RequestedAt and ExpiresAt bound the approval.
	RequestedAt time.Time
	ExpiresAt   time.Time
	// Channel and TS locate the card in Slack.
	Channel string
	TS      string
	// Status is Pending, or the outcome; DecidedBy and DecidedAt say who
	// decided and when, DecidedBy empty for an expiry.
	Status    string
	DecidedBy string
	DecidedAt time.Time
	// CardFinal is set once the card in Slack shows the outcome.
	CardFinal bool
}

const approvalColumns = "approval_id, client_id, agent, requester, summary, action, nonce, requested_at, expires_at, channel, ts, status, decided_by, decided_at, card_final"

// PutApproval records a, which must be Pending.
func (h *Hub) PutApproval(ctx context.Context, a Approval) error {
	_, err := h.db.ExecContext(ctx,
		"INSERT INTO approval (approval_id, client_id, agent, requester, summary, action, nonce, requested_at, expires_at, channel, ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		a.ID, a.Client, a.Agent, a.Requester, a.Summary, a.Action, a.Nonce, a.RequestedAt.UnixNano(), a.ExpiresAt.UnixNano(), a.Channel, a.TS)
	return err
}

// Approval returns the approval with id, or ErrNotFound.
func (h *Hub) Approval(ctx context.Context, id string) (Approval, error) {
	as, err := h.approvals(ctx, "WHERE approval_id = ?", id)
	if err != nil {
		return Approval{}, err
	}
	if len(as) == 0 {
		return Approval{}, ErrNotFound
	}
	return as[0], nil
}

// PendingApprovals returns the approvals still Pending, oldest first.
func (h *Hub) PendingApprovals(ctx context.Context) ([]Approval, error) {
	return h.approvals(ctx, "WHERE status = ? ORDER BY requested_at", Pending)
}

// UnfinishedCards returns the decided approvals whose card does not show
// the outcome yet, oldest first.
func (h *Hub) UnfinishedCards(ctx context.Context) ([]Approval, error) {
	return h.approvals(ctx, "WHERE status <> ? AND card_final = 0 ORDER BY decided_at", Pending)
}

func (h *Hub) approvals(ctx context.Context, where string, args ...any) ([]Approval, error) {
	rows, err := h.db.QueryContext(ctx, "SELECT "+approvalColumns+" FROM approval "+where, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var as []Approval
	for rows.Next() {
		var a Approval
		var requested, expires, decided int64
		if err := rows.Scan(&a.ID, &a.Client, &a.Agent, &a.Requester, &a.Summary, &a.Action, &a.Nonce, &requested, &expires, &a.Channel, &a.TS, &a.Status, &a.DecidedBy, &decided, &a.CardFinal); err != nil {
			return nil, err
		}
		a.RequestedAt, a.ExpiresAt = time.Unix(0, requested).UTC(), time.Unix(0, expires).UTC()
		if decided != 0 {
			a.DecidedAt = time.Unix(0, decided).UTC()
		}
		as = append(as, a)
	}
	return as, rows.Err()
}

// Decision is how an approval ends: Status is the outcome, By who decided
// (empty for an expiry) and At when. A Decision with an empty Status
// leaves the approval as it is.
type Decision struct {
	Status, By string
	At         time.Time
}

// DecideApproval decides the approval with id, in one transaction that
// holds the database's write lock from its start: it reads the approval,
// and if that is still Pending runs decide against it and a Hub bound to
// the same transaction, then records the Decision decide returns, so the
// outcome and what decide queues commit together, and decide sees the
// state no other decision can change under it. An approval that is not
// Pending, or a Decision with an empty Status, leaves it alone and
// reports false: of two decisions, only the first takes.
func (h *Hub) DecideApproval(ctx context.Context, id string, decide func(tx *Hub, a Approval) (Decision, error)) (decided bool, err error) {
	err = h.transact(ctx, func(tx *Hub) error {
		a, err := tx.Approval(ctx, id)
		if err != nil {
			return err
		}
		if a.Status != Pending {
			return nil
		}
		d, err := decide(tx, a)
		if err != nil {
			return err
		}
		if d.Status == "" {
			return errNoDecision
		}
		if _, err := tx.db.ExecContext(ctx,
			"UPDATE approval SET status = ?, decided_by = ?, decided_at = ? WHERE approval_id = ? AND status = ?",
			d.Status, d.By, d.At.UnixNano(), id, Pending); err != nil {
			return err
		}
		decided = true
		return nil
	})
	if errors.Is(err, errNoDecision) {
		err = nil
	}
	return decided && err == nil, err
}

// errNoDecision rolls back a DecideApproval whose decide returned no
// Decision, so that nothing it queued stays.
var errNoDecision = errors.New("store: no decision")

// MarkCardFinal records that the card of the approval with id shows its
// outcome.
func (h *Hub) MarkCardFinal(ctx context.Context, id string) error {
	_, err := h.db.ExecContext(ctx, "UPDATE approval SET card_final = 1 WHERE approval_id = ?", id)
	return err
}
