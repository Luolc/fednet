package store

import (
	"context"
	"database/sql"
	"errors"
)

// Keys in slack_state.
const (
	// LastSeen is the ts of the latest Slack message the hub has taken in.
	LastSeen = "last_seen"
	// BackfillFrom is where a backfill that has not finished starts; empty
	// once it has.
	BackfillFrom = "backfill_from"
)

// SlackMessage identifies a message a person posted in Slack.
type SlackMessage struct {
	Channel string
	TS      string
	// EventID is the id of the event that brought the message; empty for a
	// message read from history.
	EventID string
}

// ReceiveSlack records m and runs handle against a Hub bound to the same
// transaction, so that the record and whatever handle queues commit
// together, and advances LastSeen to m.TS if that is later. A message
// recorded before, by its event id or by its channel and ts, is not
// recorded again and handle does not run; ReceiveSlack then reports false.
func (h *Hub) ReceiveSlack(ctx context.Context, m SlackMessage, handle func(tx *Hub) error) (fresh bool, err error) {
	err = h.transact(ctx, func(tx *Hub) error {
		var eventID sql.NullString
		if m.EventID != "" {
			eventID = sql.NullString{String: m.EventID, Valid: true}
		}
		res, err := tx.db.ExecContext(ctx,
			"INSERT INTO slack_message (channel, ts, event_id) VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
			m.Channel, m.TS, eventID)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil || n == 0 {
			return err
		}
		fresh = true
		if err := tx.AdvanceLastSeen(ctx, m.TS); err != nil {
			return err
		}
		return handle(tx)
	})
	return fresh && err == nil, err
}

// AdvanceLastSeen sets LastSeen to ts unless the ts there is later. Slack
// timestamps of the same length order as strings; a shorter one is older.
func (h *Hub) AdvanceLastSeen(ctx context.Context, ts string) error {
	_, err := h.db.ExecContext(ctx,
		`INSERT INTO slack_state (key, value) VALUES (?, ?)
		 ON CONFLICT (key) DO UPDATE SET value = excluded.value
		 WHERE length(excluded.value) > length(value) OR (length(excluded.value) = length(value) AND excluded.value > value)`,
		LastSeen, ts)
	return err
}

// SlackState returns the value stored under key, or "" when there is none.
func (h *Hub) SlackState(ctx context.Context, key string) (string, error) {
	var v string
	err := h.db.QueryRowContext(ctx, "SELECT value FROM slack_state WHERE key = ?", key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}

// SetSlackState stores value under key, replacing what was there.
func (h *Hub) SetSlackState(ctx context.Context, key, value string) error {
	_, err := h.db.ExecContext(ctx,
		"INSERT INTO slack_state (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
		key, value)
	return err
}

// ThreadsIn returns the threads in channel that have an owner, in order of
// their keys.
func (h *Hub) ThreadsIn(ctx context.Context, channel string) ([]string, error) {
	prefix := channel + "/"
	rows, err := h.db.QueryContext(ctx,
		"SELECT thread FROM owner WHERE substr(thread, 1, ?) = ? ORDER BY thread", len(prefix), prefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var threads []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		threads = append(threads, t)
	}
	return threads, rows.Err()
}
