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
	// BackfillEpoch counts the Slack connections, across processes: a
	// backfill may move LastSeen on and clear BackfillFrom only while the
	// epoch is still the one it started under.
	BackfillEpoch = "backfill_epoch"
)

// FixBackfillStart is for a connection to Slack that has just come up: in
// one transaction it starts a new epoch and sets BackfillFrom to where the
// next backfill starts, the latest message seen or, earlier, where a
// backfill is still pending. It returns the new epoch.
func (h *Hub) FixBackfillStart(ctx context.Context) (epoch int64, err error) {
	err = h.transact(ctx, func(tx *Hub) error {
		if _, err := tx.db.ExecContext(ctx,
			`INSERT INTO slack_state (key, value) VALUES (?, '1')
			 ON CONFLICT (key) DO UPDATE SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT)`, BackfillEpoch); err != nil {
			return err
		}
		if err := tx.db.QueryRowContext(ctx, "SELECT CAST(value AS INTEGER) FROM slack_state WHERE key = ?", BackfillEpoch).Scan(&epoch); err != nil {
			return err
		}
		from, err := tx.SlackState(ctx, LastSeen)
		if err != nil {
			return err
		}
		pending, err := tx.SlackState(ctx, BackfillFrom)
		if err != nil {
			return err
		}
		if pending != "" && (from == "" || olderTS(pending, from)) {
			from = pending
		}
		return tx.SetSlackState(ctx, BackfillFrom, from)
	})
	return epoch, err
}

// FinishBackfill is for a backfill that has read everything up to ts: in
// one transaction it moves LastSeen on to ts and clears BackfillFrom,
// unless a connection has come up since the backfill started, under
// another epoch, whose own backfill the start now belongs to. It reports
// whether it did.
func (h *Hub) FinishBackfill(ctx context.Context, epoch int64, ts string) (bool, error) {
	done := false
	err := h.transact(ctx, func(tx *Hub) error {
		var current int64
		if err := tx.db.QueryRowContext(ctx, "SELECT CAST(value AS INTEGER) FROM slack_state WHERE key = ?", BackfillEpoch).Scan(&current); err != nil {
			return err
		}
		if current != epoch {
			return nil
		}
		if err := tx.AdvanceLastSeen(ctx, ts); err != nil {
			return err
		}
		if err := tx.SetSlackState(ctx, BackfillFrom, ""); err != nil {
			return err
		}
		done = true
		return nil
	})
	return done, err
}

// olderTS reports whether Slack timestamp a is older than b: of the same
// length they order as strings, and a shorter one is older.
func olderTS(a, b string) bool {
	return len(a) < len(b) || (len(a) == len(b) && a < b)
}

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
