package store

import (
	"context"
	"database/sql"
	"errors"
)

// Uplink is an uplink message in the hub's inbox with the client it came
// from.
type Uplink struct {
	Message
	// Client is the client the message came from; empty for a message
	// stored before the hub recorded senders.
	Client string
	// PartsSent is how many parts of the message, split for Slack, are in
	// Slack already.
	PartsSent int
}

// PartSent records that part n of msgID, counted from 0, is in Slack at
// ts, and so that its first n+1 parts are.
func (h *Hub) PartSent(ctx context.Context, msgID string, n int, ts string) error {
	return h.transact(ctx, func(tx *Hub) error {
		if _, err := tx.db.ExecContext(ctx,
			"INSERT INTO post_part (msg_id, part, ts) VALUES (?, ?, ?) ON CONFLICT (msg_id, part) DO UPDATE SET ts = excluded.ts",
			msgID, n, ts); err != nil {
			return err
		}
		_, err := tx.db.ExecContext(ctx, "UPDATE inbox SET parts_sent = ? WHERE msg_id = ?", n+1, msgID)
		return err
	})
}

// Posted is an uplink message as a request to delete it needs it.
type Posted struct {
	Uplink
	// Delivered is set once the message is out as far as it goes.
	Delivered bool
	// TS are the Slack ts of its parts that are in Slack, in order.
	TS []string
}

// Posted returns the uplink message msgID, or ErrNotFound.
func (h *Hub) Posted(ctx context.Context, msgID string) (Posted, error) {
	p := Posted{Uplink: Uplink{Message: Message{MsgID: msgID}}}
	err := h.db.QueryRowContext(ctx,
		"SELECT payload, client_id, parts_sent, delivered FROM inbox WHERE msg_id = ?", msgID).
		Scan(&p.Payload, &p.Client, &p.PartsSent, &p.Delivered)
	if errors.Is(err, sql.ErrNoRows) {
		return Posted{}, ErrNotFound
	}
	if err != nil {
		return Posted{}, err
	}
	rows, err := h.db.QueryContext(ctx, "SELECT ts FROM post_part WHERE msg_id = ? ORDER BY part", msgID)
	if err != nil {
		return Posted{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var ts string
		if err := rows.Scan(&ts); err != nil {
			return Posted{}, err
		}
		p.TS = append(p.TS, ts)
	}
	return p, rows.Err()
}

// UndeliveredFrom returns the messages not yet marked delivered, oldest
// first, each with the client it came from.
func (in HubInbox) UndeliveredFrom(ctx context.Context) ([]Uplink, error) {
	rows, err := in.db.QueryContext(ctx,
		"SELECT msg_id, payload, client_id, parts_sent FROM inbox WHERE delivered = 0 ORDER BY rowid")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var us []Uplink
	for rows.Next() {
		var u Uplink
		if err := rows.Scan(&u.MsgID, &u.Payload, &u.Client, &u.PartsSent); err != nil {
			return nil, err
		}
		us = append(us, u)
	}
	return us, rows.Err()
}

// Clients returns the registered clients that are not revoked, in order of
// their ids.
func (h *Hub) Clients(ctx context.Context) ([]string, error) {
	rows, err := h.db.QueryContext(ctx, "SELECT client_id FROM client WHERE revoked = 0 ORDER BY client_id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var clients []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		clients = append(clients, c)
	}
	return clients, rows.Err()
}

// LastUndelivered returns the rowid of the latest message from client,
// among those up to upTo, that is not marked delivered, or 0 when there
// is none. Alerts are left out: they hold up no post.
func (in HubInbox) LastUndelivered(ctx context.Context, client string, upTo int64) (int64, error) {
	var last int64
	err := in.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(rowid), 0) FROM inbox WHERE delivered = 0 AND client_id = ? AND rowid <= ?
		 AND json_extract(CAST(payload AS TEXT), '$.type') IS NOT 'alert'`, client, upTo).Scan(&last)
	return last, err
}
