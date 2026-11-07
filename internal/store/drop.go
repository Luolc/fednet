package store

import (
	"context"
	"time"
)

// dropSchema runs each time the hub's database is opened, as routeSchema
// does and for the same reason: a hub of an earlier release still opens
// the database, and neither reads nor writes these tables.
const dropSchema = `
-- The messages in the outbox the hub put a reaction on while they wait
-- for an offline client: mark is the thread key of the message's channel
-- and ts. A row whose message has left the outbox is swept at the next
-- ack or drop.
CREATE TABLE IF NOT EXISTS outbox_mark (
	msg_id TEXT PRIMARY KEY,
	mark   TEXT NOT NULL
);
-- The messages dropped from the outbox for having waited too long, until
-- both their notices are out: told is set once the thread is told,
-- alerted once the alert is sent. Times are Unix milliseconds.
CREATE TABLE IF NOT EXISTS dropped (
	seq         INTEGER PRIMARY KEY,
	client_id   TEXT NOT NULL,
	msg_id      TEXT NOT NULL,
	payload     BLOB NOT NULL,
	enqueued_at INTEGER NOT NULL,
	dropped_at  INTEGER NOT NULL,
	told        INTEGER NOT NULL DEFAULT 0,
	alerted     INTEGER NOT NULL DEFAULT 0
);
`

// Mark records mark with the queued message msgID, and reports whether
// the message was still queued; for a message acked or dropped already
// nothing is recorded.
func (o HubOutbox) Mark(ctx context.Context, msgID, mark string) (bool, error) {
	res, err := o.db.ExecContext(ctx,
		`INSERT INTO outbox_mark (msg_id, mark) SELECT ?, ? WHERE EXISTS (SELECT 1 FROM outbox WHERE msg_id = ?)
		 ON CONFLICT (msg_id) DO UPDATE SET mark = excluded.mark`,
		msgID, mark, msgID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// sweepMarks forgets the marks of the messages no longer in the outbox
// and returns them.
func (o HubOutbox) sweepMarks(ctx context.Context) ([]string, error) {
	rows, err := o.db.QueryContext(ctx,
		"DELETE FROM outbox_mark WHERE msg_id NOT IN (SELECT msg_id FROM outbox) RETURNING mark")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ms []string
	for rows.Next() {
		var m string
		if err := rows.Scan(&m); err != nil {
			return nil, err
		}
		ms = append(ms, m)
	}
	return ms, rows.Err()
}

// Dropped is a message dropped from the outbox, waiting for its notices.
type Dropped struct {
	Downlink
	Client string
	// Enqueued is when the message was queued, At when it was dropped.
	Enqueued, At time.Time
	// Told is set once its thread is told, Alerted once it is alerted.
	Told, Alerted bool
}

// Overdue returns the clients with a message queued at or before cutoff,
// in order of their ids.
func (o HubOutbox) Overdue(ctx context.Context, cutoff time.Time) ([]string, error) {
	rows, err := o.db.QueryContext(ctx,
		"SELECT DISTINCT client_id FROM outbox WHERE enqueued_at <= ? ORDER BY client_id", cutoff.UnixMilli())
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

// DropQueued moves the messages of clients queued at or before cutoff out
// of the outbox and records them as dropped at now, in one transaction;
// it returns the marks of the messages no longer queued, forgetting them.
func (h *Hub) DropQueued(ctx context.Context, clients []string, cutoff, now time.Time) ([]string, error) {
	var ms []string
	err := h.transact(ctx, func(tx *Hub) error {
		for _, c := range clients {
			if _, err := tx.db.ExecContext(ctx,
				`INSERT INTO dropped (seq, client_id, msg_id, payload, enqueued_at, dropped_at)
				 SELECT seq, client_id, msg_id, payload, enqueued_at, ? FROM outbox WHERE client_id = ? AND enqueued_at <= ?`,
				now.UnixMilli(), c, cutoff.UnixMilli()); err != nil {
				return err
			}
			if _, err := tx.db.ExecContext(ctx, "DELETE FROM outbox WHERE client_id = ? AND enqueued_at <= ?", c, cutoff.UnixMilli()); err != nil {
				return err
			}
		}
		var err error
		ms, err = tx.Outbox.sweepMarks(ctx)
		return err
	})
	return ms, err
}

// Dropped returns the dropped messages whose notices are not all out, in
// seq order.
func (h *Hub) Dropped(ctx context.Context) ([]Dropped, error) {
	rows, err := h.db.QueryContext(ctx,
		"SELECT seq, client_id, msg_id, payload, enqueued_at, dropped_at, told, alerted FROM dropped ORDER BY seq")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ds []Dropped
	for rows.Next() {
		var d Dropped
		var enqueued, at int64
		if err := rows.Scan(&d.Seq, &d.Client, &d.MsgID, &d.Payload, &enqueued, &at, &d.Told, &d.Alerted); err != nil {
			return nil, err
		}
		d.Enqueued, d.At = time.UnixMilli(enqueued), time.UnixMilli(at)
		ds = append(ds, d)
	}
	return ds, rows.Err()
}

// Notified records which notices of the dropped message seq are out; once
// both are, the message is forgotten.
func (h *Hub) Notified(ctx context.Context, seq int64, told, alerted bool) error {
	var err error
	if told && alerted {
		_, err = h.db.ExecContext(ctx, "DELETE FROM dropped WHERE seq = ?", seq)
	} else {
		_, err = h.db.ExecContext(ctx, "UPDATE dropped SET told = ?, alerted = ? WHERE seq = ?", told, alerted, seq)
	}
	return err
}
