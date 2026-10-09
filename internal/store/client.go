package store

import (
	"context"
	"database/sql"
	"time"
)

var clientMigrations = []string{`
CREATE TABLE inbox (
	msg_id    TEXT PRIMARY KEY,
	payload   BLOB NOT NULL,
	delivered INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE outbox (
	msg_id  TEXT PRIMARY KEY,
	payload BLOB NOT NULL
);
`, `
-- Retry state of the hook. attempts is how many times the hook has run for
-- the message, next_attempt_at (Unix milliseconds) when it may run next,
-- delivered_at (Unix milliseconds) when it exited 0. Rows delivered before
-- this column existed get the time of the migration.
ALTER TABLE inbox ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE inbox ADD COLUMN next_attempt_at INTEGER NOT NULL DEFAULT 0;
ALTER TABLE inbox ADD COLUMN delivered_at INTEGER NOT NULL DEFAULT 0;
UPDATE inbox SET delivered_at = CAST(unixepoch('subsec') * 1000 AS INTEGER) WHERE delivered = 1;
-- Messages the hook could not deliver within the retry limit. They stay
-- here, are not retried on their own, and still count for dedup.
CREATE TABLE dead_letter (
	msg_id   TEXT PRIMARY KEY,
	payload  BLOB NOT NULL,
	attempts INTEGER NOT NULL,
	reason   TEXT NOT NULL,
	dead_at  INTEGER NOT NULL
);
`}

// Client is the client's database.
type Client struct {
	db *sql.DB
	// Inbox holds the downlink messages received from the hub.
	Inbox ClientInbox
	// Outbox holds the uplink messages until the hub confirms them.
	Outbox ClientOutbox
}

// OpenClient opens, or creates, the client database at path.
func OpenClient(ctx context.Context, path string) (*Client, error) {
	db, err := open(ctx, path, clientMigrations)
	if err != nil {
		return nil, err
	}
	return &Client{db: db, Inbox: ClientInbox{Inbox{db}, db}, Outbox: ClientOutbox{db}}, nil
}

// Close closes the database.
func (c *Client) Close() error { return c.db.Close() }

// ClientInbox is the client's inbox, which also tracks the hook's attempts
// at each message and keeps the messages the hook gave up on.
type ClientInbox struct {
	Inbox
	// conn opens the transactions Bury needs.
	conn *sql.DB
}

// Queued is an undelivered inbox message with the hook's retry state.
type Queued struct {
	Message
	// Attempts is how many times the hook has run for the message.
	Attempts int
	// NextAttempt is when the hook may run next; the epoch for a message
	// the hook has not failed yet.
	NextAttempt time.Time
}

// DeadLetter is a message the hook could not deliver within the retry limit.
type DeadLetter struct {
	Message
	// Attempts is how many times the hook ran, counting the last one.
	Attempts int
	// Reason is why the last attempt failed.
	Reason string
	// At is when the message was moved here.
	At time.Time
}

// Put stores a message unless one with the same msg_id is already in the
// inbox or among the dead letters, and reports whether it was new.
func (in ClientInbox) Put(ctx context.Context, m Message) (bool, error) {
	res, err := in.db.ExecContext(ctx,
		`INSERT INTO inbox (msg_id, payload) SELECT ?, ?
		 WHERE NOT EXISTS (SELECT 1 FROM dead_letter WHERE msg_id = ?)
		 ON CONFLICT (msg_id) DO NOTHING`,
		m.MsgID, m.Payload, m.MsgID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// Queued returns the undelivered messages with their retry state, oldest
// first, due or not.
func (in ClientInbox) Queued(ctx context.Context) ([]Queued, error) {
	rows, err := in.db.QueryContext(ctx,
		"SELECT msg_id, payload, attempts, next_attempt_at FROM inbox WHERE delivered = 0 ORDER BY rowid")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var qs []Queued
	for rows.Next() {
		var q Queued
		var at int64
		if err := rows.Scan(&q.MsgID, &q.Payload, &q.Attempts, &at); err != nil {
			return nil, err
		}
		q.NextAttempt = time.UnixMilli(at)
		qs = append(qs, q)
	}
	return qs, rows.Err()
}

// MarkDelivered records that the hook delivered the message, and when.
func (in ClientInbox) MarkDelivered(ctx context.Context, msgID string) error {
	_, err := in.db.ExecContext(ctx,
		"UPDATE inbox SET delivered = 1, delivered_at = ? WHERE msg_id = ?", time.Now().UnixMilli(), msgID)
	return err
}

// Retry records a failed attempt at the message and when the next may run.
func (in ClientInbox) Retry(ctx context.Context, msgID string, next time.Time) error {
	_, err := in.db.ExecContext(ctx,
		"UPDATE inbox SET attempts = attempts + 1, next_attempt_at = ? WHERE msg_id = ?", next.UnixMilli(), msgID)
	return err
}

// Bury moves the message to the dead letters after a last failed attempt,
// recording reason. It returns ErrNotFound if the message is not in the
// inbox.
func (in ClientInbox) Bury(ctx context.Context, msgID, reason string) error {
	tx, err := in.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx,
		`INSERT INTO dead_letter (msg_id, payload, attempts, reason, dead_at)
		 SELECT msg_id, payload, attempts + 1, ?, ? FROM inbox WHERE msg_id = ?`,
		reason, time.Now().UnixMilli(), msgID)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return ErrNotFound
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM inbox WHERE msg_id = ?", msgID); err != nil {
		return err
	}
	return tx.Commit()
}

// DeadLetters returns the dead letters, oldest first.
func (in ClientInbox) DeadLetters(ctx context.Context) ([]DeadLetter, error) {
	rows, err := in.db.QueryContext(ctx,
		"SELECT msg_id, payload, attempts, reason, dead_at FROM dead_letter ORDER BY dead_at, rowid")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ds []DeadLetter
	for rows.Next() {
		var d DeadLetter
		var at int64
		if err := rows.Scan(&d.MsgID, &d.Payload, &d.Attempts, &d.Reason, &at); err != nil {
			return nil, err
		}
		d.At = time.UnixMilli(at)
		ds = append(ds, d)
	}
	return ds, rows.Err()
}

// Prune deletes the messages delivered before t, which the inbox no longer
// needs for dedup, and returns how many it deleted.
func (in ClientInbox) Prune(ctx context.Context, t time.Time) (int64, error) {
	res, err := in.db.ExecContext(ctx,
		"DELETE FROM inbox WHERE delivered = 1 AND delivered_at < ?", t.UnixMilli())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// ClientOutbox is the client's outbox toward the hub.
type ClientOutbox struct{ db *sql.DB }

// Enqueue queues payload under a new msg_id and returns the msg_id.
func (o ClientOutbox) Enqueue(ctx context.Context, payload []byte) (string, error) {
	id := newMsgID()
	if _, err := o.db.ExecContext(ctx, "INSERT INTO outbox (msg_id, payload) VALUES (?, ?)", id, payload); err != nil {
		return "", err
	}
	return id, nil
}

// Pending returns the messages the hub has not confirmed, oldest first.
func (o ClientOutbox) Pending(ctx context.Context) ([]Message, error) {
	rows, err := o.db.QueryContext(ctx, "SELECT msg_id, payload FROM outbox ORDER BY rowid")
	if err != nil {
		return nil, err
	}
	return scanMessages(rows)
}

// Ack removes a message the hub has confirmed.
func (o ClientOutbox) Ack(ctx context.Context, msgID string) error {
	_, err := o.db.ExecContext(ctx, "DELETE FROM outbox WHERE msg_id = ?", msgID)
	return err
}
