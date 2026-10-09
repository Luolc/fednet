package store

import (
	"context"
	"database/sql"
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
`}

// Client is the client's database.
type Client struct {
	db *sql.DB
	// Inbox holds the downlink messages received from the hub.
	Inbox Inbox
	// Outbox holds the uplink messages until the hub confirms them.
	Outbox ClientOutbox
}

// OpenClient opens, or creates, the client database at path.
func OpenClient(ctx context.Context, path string) (*Client, error) {
	db, err := open(ctx, path, clientMigrations)
	if err != nil {
		return nil, err
	}
	return &Client{db: db, Inbox: Inbox{db}, Outbox: ClientOutbox{db}}, nil
}

// Close closes the database.
func (c *Client) Close() error { return c.db.Close() }

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
