package store

import (
	"context"
	"database/sql"
	"errors"
)

var hubMigrations = []string{`
CREATE TABLE outbox (
	seq       INTEGER PRIMARY KEY AUTOINCREMENT,
	client_id TEXT NOT NULL,
	msg_id    TEXT NOT NULL UNIQUE,
	payload   BLOB NOT NULL
);
CREATE INDEX outbox_client_seq ON outbox (client_id, seq);
CREATE TABLE inbox (
	msg_id    TEXT PRIMARY KEY,
	payload   BLOB NOT NULL,
	delivered INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE owner (
	thread    TEXT PRIMARY KEY,
	client_id TEXT NOT NULL
);
`}

// ErrNotFound is returned when a looked-up row does not exist.
var ErrNotFound = errors.New("store: not found")

// Hub is the hub's database.
type Hub struct {
	db *sql.DB
	// Outbox holds the downlink messages for each client until it acks them.
	Outbox HubOutbox
	// Inbox holds the uplink messages received from clients.
	Inbox Inbox
}

// OpenHub opens, or creates, the hub database at path.
func OpenHub(ctx context.Context, path string) (*Hub, error) {
	db, err := open(ctx, path, hubMigrations)
	if err != nil {
		return nil, err
	}
	return &Hub{db: db, Outbox: HubOutbox{db}, Inbox: Inbox{db}}, nil
}

// Close closes the database.
func (h *Hub) Close() error { return h.db.Close() }

// SetOwner records client as the owner of thread, replacing any earlier owner.
func (h *Hub) SetOwner(ctx context.Context, thread, client string) error {
	_, err := h.db.ExecContext(ctx,
		"INSERT INTO owner (thread, client_id) VALUES (?, ?) ON CONFLICT (thread) DO UPDATE SET client_id = excluded.client_id",
		thread, client)
	return err
}

// Owner returns the client that owns thread, or ErrNotFound.
func (h *Hub) Owner(ctx context.Context, thread string) (string, error) {
	var client string
	err := h.db.QueryRowContext(ctx, "SELECT client_id FROM owner WHERE thread = ?", thread).Scan(&client)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	return client, err
}

// Downlink is a message queued for one client.
type Downlink struct {
	// Seq increases with every enqueue. It is shared by all clients, so one
	// client's sequence has gaps; it never goes back, even after the outbox
	// is emptied.
	Seq int64
	Message
}

// HubOutbox is the hub's outbox, one queue per client.
type HubOutbox struct{ db *sql.DB }

// Enqueue queues payload for client under a new msg_id.
func (o HubOutbox) Enqueue(ctx context.Context, client string, payload []byte) (Downlink, error) {
	d := Downlink{Message: Message{MsgID: newMsgID(), Payload: payload}}
	res, err := o.db.ExecContext(ctx,
		"INSERT INTO outbox (client_id, msg_id, payload) VALUES (?, ?, ?)",
		client, d.MsgID, payload)
	if err != nil {
		return Downlink{}, err
	}
	d.Seq, err = res.LastInsertId()
	return d, err
}

// After returns client's queued messages with a seq greater than seq, in seq
// order. After(ctx, client, 0) returns everything client has not acked.
func (o HubOutbox) After(ctx context.Context, client string, seq int64) ([]Downlink, error) {
	rows, err := o.db.QueryContext(ctx,
		"SELECT seq, msg_id, payload FROM outbox WHERE client_id = ? AND seq > ? ORDER BY seq",
		client, seq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ds []Downlink
	for rows.Next() {
		var d Downlink
		if err := rows.Scan(&d.Seq, &d.MsgID, &d.Payload); err != nil {
			return nil, err
		}
		ds = append(ds, d)
	}
	return ds, rows.Err()
}

// Ack removes client's queued messages up to and including seq.
func (o HubOutbox) Ack(ctx context.Context, client string, seq int64) error {
	_, err := o.db.ExecContext(ctx, "DELETE FROM outbox WHERE client_id = ? AND seq <= ?", client, seq)
	return err
}
