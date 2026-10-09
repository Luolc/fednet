package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
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
`, `
-- enqueued_at is in Unix milliseconds. Rows queued before this column
-- existed get the time of the migration.
ALTER TABLE outbox ADD COLUMN enqueued_at INTEGER NOT NULL DEFAULT 0;
UPDATE outbox SET enqueued_at = CAST(unixepoch('subsec') * 1000 AS INTEGER);
-- The client an uplink message came from; empty for rows from before.
ALTER TABLE inbox ADD COLUMN client_id TEXT NOT NULL DEFAULT '';
`, `
-- The clients allowed to connect. secret_hash is the SHA-256 of the
-- client's credential; the credential itself is never stored.
CREATE TABLE client (
	client_id   TEXT PRIMARY KEY,
	secret_hash BLOB NOT NULL,
	revoked     INTEGER NOT NULL DEFAULT 0,
	version     TEXT NOT NULL DEFAULT ''
);
`, `
-- The messages people posted in Slack that the hub has taken in, live or
-- by backfill, keyed by channel and ts; a live one also records the id of
-- the event that brought it. Either key repeating means the message was
-- taken in before.
CREATE TABLE slack_message (
	channel  TEXT NOT NULL,
	ts       TEXT NOT NULL,
	event_id TEXT UNIQUE,
	PRIMARY KEY (channel, ts)
);
-- Where the hub stands with Slack's history, by key: see the constants in
-- inbound.go.
CREATE TABLE slack_state (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
`, `
-- The approvals agents requested: the request, the card in Slack, and
-- how it ended. Times are Unix nanoseconds. status is pending until the
-- approval is approved, rejected or expired; card_final is set once the
-- card in Slack shows that.
CREATE TABLE approval (
	approval_id  TEXT PRIMARY KEY,
	client_id    TEXT NOT NULL,
	agent        TEXT NOT NULL,
	requester    TEXT NOT NULL DEFAULT '',
	summary      TEXT NOT NULL,
	action       BLOB NOT NULL,
	nonce        BLOB NOT NULL,
	requested_at INTEGER NOT NULL,
	expires_at   INTEGER NOT NULL,
	channel      TEXT NOT NULL,
	ts           TEXT NOT NULL,
	status       TEXT NOT NULL DEFAULT 'pending',
	decided_by   TEXT NOT NULL DEFAULT '',
	decided_at   INTEGER NOT NULL DEFAULT 0,
	card_final   INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX approval_status ON approval (status);
`, `
-- How many parts of a split post are in Slack already, so that neither a
-- retry nor another process posts them again.
ALTER TABLE inbox ADD COLUMN parts_sent INTEGER NOT NULL DEFAULT 0;
`, `
-- The name of the channel a thread is in, as Slack gave it when the
-- thread got its owner; empty when the hub could not read it, for a
-- direct message, and for threads owned before this column existed.
ALTER TABLE owner ADD COLUMN channel_name TEXT NOT NULL DEFAULT '';
`}

// ErrNotFound is returned when a looked-up row does not exist.
var ErrNotFound = errors.New("store: not found")

// Hub is the hub's database. A Hub that ReceiveSlack hands to its callback
// is bound to one transaction: its Outbox, its Inbox and the thread
// ownership it reads and writes are all part of that transaction.
type Hub struct {
	// db runs the queries: the database, or the transaction the Hub is
	// bound to.
	db dbtx
	// conn is the database itself, which opens transactions and closes.
	conn *sql.DB
	// Outbox holds the downlink messages for each client until it acks them.
	Outbox HubOutbox
	// Inbox holds the uplink messages received from clients.
	Inbox HubInbox
}

// dbtx is a *sql.DB or a *sql.Tx.
type dbtx interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// OpenHub opens, or creates, the hub database at path.
func OpenHub(ctx context.Context, path string) (*Hub, error) {
	db, err := open(ctx, path, hubMigrations)
	if err != nil {
		return nil, err
	}
	return &Hub{db: db, conn: db, Outbox: HubOutbox{db}, Inbox: HubInbox{Inbox{db}}}, nil
}

// Close closes the database.
func (h *Hub) Close() error { return h.conn.Close() }

// transact runs f in a transaction and commits it if f succeeds. On a Hub
// already bound to a transaction, f runs in that one and the caller
// commits.
func (h *Hub) transact(ctx context.Context, f func(tx *Hub) error) error {
	if _, ok := h.db.(*sql.Tx); ok {
		return f(h)
	}
	tx, err := h.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := f(&Hub{db: tx, conn: h.conn, Outbox: HubOutbox{tx}, Inbox: HubInbox{Inbox{tx}}}); err != nil {
		return err
	}
	return tx.Commit()
}

// ClaimAndEnqueue queues payload for the first message of thread, in one
// transaction: client becomes the owner of thread unless it already has one,
// and payload is queued for whichever client the owner then is. It returns
// that owner. Of several concurrent calls on a new thread, the first wins and
// all of them queue for its client. channelName is recorded with the owner
// it makes, as Claim records it.
func (h *Hub) ClaimAndEnqueue(ctx context.Context, thread, client, channelName string, payload []byte) (owner string, d Downlink, err error) {
	err = h.transact(ctx, func(tx *Hub) error {
		if _, err := tx.db.ExecContext(ctx,
			"INSERT INTO owner (thread, client_id, channel_name) VALUES (?, ?, ?) ON CONFLICT (thread) DO NOTHING",
			thread, client, channelName); err != nil {
			return err
		}
		if err := tx.db.QueryRowContext(ctx, "SELECT client_id FROM owner WHERE thread = ?", thread).Scan(&owner); err != nil {
			return err
		}
		d, err = enqueue(ctx, tx.db, owner, payload)
		return err
	})
	if err != nil {
		return "", Downlink{}, err
	}
	return owner, d, nil
}

// Claim makes client the owner of thread, a thread that has just been
// started and has no owner yet; it fails if thread already has one.
// channelName, the name of the thread's channel, is recorded with the
// owner and stays with the thread whoever owns it later.
func (h *Hub) Claim(ctx context.Context, thread, client, channelName string) error {
	_, err := h.db.ExecContext(ctx, "INSERT INTO owner (thread, client_id, channel_name) VALUES (?, ?, ?)", thread, client, channelName)
	return err
}

// Threads returns the threads client owns, in order of their keys.
func (h *Hub) Threads(ctx context.Context, client string) ([]string, error) {
	rows, err := h.db.QueryContext(ctx, "SELECT thread FROM owner WHERE client_id = ? ORDER BY thread", client)
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

// Reassign makes client the owner of thread, which must already have an
// owner; otherwise it returns ErrNotFound. This is the explicit takeover of
// one thread by another machine.
func (h *Hub) Reassign(ctx context.Context, thread, client string) error {
	res, err := h.db.ExecContext(ctx, "UPDATE owner SET client_id = ? WHERE thread = ?", client, thread)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err == nil && n == 0 {
		err = ErrNotFound
	}
	return err
}

// ReassignClient moves every thread owned by from to to, and returns how
// many threads it moved. Messages already queued for from stay with from.
func (h *Hub) ReassignClient(ctx context.Context, from, to string) (int64, error) {
	res, err := h.db.ExecContext(ctx, "UPDATE owner SET client_id = ? WHERE client_id = ?", to, from)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
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

// ChannelName returns the channel name recorded with thread's owner, or
// ErrNotFound when thread has no owner.
func (h *Hub) ChannelName(ctx context.Context, thread string) (string, error) {
	var name string
	err := h.db.QueryRowContext(ctx, "SELECT channel_name FROM owner WHERE thread = ?", thread).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	return name, err
}

// Registration is a client's row in the hub's registry.
type Registration struct {
	// SecretHash is the SHA-256 of the client's credential.
	SecretHash []byte
	// Revoked is set once the client has been retired; its credential no
	// longer identifies it.
	Revoked bool
	// Version is the version the client reported when it last connected,
	// empty until it has.
	Version string
}

// Register adds client with secretHash, or replaces the credential of a
// client already registered. Registering again also lifts a revocation.
func (h *Hub) Register(ctx context.Context, client string, secretHash []byte) error {
	_, err := h.db.ExecContext(ctx,
		`INSERT INTO client (client_id, secret_hash) VALUES (?, ?)
		 ON CONFLICT (client_id) DO UPDATE SET secret_hash = excluded.secret_hash, revoked = 0`,
		client, secretHash)
	return err
}

// Revoke retires client: its credential stops identifying it. It returns
// ErrNotFound if client is not registered.
func (h *Hub) Revoke(ctx context.Context, client string) error {
	res, err := h.db.ExecContext(ctx, "UPDATE client SET revoked = 1 WHERE client_id = ?", client)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err == nil && n == 0 {
		err = ErrNotFound
	}
	return err
}

// Registration returns client's registration, or ErrNotFound.
func (h *Hub) Registration(ctx context.Context, client string) (Registration, error) {
	var r Registration
	err := h.db.QueryRowContext(ctx,
		"SELECT secret_hash, revoked, version FROM client WHERE client_id = ?", client).
		Scan(&r.SecretHash, &r.Revoked, &r.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return Registration{}, ErrNotFound
	}
	return r, err
}

// SetVersion records the version client reported.
func (h *Hub) SetVersion(ctx context.Context, client, version string) error {
	_, err := h.db.ExecContext(ctx, "UPDATE client SET version = ? WHERE client_id = ?", version, client)
	return err
}

// HubInbox is the hub's inbox, which also records which client each message
// came from.
type HubInbox struct{ Inbox }

// PutFrom is Put for a message from client.
func (in HubInbox) PutFrom(ctx context.Context, client string, m Message) (bool, error) {
	res, err := in.db.ExecContext(ctx,
		"INSERT INTO inbox (msg_id, payload, client_id) VALUES (?, ?, ?) ON CONFLICT (msg_id) DO NOTHING",
		m.MsgID, m.Payload, client)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
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
type HubOutbox struct{ db dbtx }

// Enqueue queues payload for client under a new msg_id.
func (o HubOutbox) Enqueue(ctx context.Context, client string, payload []byte) (Downlink, error) {
	return enqueue(ctx, o.db, client, payload)
}

func enqueue(ctx context.Context, db dbtx, client string, payload []byte) (Downlink, error) {
	d := Downlink{Message: Message{MsgID: newMsgID(), Payload: payload}}
	res, err := db.ExecContext(ctx,
		"INSERT INTO outbox (client_id, msg_id, payload, enqueued_at) VALUES (?, ?, ?, ?)",
		client, d.MsgID, payload, time.Now().UnixMilli())
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

// QueuedFor reports whether client has a message that has waited in the
// outbox for d or longer.
func (o HubOutbox) QueuedFor(ctx context.Context, client string, d time.Duration) (bool, error) {
	var found bool
	err := o.db.QueryRowContext(ctx,
		"SELECT EXISTS (SELECT 1 FROM outbox WHERE client_id = ? AND enqueued_at <= ?)",
		client, time.Now().Add(-d).UnixMilli()).Scan(&found)
	return found, err
}
