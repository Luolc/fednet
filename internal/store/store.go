// Package store is the SQLite persistence for the hub and the client: the
// outbox and inbox on each side, and the hub's thread ownership. Every write
// returns only after its transaction has committed, so a caller may ack a
// message as soon as the write returns.
package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"net/url"

	_ "modernc.org/sqlite" // registers the "sqlite" driver
)

// Message is one message in an inbox or in the client outbox.
type Message struct {
	MsgID   string
	Payload []byte
}

// open opens the database at path and brings its schema up to date.
func open(ctx context.Context, path string, migrations []string) (*sql.DB, error) {
	// WAL lets the client daemon and a short-lived `fednet client post`
	// process use the same file at once; busy_timeout makes one wait for the
	// other's lock instead of failing. With _txlock=immediate a transaction
	// takes the write lock when it begins, so two writers cannot deadlock
	// upgrading from a read lock.
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Set("_txlock", "immediate")
	db, err := sql.Open("sqlite", "file:"+path+"?"+q.Encode())
	if err != nil {
		return nil, err
	}
	if err := migrate(ctx, db, migrations); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// migrate runs, in one transaction, the migrations past the version recorded
// in the database's user_version, then records the new version.
func migrate(ctx context.Context, db *sql.DB, migrations []string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var v int
	if err := tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&v); err != nil {
		return err
	}
	if v > len(migrations) {
		return fmt.Errorf("store: schema version %d is newer than this binary knows (%d)", v, len(migrations))
	}
	if v == len(migrations) {
		return nil
	}
	for i := v; i < len(migrations); i++ {
		if _, err := tx.ExecContext(ctx, migrations[i]); err != nil {
			return fmt.Errorf("store: migration %d: %w", i+1, err)
		}
	}
	// PRAGMA does not take bound parameters.
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", len(migrations))); err != nil {
		return err
	}
	return tx.Commit()
}

// Inbox is the receiving side of a channel. The msg_id is unique, so a
// redelivered message is stored once.
type Inbox struct{ db *sql.DB }

// Put stores a message unless one with the same msg_id is already there, and
// reports whether it was new.
func (in Inbox) Put(ctx context.Context, m Message) (bool, error) {
	res, err := in.db.ExecContext(ctx,
		"INSERT INTO inbox (msg_id, payload) VALUES (?, ?) ON CONFLICT (msg_id) DO NOTHING",
		m.MsgID, m.Payload)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// Undelivered returns the messages not yet marked delivered, oldest first.
func (in Inbox) Undelivered(ctx context.Context) ([]Message, error) {
	rows, err := in.db.QueryContext(ctx,
		"SELECT msg_id, payload FROM inbox WHERE delivered = 0 ORDER BY rowid")
	if err != nil {
		return nil, err
	}
	return scanMessages(rows)
}

// MarkDelivered records that the message has been handed on.
func (in Inbox) MarkDelivered(ctx context.Context, msgID string) error {
	_, err := in.db.ExecContext(ctx, "UPDATE inbox SET delivered = 1 WHERE msg_id = ?", msgID)
	return err
}

func scanMessages(rows *sql.Rows) ([]Message, error) {
	defer rows.Close()
	var ms []Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.MsgID, &m.Payload); err != nil {
			return nil, err
		}
		ms = append(ms, m)
	}
	return ms, rows.Err()
}

func newMsgID() string { return rand.Text() }
