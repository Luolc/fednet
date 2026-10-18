package store

import (
	"context"
	"database/sql"
	"errors"
)

// relaySchema runs each time the hub's database is opened, as routeSchema
// does and for the same reason: a hub of an earlier release still opens
// the database, and neither reads nor writes this table.
const relaySchema = `
-- How many relays in a row each thread has had since a person last
-- posted in it; a thread with none has no row.
CREATE TABLE IF NOT EXISTS relay_hop (
	thread TEXT PRIMARY KEY,
	hop    INTEGER NOT NULL
);
`

// Hop returns how many relays in a row thread has had since a person last
// posted in it.
func (h *Hub) Hop(ctx context.Context, thread string) (int, error) {
	var hop int
	err := h.db.QueryRowContext(ctx, "SELECT hop FROM relay_hop WHERE thread = ?", thread).Scan(&hop)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return hop, err
}

// SetHop records hop as thread's relays in a row; zero, as when a person
// posts in it, clears them.
func (h *Hub) SetHop(ctx context.Context, thread string, hop int) error {
	if hop == 0 {
		_, err := h.db.ExecContext(ctx, "DELETE FROM relay_hop WHERE thread = ?", thread)
		return err
	}
	_, err := h.db.ExecContext(ctx, "INSERT INTO relay_hop (thread, hop) VALUES (?, ?) ON CONFLICT (thread) DO UPDATE SET hop = excluded.hop", thread, hop)
	return err
}

// Transact runs f in a transaction, on a Hub bound to it, and commits it
// if f succeeds. On a Hub already bound to a transaction, f runs in that
// one.
func (h *Hub) Transact(ctx context.Context, f func(tx *Hub) error) error {
	return h.transact(ctx, f)
}
