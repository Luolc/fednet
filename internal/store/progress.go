package store

import (
	"context"
	"database/sql"
	"errors"
)

// OpenProgress returns the ts and the card, as JSON, of the progress card
// open in thread, or ErrNotFound when none is.
func (h *Hub) OpenProgress(ctx context.Context, thread string) (ts string, card []byte, err error) {
	err = h.db.QueryRowContext(ctx, "SELECT ts, card FROM progress WHERE thread = ?", thread).Scan(&ts, &card)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil, ErrNotFound
	}
	return ts, card, err
}

// SetProgress records card, as JSON, at ts as the progress card open in
// thread, in place of the one before.
func (h *Hub) SetProgress(ctx context.Context, thread, ts string, card []byte) error {
	_, err := h.db.ExecContext(ctx,
		"INSERT INTO progress (thread, ts, card) VALUES (?, ?, ?) ON CONFLICT (thread) DO UPDATE SET ts = excluded.ts, card = excluded.card",
		thread, ts, card)
	return err
}

// CloseProgress records that thread has no open progress card.
func (h *Hub) CloseProgress(ctx context.Context, thread string) error {
	_, err := h.db.ExecContext(ctx, "DELETE FROM progress WHERE thread = ?", thread)
	return err
}
