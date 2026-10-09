package store

import "context"

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

// SetPartsSent records that the first n parts of msgID are in Slack.
func (in HubInbox) SetPartsSent(ctx context.Context, msgID string, n int) error {
	_, err := in.db.ExecContext(ctx, "UPDATE inbox SET parts_sent = ? WHERE msg_id = ?", n, msgID)
	return err
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
