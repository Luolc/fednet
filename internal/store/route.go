package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// routeSchema holds the tables of the routes and of the requests to
// change them. It runs each time the hub's database is opened and is not
// one of hubMigrations: a hub of an earlier release refuses a database
// whose user_version is newer than it knows, and must still open this one
// when the hub is rolled back to it. Every statement only creates what is
// missing, and an earlier hub neither reads nor writes these tables.
const routeSchema = `
-- Which client takes the new threads of each channel, by Slack channel
-- id, or of direct messages, under DMRoute; machine is empty when no
-- client does. scope goes down with each message of a thread the route
-- hands over; open_thread is a JSON array of the clients that may open
-- threads in the channel.
CREATE TABLE IF NOT EXISTS route (
	channel     TEXT PRIMARY KEY,
	machine     TEXT NOT NULL,
	scope       TEXT NOT NULL DEFAULT '',
	open_thread TEXT NOT NULL DEFAULT '[]'
);
-- Every change of the routes: the import from the config, which has an
-- empty request_id, or an approved route request, with who asked and who
-- approved. before and after are JSON arrays of the routes it changed, as
-- they were and as they are; at is in Unix nanoseconds. posted is set
-- once the change is told in the approvals channel.
CREATE TABLE IF NOT EXISTS route_change (
	seq        INTEGER PRIMARY KEY AUTOINCREMENT,
	request_id TEXT NOT NULL DEFAULT '',
	client_id  TEXT NOT NULL DEFAULT '',
	agent      TEXT NOT NULL DEFAULT '',
	approver   TEXT NOT NULL DEFAULT '',
	at         INTEGER NOT NULL,
	before     TEXT NOT NULL,
	after      TEXT NOT NULL,
	posted     INTEGER NOT NULL DEFAULT 0
);
-- The requests to change a route, kept as the approval table keeps
-- approvals; action is the change as JSON. They are not in that table so
-- that an earlier hub, which would sign them as approvals, never sees
-- them.
CREATE TABLE IF NOT EXISTS route_request (
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
CREATE INDEX IF NOT EXISTS route_request_status ON route_request (status);
-- The requests to create a channel with a route, kept as route_request
-- keeps its requests, with action the channel and its route as JSON, and
-- what came of carrying out one approved: attempted is set before the
-- hub asks Slack to create the channel, created to the id Slack gave as
-- soon as it gave it, done once the request is carried out, and note
-- says what came of it. They are not in route_request so that a hub of
-- the release before, which would take them for route changes, never
-- sees them.
CREATE TABLE IF NOT EXISTS channel_request (
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
	card_final   INTEGER NOT NULL DEFAULT 0,
	attempted    INTEGER NOT NULL DEFAULT 0,
	created      TEXT NOT NULL DEFAULT '',
	done         INTEGER NOT NULL DEFAULT 0,
	note         TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS channel_request_status ON channel_request (status);
-- The scope of each thread that had one when it got its owner.
CREATE TABLE IF NOT EXISTS thread_scope (
	thread TEXT PRIMARY KEY,
	scope  TEXT NOT NULL
);
`

// DMRoute is the channel of the route of direct messages: no Slack
// channel id is in lower case.
const DMRoute = "dm"

// Route is where the new threads of a channel go.
type Route struct {
	// Channel is the Slack channel id, or DMRoute; empty in a request
	// to create the channel.
	Channel string `json:"channel,omitempty"`
	// Machine is the client that takes the channel's new threads; empty
	// when none does.
	Machine string `json:"machine,omitempty"`
	// Scope goes down with every message of a thread the route hands
	// over; empty means none.
	Scope string `json:"scope,omitempty"`
	// OpenThread lists the clients that may open threads in the channel.
	OpenThread []string `json:"open_thread,omitempty"`
}

// Route returns the route of channel, or ErrNotFound.
func (h *Hub) Route(ctx context.Context, channel string) (Route, error) {
	rs, err := h.routes(ctx, "WHERE channel = ?", channel)
	if err != nil {
		return Route{}, err
	}
	if len(rs) == 0 {
		return Route{}, ErrNotFound
	}
	return rs[0], nil
}

// Routes returns every route, in order of channel.
func (h *Hub) Routes(ctx context.Context) ([]Route, error) {
	return h.routes(ctx, "ORDER BY channel")
}

func (h *Hub) routes(ctx context.Context, where string, args ...any) ([]Route, error) {
	rows, err := h.db.QueryContext(ctx, "SELECT channel, machine, scope, open_thread FROM route "+where, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var rs []Route
	for rows.Next() {
		var r Route
		var open string
		if err := rows.Scan(&r.Channel, &r.Machine, &r.Scope, &open); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(open), &r.OpenThread); err != nil {
			return nil, err
		}
		if len(r.OpenThread) == 0 {
			r.OpenThread = nil
		}
		rs = append(rs, r)
	}
	return rs, rows.Err()
}

// putRoute writes r over the route of its channel.
func (h *Hub) putRoute(ctx context.Context, r Route) error {
	open, err := json.Marshal(r.OpenThread)
	if err != nil {
		return err
	}
	if r.OpenThread == nil {
		open = []byte("[]")
	}
	_, err = h.db.ExecContext(ctx,
		`INSERT INTO route (channel, machine, scope, open_thread) VALUES (?, ?, ?, ?)
		 ON CONFLICT (channel) DO UPDATE SET machine = excluded.machine, scope = excluded.scope, open_thread = excluded.open_thread`,
		r.Channel, r.Machine, r.Scope, string(open))
	return err
}

// RouteChange is one change of the routes, as route_change keeps it.
type RouteChange struct {
	Seq int64
	// RequestID is the route request approved; empty for the import.
	RequestID string
	// Client and Agent asked for the change, Approver approved it; all
	// empty for the import.
	Client, Agent, Approver string
	At                      time.Time
	// Before and After are the routes the change touched, as they were
	// and as they are: a route added is only in After, one deleted only
	// in Before.
	Before, After []Route
}

// ImportRoutes makes routes the hub's routes, in one transaction, unless
// the routes have been set before: by an import, or by a change, even one
// that left no route. It reports whether it imported; routes empty
// imports nothing.
func (h *Hub) ImportRoutes(ctx context.Context, routes []Route, at time.Time) (imported bool, err error) {
	if len(routes) == 0 {
		return false, nil
	}
	err = h.transact(ctx, func(tx *Hub) error {
		var n int
		if err := tx.db.QueryRowContext(ctx, "SELECT (SELECT count(*) FROM route) + (SELECT count(*) FROM route_change)").Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			return nil
		}
		for _, r := range routes {
			if err := tx.putRoute(ctx, r); err != nil {
				return err
			}
		}
		imported = true
		return tx.recordChange(ctx, RouteChange{At: at, Before: []Route{}, After: routes})
	})
	return imported && err == nil, err
}

// SetRoute writes after over the route of channel, or deletes that route
// when after is nil, and records the change, c with Before and After
// filled in. A Hub bound to a transaction does both in it.
func (h *Hub) SetRoute(ctx context.Context, channel string, after *Route, c RouteChange) error {
	return h.transact(ctx, func(tx *Hub) error {
		c.Before, c.After = []Route{}, []Route{}
		before, err := tx.Route(ctx, channel)
		switch {
		case err == nil:
			c.Before = []Route{before}
		case !errors.Is(err, ErrNotFound):
			return err
		}
		if after == nil {
			if _, err := tx.db.ExecContext(ctx, "DELETE FROM route WHERE channel = ?", channel); err != nil {
				return err
			}
		} else {
			if err := tx.putRoute(ctx, *after); err != nil {
				return err
			}
			c.After = []Route{*after}
		}
		return tx.recordChange(ctx, c)
	})
}

func (h *Hub) recordChange(ctx context.Context, c RouteChange) error {
	before, err := json.Marshal(c.Before)
	if err != nil {
		return err
	}
	after, err := json.Marshal(c.After)
	if err != nil {
		return err
	}
	_, err = h.db.ExecContext(ctx,
		"INSERT INTO route_change (request_id, client_id, agent, approver, at, before, after) VALUES (?, ?, ?, ?, ?, ?, ?)",
		c.RequestID, c.Client, c.Agent, c.Approver, c.At.UnixNano(), string(before), string(after))
	return err
}

// UnpostedRouteChanges returns the changes not yet told in the approvals
// channel, oldest first.
func (h *Hub) UnpostedRouteChanges(ctx context.Context) ([]RouteChange, error) {
	rows, err := h.db.QueryContext(ctx,
		"SELECT seq, request_id, client_id, agent, approver, at, before, after FROM route_change WHERE posted = 0 ORDER BY seq")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cs []RouteChange
	for rows.Next() {
		var c RouteChange
		var at int64
		var before, after string
		if err := rows.Scan(&c.Seq, &c.RequestID, &c.Client, &c.Agent, &c.Approver, &at, &before, &after); err != nil {
			return nil, err
		}
		c.At = time.Unix(0, at).UTC()
		if err := json.Unmarshal([]byte(before), &c.Before); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(after), &c.After); err != nil {
			return nil, err
		}
		cs = append(cs, c)
	}
	return cs, rows.Err()
}

// MarkRouteChangePosted records that the change seq is told in the
// approvals channel.
func (h *Hub) MarkRouteChangePosted(ctx context.Context, seq int64) error {
	_, err := h.db.ExecContext(ctx, "UPDATE route_change SET posted = 1 WHERE seq = ?", seq)
	return err
}

// ThreadScope returns the scope recorded with thread's owner; empty when
// it has none, or has no owner.
func (h *Hub) ThreadScope(ctx context.Context, thread string) (string, error) {
	var scope string
	err := h.db.QueryRowContext(ctx, "SELECT scope FROM thread_scope WHERE thread = ?", thread).Scan(&scope)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return scope, err
}

// putScope records scope with thread, unless scope is empty.
func putScope(ctx context.Context, db dbtx, thread, scope string) error {
	if scope == "" {
		return nil
	}
	_, err := db.ExecContext(ctx, "INSERT INTO thread_scope (thread, scope) VALUES (?, ?) ON CONFLICT (thread) DO NOTHING", thread, scope)
	return err
}

// ChannelRequest is what came of carrying out an approved request to
// create a channel.
type ChannelRequest struct {
	ID string
	// Attempted is set once the hub has asked Slack to create the
	// channel, or is about to; Created is the id Slack gave, once it gave
	// one; Done is set once the request is carried out.
	Attempted bool
	Created   string
	Done      bool
	// Note says what came of the request.
	Note string
}

// ChannelRequest returns what came of the request id to create a channel,
// or ErrNotFound when there is no such request.
func (h *Hub) ChannelRequest(ctx context.Context, id string) (ChannelRequest, error) {
	cs, err := h.channelRequests(ctx, "WHERE approval_id = ?", id)
	if err != nil {
		return ChannelRequest{}, err
	}
	if len(cs) == 0 {
		return ChannelRequest{}, ErrNotFound
	}
	return cs[0], nil
}

// ChannelRequestsToDo returns the approved requests to create a channel
// not carried out yet, oldest decision first.
func (h *Hub) ChannelRequestsToDo(ctx context.Context) ([]ChannelRequest, error) {
	return h.channelRequests(ctx, "WHERE status = 'approved' AND done = 0 ORDER BY decided_at")
}

func (h *Hub) channelRequests(ctx context.Context, where string, args ...any) ([]ChannelRequest, error) {
	rows, err := h.db.QueryContext(ctx, "SELECT approval_id, attempted, created, done, note FROM channel_request "+where, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cs []ChannelRequest
	for rows.Next() {
		var c ChannelRequest
		if err := rows.Scan(&c.ID, &c.Attempted, &c.Created, &c.Done, &c.Note); err != nil {
			return nil, err
		}
		cs = append(cs, c)
	}
	return cs, rows.Err()
}

// MarkChannelAttempted records that the hub asks Slack to create the
// channel of request id.
func (h *Hub) MarkChannelAttempted(ctx context.Context, id string) error {
	_, err := h.db.ExecContext(ctx, "UPDATE channel_request SET attempted = 1 WHERE approval_id = ?", id)
	return err
}

// MarkChannelCreated records channel, the id Slack gave, as the channel
// request id created.
func (h *Hub) MarkChannelCreated(ctx context.Context, id, channel string) error {
	_, err := h.db.ExecContext(ctx, "UPDATE channel_request SET created = ? WHERE approval_id = ?", channel, id)
	return err
}

// FinishChannelRequest records that request id is carried out, with
// note, and runs then against a Hub bound to the same transaction, so
// that what then writes commits with it. A request carried out before is
// left as it is, and then does not run.
func (h *Hub) FinishChannelRequest(ctx context.Context, id, note string, then func(tx *Hub) error) error {
	return h.transact(ctx, func(tx *Hub) error {
		res, err := tx.db.ExecContext(ctx, "UPDATE channel_request SET done = 1, note = ? WHERE approval_id = ? AND done = 0", note, id)
		if err != nil {
			return err
		}
		if n, err := res.RowsAffected(); err != nil || n == 0 {
			return err
		}
		return then(tx)
	})
}
