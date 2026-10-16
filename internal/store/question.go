package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/Luolc/fednet/internal/question"
)

// questionSchema holds the table of the question cards. It runs each time
// the hub's database is opened and is not one of hubMigrations, for the
// reason routeSchema is not: a hub of an earlier release must still open
// the database. Such a hub neither reads nor writes the table, so the
// snoozes in it wait for a hub that does.
const questionSchema = `
-- The question cards agents asked in threads. ref is the asking client's
-- own id of the question, one card per client and ref; card is the
-- question as JSON and mentions a JSON array of Slack user ids. ts and
-- permalink locate the card in its thread now: a snoozed card that comes
-- back is a new message lower in the thread, and moved_ts is the message
-- it was until that shows where it went. status is open, snoozed,
-- answered, closed or withdrawn; answer is the JSON the asking client
-- last got, note what it gave when it withdrew the card. card_final is
-- set while the card in Slack shows status. Times are Unix nanoseconds.
CREATE TABLE IF NOT EXISTS question (
	question_id  TEXT PRIMARY KEY,
	client_id    TEXT NOT NULL,
	ref          TEXT NOT NULL,
	thread       TEXT NOT NULL,
	card         BLOB NOT NULL,
	mentions     TEXT NOT NULL DEFAULT '[]',
	asked_at     INTEGER NOT NULL,
	ts           TEXT NOT NULL,
	permalink    TEXT NOT NULL DEFAULT '',
	status       TEXT NOT NULL DEFAULT 'open',
	answer       BLOB NOT NULL DEFAULT '',
	note         TEXT NOT NULL DEFAULT '',
	snooze_until INTEGER NOT NULL DEFAULT 0,
	card_final   INTEGER NOT NULL DEFAULT 1,
	moved_ts     TEXT NOT NULL DEFAULT '',
	UNIQUE (client_id, ref)
);
CREATE INDEX IF NOT EXISTS question_status ON question (status);
`

// Statuses of a Question besides the outcomes of an answer, which are
// question.Answered, Snoozed and Closed.
const (
	QuestionOpen      = "open"
	QuestionWithdrawn = "withdrawn"
)

// Question is a question card an agent asked, as the hub keeps it. Times
// read back are in UTC.
type Question struct {
	ID string
	// Client asked, with Ref its own id of the question, in Thread.
	Client string
	Ref    string
	Thread string
	// Card is the question, the JSON the agent handed in; Mentions the
	// people the card mentions.
	Card     []byte
	Mentions []string
	AskedAt  time.Time
	// TS and Permalink locate the card in its thread now; MovedTS is the
	// message the card was before it came back from a snooze, while that
	// does not show where it went yet, and empty otherwise.
	TS        string
	Permalink string
	MovedTS   string
	// Status is QuestionOpen, an outcome, or QuestionWithdrawn; Answer
	// the JSON the client last got, and Note what it gave on withdrawing.
	Status      string
	Answer      []byte
	Note        string
	SnoozeUntil time.Time
	// CardFinal is set while the card in Slack shows Status.
	CardFinal bool
}

// ErrExists is returned by PutQuestion for a ref the client has asked
// with before.
var ErrExists = errors.New("store: exists")

const questionColumns = "question_id, client_id, ref, thread, card, mentions, asked_at, ts, permalink, status, answer, note, snooze_until, card_final, moved_ts"

// PutQuestion records q, open and with its card final, unless q.Client
// has asked with q.Ref before, which is ErrExists.
func (h *Hub) PutQuestion(ctx context.Context, q Question) error {
	mentions, err := json.Marshal(append([]string{}, q.Mentions...))
	if err != nil {
		return err
	}
	res, err := h.db.ExecContext(ctx,
		"INSERT INTO question (question_id, client_id, ref, thread, card, mentions, asked_at, ts, permalink) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (client_id, ref) DO NOTHING",
		q.ID, q.Client, q.Ref, q.Thread, q.Card, mentions, q.AskedAt.UnixNano(), q.TS, q.Permalink)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return ErrExists
	}
	return nil
}

// Question returns the question with id, or ErrNotFound.
func (h *Hub) Question(ctx context.Context, id string) (Question, error) {
	return h.question(ctx, "WHERE question_id = ?", id)
}

// QuestionByRef returns the question client asked with ref, or
// ErrNotFound.
func (h *Hub) QuestionByRef(ctx context.Context, client, ref string) (Question, error) {
	return h.question(ctx, "WHERE client_id = ? AND ref = ?", client, ref)
}

func (h *Hub) question(ctx context.Context, where string, args ...any) (Question, error) {
	qs, err := h.questions(ctx, where, args...)
	if err != nil {
		return Question{}, err
	}
	if len(qs) == 0 {
		return Question{}, ErrNotFound
	}
	return qs[0], nil
}

// DueQuestions returns the snoozed questions whose snooze has ended by
// now, the earliest first.
func (h *Hub) DueQuestions(ctx context.Context, now time.Time) ([]Question, error) {
	return h.questions(ctx, "WHERE status = ? AND snooze_until <= ? ORDER BY snooze_until", question.Snoozed, now.UnixNano())
}

// UnfinishedQuestions returns the questions whose card does not show
// their status, or whose card before a snooze does not show where it
// went, oldest first.
func (h *Hub) UnfinishedQuestions(ctx context.Context) ([]Question, error) {
	return h.questions(ctx, "WHERE card_final = 0 OR moved_ts <> '' ORDER BY asked_at")
}

func (h *Hub) questions(ctx context.Context, where string, args ...any) ([]Question, error) {
	rows, err := h.db.QueryContext(ctx, "SELECT "+questionColumns+" FROM question "+where, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var qs []Question
	for rows.Next() {
		var q Question
		var mentions string
		var asked, until int64
		if err := rows.Scan(&q.ID, &q.Client, &q.Ref, &q.Thread, &q.Card, &mentions, &asked, &q.TS, &q.Permalink, &q.Status, &q.Answer, &q.Note, &until, &q.CardFinal, &q.MovedTS); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(mentions), &q.Mentions); err != nil {
			return nil, err
		}
		q.AskedAt = time.Unix(0, asked).UTC()
		if until != 0 {
			q.SnoozeUntil = time.Unix(0, until).UTC()
		}
		qs = append(qs, q)
	}
	return qs, rows.Err()
}

// ChangeQuestion changes the question with id, in one transaction that
// holds the database's write lock from its start: it reads the question
// and runs change against it and a Hub bound to the same transaction;
// when change reports true, the question as change left it is written
// back, so what change queues and the new state commit together. It
// reports whether the question was written.
func (h *Hub) ChangeQuestion(ctx context.Context, id string, change func(tx *Hub, q *Question) (bool, error)) (changed bool, err error) {
	err = h.transact(ctx, func(tx *Hub) error {
		q, err := tx.Question(ctx, id)
		if err != nil {
			return err
		}
		if changed, err = change(tx, &q); err != nil || !changed {
			return err
		}
		var until int64
		if !q.SnoozeUntil.IsZero() {
			until = q.SnoozeUntil.UnixNano()
		}
		if q.Answer == nil {
			q.Answer = []byte{}
		}
		_, err = tx.db.ExecContext(ctx,
			"UPDATE question SET ts = ?, permalink = ?, moved_ts = ?, status = ?, answer = ?, note = ?, snooze_until = ?, card_final = ? WHERE question_id = ?",
			q.TS, q.Permalink, q.MovedTS, q.Status, q.Answer, q.Note, until, q.CardFinal, id)
		return err
	})
	return changed && err == nil, err
}
