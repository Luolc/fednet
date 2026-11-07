package watch

import (
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/slack"
)

// drain runs the reactions m has waiting, as Run would.
func drain(t *testing.T, m *Marks) {
	t.Helper()
	m.init()
	for len(m.jobs) > 0 {
		(<-m.jobs)(t.Context())
	}
}

// post starts a thread in C1 as a person and queues the message for
// client, as the inbound side does, telling marks; it returns the ts and
// the seq the message is queued under.
func (f *fixture) post(m *Marks, client string) (string, int64) {
	f.t.Helper()
	ts, err := f.fake.Start("C1", "U1", "please fix the build")
	if err != nil {
		f.t.Fatal(err)
	}
	b, _ := json.Marshal(payload.Message{Type: payload.Inbound, Thread: slack.ThreadKey("C1", ts), TS: ts, Text: "please fix the build"})
	d, err := f.w.Store.Outbox.Enqueue(f.t.Context(), client, b)
	if err != nil {
		f.t.Fatal(err)
	}
	m.Queued(client, d.MsgID, "C1", ts)
	return ts, d.Seq
}

func (f *fixture) marks() *Marks {
	return &Marks{Store: f.w.Store, Slack: f.fake, Online: func(c string) bool { return f.online[c] }}
}

func (f *fixture) hourglass(ts string) bool {
	return slices.Contains(f.fake.Reactions("C1", ts), Hourglass)
}

// A message queued for an offline client gets the hourglass, and loses it
// once the client acks it; one for an online client never gets it.
func TestHourglass(t *testing.T) {
	f := newFixture(t)
	m := f.marks()
	f.online["workstation"] = true
	tsOn, _ := f.post(m, "workstation")
	tsOff, seq := f.post(m, "laptop")
	drain(t, m)
	if f.hourglass(tsOn) {
		t.Fatal("a message for an online client got the hourglass")
	}
	if !f.hourglass(tsOff) {
		t.Fatal("a message for an offline client has no hourglass")
	}
	marks, err := f.w.Store.Outbox.Ack(t.Context(), "laptop", seq)
	if err != nil {
		t.Fatal(err)
	}
	m.Done(marks)
	drain(t, m)
	if f.hourglass(tsOff) {
		t.Fatalf("the hourglass stayed after the ack (marks %q)", marks)
	}
}

// A message the client acks before the hourglass is recorded loses it
// again.
func TestHourglassAfterAck(t *testing.T) {
	f := newFixture(t)
	m := f.marks()
	ts, seq := f.post(m, "laptop")
	if _, err := f.w.Store.Outbox.Ack(t.Context(), "laptop", seq); err != nil {
		t.Fatal(err)
	}
	drain(t, m)
	if f.hourglass(ts) {
		t.Fatal("a message acked before its hourglass was recorded kept it")
	}
}

// A dropped message loses the hourglass.
func TestHourglassOffOnDrop(t *testing.T) {
	f := newFixture(t)
	f.w.Offline = time.Hour
	m := f.marks()
	f.w.Marks = m
	ts, _ := f.post(m, "laptop")
	drain(t, m)
	if !f.hourglass(ts) {
		t.Fatal("a message for an offline client has no hourglass")
	}
	f.w.Now = func() time.Time { return time.Now().Add(DefaultDrop + time.Second) }
	f.check()
	drain(t, m)
	if f.queued("laptop") != 0 || f.hourglass(ts) {
		t.Fatalf("after the drop: %d queued, hourglass %v; want it dropped without the hourglass", f.queued("laptop"), f.hourglass(ts))
	}
}

// A reaction Slack refuses changes nothing for the message: it stays
// queued, and the ack removes it as any other.
func TestHourglassFailureKeepsMessage(t *testing.T) {
	f := newFixture(t)
	m := f.marks()
	b, _ := json.Marshal(payload.Message{Type: payload.Inbound, Thread: "C1/9.9", TS: "9.9"})
	d, err := f.w.Store.Outbox.Enqueue(t.Context(), "laptop", b)
	if err != nil {
		t.Fatal(err)
	}
	// Slack has no message at 9.9: the reaction fails.
	m.Queued("laptop", d.MsgID, "C1", "9.9")
	drain(t, m)
	if n := f.queued("laptop"); n != 1 {
		t.Fatalf("%d messages queued after the reaction failed, want 1", n)
	}
	marks, err := f.w.Store.Outbox.Ack(t.Context(), "laptop", d.Seq)
	if err != nil || len(marks) != 0 || f.queued("laptop") != 0 {
		t.Fatalf("ack = %q, %v, %d left; want no marks and nothing left", marks, err, f.queued("laptop"))
	}
}
