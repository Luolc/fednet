package watch

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	slackgo "github.com/slack-go/slack"

	"github.com/Luolc/fednet/internal/slack"
)

// queueAt queues a message for client in a new thread of C1 and returns
// the thread's ts with the times just before and just after it was
// queued: the queue's own clock is between them.
func (f *fixture) queueAt(client string) (ts string, before, after time.Time) {
	f.t.Helper()
	before = time.Now()
	ts = f.queue(client)
	return ts, before, time.Now()
}

func (f *fixture) queued(client string) int {
	f.t.Helper()
	ds, err := f.w.Store.Outbox.After(f.t.Context(), client, 0)
	if err != nil {
		f.t.Fatal(err)
	}
	return len(ds)
}

// A message is kept until it has waited the limit and dropped once it
// has: its thread is told, naming the client and how long it waited, and
// it is alerted, each once.
func TestDropAfterLimit(t *testing.T) {
	f := newFixture(t)
	f.w.Offline = time.Hour
	ts, before, after := f.queueAt("laptop")
	for _, under := range []time.Duration{6*24*time.Hour + 23*time.Hour, DefaultDrop - time.Second} {
		f.w.Now = func() time.Time { return before.Add(under) }
		f.check()
		if n := f.queued("laptop"); n != 1 {
			t.Fatalf("after %v, %d messages queued, want the one kept", under, n)
		}
		if told, got := f.told(ts), f.hook.got(); len(told) != 0 || len(got) != 0 {
			t.Fatalf("after %v, thread got %q and alerts %q, want nothing", under, told, got)
		}
	}

	f.w.Now = func() time.Time { return after.Add(DefaultDrop + time.Second) }
	f.check()
	if n := f.queued("laptop"); n != 0 {
		t.Fatalf("after 7 days and a second, %d messages queued, want it dropped", n)
	}
	told := f.told(ts)
	if len(told) != 1 || told[0] != "给 laptop 的一条消息排队 7 天仍没送到，已丢弃。" {
		t.Fatalf("thread got %q, want one notice of the drop", told)
	}
	got := f.hook.got()
	if len(got) != 1 || !strings.Contains(got[0], "给 laptop 的一条消息排队 7 天仍没送到") || !strings.Contains(got[0], "已丢弃") {
		t.Fatalf("alerts = %q, want one about the drop", got)
	}
	f.check()
	if len(f.told(ts)) != 1 || len(f.hook.got()) != 1 {
		t.Fatalf("after another check: told %q, alerts %q; want each once", f.told(ts), f.hook.got())
	}
}

// Drop sets the limit.
func TestDropLimitSet(t *testing.T) {
	f := newFixture(t)
	f.w.Offline = time.Hour
	f.w.Drop = 2 * time.Hour
	ts, before, after := f.queueAt("laptop")
	f.w.Now = func() time.Time { return before.Add(2*time.Hour - time.Second) }
	f.check()
	if n := f.queued("laptop"); n != 1 {
		t.Fatalf("under the limit, %d messages queued, want 1", n)
	}
	f.w.Now = func() time.Time { return after.Add(2*time.Hour + time.Second) }
	f.check()
	if told := f.told(ts); f.queued("laptop") != 0 || len(told) != 1 || told[0] != "给 laptop 的一条消息排队 2 小时仍没送到，已丢弃。" {
		t.Fatalf("over the limit: %d queued, thread got %q; want it dropped and told", f.queued("laptop"), told)
	}
}

// failingReplies is a Slack whose replies fail while failing is set.
type failingReplies struct {
	*slack.Fake
	failing bool
}

func (s *failingReplies) PostReply(ctx context.Context, channel, ts, text string) (string, error) {
	if s.failing {
		return "", errors.New("slack is down")
	}
	return s.Fake.PostReply(ctx, channel, ts, text)
}

// A notice of a drop that fails is sent on a later check, by this watch
// or by a new one on the same database, as after a restart; one that went
// out is not sent again.
func TestDropNoticesRetried(t *testing.T) {
	f := newFixture(t)
	f.w.Offline = time.Hour
	sl := &failingReplies{Fake: f.fake, failing: true}
	f.w.Slack = sl
	ts, _, after := f.queueAt("laptop")
	f.w.Now = func() time.Time { return after.Add(DefaultDrop + time.Second) }
	if err := f.w.Check(t.Context()); err == nil {
		t.Fatal("Check with the thread failing returned no error")
	}
	if told, got := f.told(ts), f.hook.got(); len(told) != 0 || len(got) != 1 {
		t.Fatalf("thread got %q, alerts %q; want no notice, one alert", told, got)
	}

	sl.failing = false
	f.hook.failing = true
	restarted := *f.w
	f.w = &restarted
	f.check()
	if told, got := f.told(ts), f.hook.got(); len(told) != 1 || len(got) != 1 {
		t.Fatalf("after a restart: thread got %q, alerts %q; want one notice, still one alert", told, got)
	}
	f.check()
	if told := f.told(ts); len(told) != 1 {
		t.Fatalf("thread got %q, want the notice once", told)
	}
}

// A client online keeps its messages past the limit, as it may have them
// already with its ack on the way; once it is offline they are dropped.
func TestDropOnlyOffline(t *testing.T) {
	f := newFixture(t)
	f.w.Offline = time.Hour
	f.online["laptop"] = true
	ts, _, after := f.queueAt("laptop")
	f.w.Now = func() time.Time { return after.Add(DefaultDrop + time.Second) }
	f.check()
	if n := f.queued("laptop"); n != 1 || len(f.told(ts)) != 0 || len(f.hook.got()) != 0 {
		t.Fatalf("online past the limit: %d queued, told %q, alerts %q; want it kept, nothing said", n, f.told(ts), f.hook.got())
	}
	f.online["laptop"] = false
	f.check()
	if n := f.queued("laptop"); n != 0 || len(f.told(ts)) != 1 {
		t.Fatalf("offline past the limit: %d queued, told %q; want it dropped and told", n, f.told(ts))
	}
}

// slackError is a Slack whose replies fail with the error code.
type slackError struct {
	*slack.Fake
	code string
}

func (s *slackError) PostReply(context.Context, string, string, string) (string, error) {
	return "", slackgo.SlackErrorResponse{Err: s.code}
}

// A thread Slack will never take a post in again counts as told; one that
// failed for any other reason is tried again.
func TestDropNoticeInClosedThread(t *testing.T) {
	for _, c := range []struct {
		code  string
		retry bool
	}{{"is_archived", false}, {"not_in_channel", false}, {"ratelimited", true}} {
		f := newFixture(t)
		f.w.Offline = time.Hour
		f.w.Slack = &slackError{Fake: f.fake, code: c.code}
		_, _, after := f.queueAt("laptop")
		f.w.Now = func() time.Time { return after.Add(DefaultDrop + time.Second) }
		err := f.w.Check(t.Context())
		ds, derr := f.w.Store.Dropped(t.Context())
		if derr != nil {
			t.Fatal(derr)
		}
		if c.retry != (err != nil) || c.retry != (len(ds) == 1) {
			t.Fatalf("%s: Check = %v, %d dropped messages still waiting for notices; want retry %v", c.code, err, len(ds), c.retry)
		}
	}
}
