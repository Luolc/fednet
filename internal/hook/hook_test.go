package hook

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Luolc/fednet/internal/store"
)

var testRetry = Retry{Min: time.Millisecond, Max: 4 * time.Millisecond, Attempts: 3}

// fixture is a Runner over a fresh store whose hook is a shell script, with
// a directory the script may write to.
type fixture struct {
	t    *testing.T
	st   *store.Client
	r    *Runner
	dir  string
	logs *syncBuffer
}

// newFixture writes body as the hook script. The script gets the event file
// as $1 and the fixture's directory as $DIR.
func newFixture(t *testing.T, body string) *fixture {
	t.Helper()
	dir := t.TempDir()
	st, err := store.OpenClient(t.Context(), filepath.Join(dir, "client.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	script := filepath.Join(dir, "hook.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\n"+body), 0o700); err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	events := filepath.Join(dir, "events")
	if err := os.Mkdir(events, 0o700); err != nil {
		t.Fatal(err)
	}
	r := &Runner{
		Store:   st,
		Command: []string{"/bin/sh", script},
		Env:     []string{"DIR=" + dir},
		Dir:     events,
		Timeout: 200 * time.Millisecond,
		Retry:   testRetry,
	}
	return &fixture{t: t, st: st, r: r, dir: dir, logs: logs}
}

// run starts the Runner; the returned func stops it and waits for it.
func (f *fixture) run() func() {
	ctx, cancel := context.WithCancel(f.t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.r.Run(ctx)
	}()
	stop := func() {
		cancel()
		<-done
	}
	f.t.Cleanup(stop)
	return stop
}

func (f *fixture) put(msgID, payload string) {
	f.t.Helper()
	if _, err := f.st.Inbox.Put(f.t.Context(), store.Message{MsgID: msgID, Payload: []byte(payload)}); err != nil {
		f.t.Fatal(err)
	}
	f.r.Nudge()
}

// lines returns the lines of a file in the fixture's directory, none if it
// does not exist yet.
func (f *fixture) lines(name string) []string {
	f.t.Helper()
	b, err := os.ReadFile(filepath.Join(f.dir, name))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		f.t.Fatal(err)
	}
	return strings.Split(strings.TrimSpace(string(b)), "\n")
}

func (f *fixture) queued() []store.Queued {
	f.t.Helper()
	qs, err := f.st.Inbox.Queued(f.t.Context())
	if err != nil {
		f.t.Fatal(err)
	}
	return qs
}

func (f *fixture) deadLetters() []store.DeadLetter {
	f.t.Helper()
	ds, err := f.st.Inbox.DeadLetters(f.t.Context())
	if err != nil {
		f.t.Fatal(err)
	}
	return ds
}

// delivered reports whether msgID is marked delivered: it is neither queued
// nor a dead letter, and still a duplicate.
func (f *fixture) delivered(msgID string) bool {
	f.t.Helper()
	for _, q := range f.queued() {
		if q.MsgID == msgID {
			return false
		}
	}
	for _, d := range f.deadLetters() {
		if d.MsgID == msgID {
			return false
		}
	}
	isNew, err := f.st.Inbox.Put(f.t.Context(), store.Message{MsgID: msgID, Payload: []byte("probe")})
	if err != nil {
		f.t.Fatal(err)
	}
	if isNew {
		f.t.Fatalf("%s is in neither the inbox nor the dead letters", msgID)
	}
	return true
}

// waitFor polls cond until it holds or five seconds pass.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// syncBuffer is a bytes.Buffer that several goroutines may write and read.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// The hook runs once per message, a redelivered message does not run it
// again, and it gets the message in the event file.
func TestRunsOncePerMessage(t *testing.T) {
	f := newFixture(t, `cat "$1" >> "$DIR/seen"`)
	f.run()
	f.put("m1", `{"t":"first"}`)
	f.put("m2", `{"t":"second"}`)
	f.put("m1", `{"t":"redelivered"}`)
	waitFor(t, "both messages delivered", func() bool { return f.delivered("m1") && f.delivered("m2") })

	var got []Event
	for _, line := range f.lines("seen") {
		var e Event
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("event %q: %v", line, err)
		}
		got = append(got, e)
	}
	if len(got) != 2 || got[0].MsgID != "m1" || string(got[0].Payload) != `{"t":"first"}` || got[1].MsgID != "m2" || string(got[1].Payload) != `{"t":"second"}` {
		t.Fatalf("hook saw %v, want m1 {\"t\":\"first\"} then m2 {\"t\":\"second\"}", got)
	}
	// The event files are gone once the hook has run.
	if left, _ := os.ReadDir(f.r.Dir); len(left) != 0 {
		t.Fatalf("%d event files left behind", len(left))
	}
}

// A failing hook is retried with growing delays until it succeeds, and not
// run for that message afterwards.
func TestRetriesUntilSuccess(t *testing.T) {
	f := newFixture(t, `echo "$1" >> "$DIR/runs"; [ -e "$DIR/fail" ] && { echo boom >&2; exit 3; }; exit 0`)
	if err := os.WriteFile(filepath.Join(f.dir, "fail"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	// Enough attempts that the fail marker is removed before they run out.
	f.r.Retry.Attempts = 1000
	f.run()
	before := time.Now()
	f.put("m1", `{"t":"x"}`)
	waitFor(t, "two failed attempts", func() bool {
		qs := f.queued()
		return len(qs) == 1 && qs[0].Attempts == 2
	})
	if qs := f.queued(); qs[0].NextAttempt.Before(before) {
		t.Fatalf("next attempt %v is before the message arrived at %v", qs[0].NextAttempt, before)
	}
	if err := os.Remove(filepath.Join(f.dir, "fail")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "m1 delivered", func() bool { return f.delivered("m1") })
	runs := len(f.lines("runs"))
	if runs < 3 {
		t.Fatalf("hook ran %d times, want at least 3", runs)
	}
	// A later message goes through the loop again; m1 is not run again.
	f.put("m2", `{"t":"y"}`)
	waitFor(t, "m2 delivered", func() bool { return f.delivered("m2") })
	if got := len(f.lines("runs")); got != runs+1 {
		t.Fatalf("hook ran %d times after m2, want %d", got, runs+1)
	}
	if !strings.Contains(f.logs.String(), "boom") || !strings.Contains(f.logs.String(), "msg_id=m1") {
		t.Fatalf("log does not report the failure with its stderr and msg_id:\n%s", f.logs.String())
	}
}

// After the last allowed attempt the message is a dead letter: not run
// again, not even when the hub delivers it again, and logged.
func TestDeadLetterAtLimit(t *testing.T) {
	f := newFixture(t, `echo "$1" >> "$DIR/runs"; echo "no agent here" >&2; exit 1`)
	f.run()
	f.put("m1", `{"t":"x"}`)
	waitFor(t, "m1 among the dead letters", func() bool { return len(f.deadLetters()) == 1 })
	d := f.deadLetters()[0]
	if d.MsgID != "m1" || string(d.Payload) != `{"t":"x"}` || d.Attempts != testRetry.Attempts {
		t.Fatalf("dead letter = %+v, want m1 (x) after %d attempts", d, testRetry.Attempts)
	}
	if !strings.Contains(d.Reason, "exit status 1") || !strings.Contains(d.Reason, "no agent here") {
		t.Fatalf("reason %q does not say the exit status and the stderr", d.Reason)
	}
	if qs := f.queued(); len(qs) != 0 {
		t.Fatalf("still queued: %v", qs)
	}
	if runs := len(f.lines("runs")); runs != testRetry.Attempts {
		t.Fatalf("hook ran %d times, want %d", runs, testRetry.Attempts)
	}
	if !strings.Contains(f.logs.String(), "dead letter") || !strings.Contains(f.logs.String(), "msg_id=m1") {
		t.Fatalf("log does not report the dead letter with its msg_id:\n%s", f.logs.String())
	}

	// Redelivered: dropped by dedup, so the hook does not run for it. m2
	// going through proves the loop looked again.
	f.put("m1", `{"t":"again"}`)
	f.put("m2", `{"t":"y"}`)
	waitFor(t, "m2 among the dead letters", func() bool { return len(f.deadLetters()) == 2 })
	if runs := len(f.lines("runs")); runs != 2*testRetry.Attempts {
		t.Fatalf("hook ran %d times, want %d", runs, 2*testRetry.Attempts)
	}
}

// Each dead letter is alerted once, with its msg_id and reason; an alert
// that fails is logged.
func TestDeadLetterAlertsOnce(t *testing.T) {
	var mu sync.Mutex
	var alerts []string
	var fail bool
	got := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(alerts)
	}

	f := newFixture(t, `echo "no agent here" >&2; exit 1`)
	f.r.Alert = func(_ context.Context, text string) error {
		mu.Lock()
		defer mu.Unlock()
		if fail {
			return errors.New("hub unreachable")
		}
		alerts = append(alerts, text)
		return nil
	}
	f.run()
	f.put("m1", `{"t":"x"}`)
	waitFor(t, "m1 among the dead letters", func() bool { return len(f.deadLetters()) == 1 })
	f.put("m1", `{"t":"again"}`)
	f.put("m2", `{"t":"y"}`)
	waitFor(t, "m2 among the dead letters", func() bool { return len(f.deadLetters()) == 2 })
	waitFor(t, "the alert for m2", func() bool { return len(got()) >= 2 })
	a := got()
	if len(a) != 2 || !strings.Contains(a[0], "m1") || !strings.Contains(a[0], "no agent here") || !strings.Contains(a[1], "m2") {
		t.Fatalf("alerts = %q, want one for m1 then one for m2, each with the reason", a)
	}

	mu.Lock()
	fail = true
	mu.Unlock()
	f.put("m3", `{"t":"z"}`)
	waitFor(t, "m3 among the dead letters", func() bool { return len(f.deadLetters()) == 3 })
	waitFor(t, "the failed alert for m3 in the log", func() bool {
		return strings.Contains(f.logs.String(), `msg="hook: alert" msg_id=m3`)
	})
}

// pid reads a pid the hook script wrote.
func (f *fixture) pid(name string) (int, bool) {
	f.t.Helper()
	lines := f.lines(name)
	if len(lines) != 1 || lines[0] == "" {
		return 0, false
	}
	pid, err := strconv.Atoi(lines[0])
	if err != nil {
		f.t.Fatal(err)
	}
	return pid, true
}

// At the timeout the hook and what it started are killed, and nothing is
// left behind.
func TestTimeoutKillsProcessGroup(t *testing.T) {
	f := newFixture(t, `sleep 60 & echo $! > "$DIR/child"; echo $$ > "$DIR/self"; wait`)
	f.r.Retry.Attempts = 2
	f.run()
	f.put("m1", `{"t":"x"}`)
	var self, child int
	waitFor(t, "the hook to start", func() bool {
		var ok1, ok2 bool
		self, ok1 = f.pid("self")
		child, ok2 = f.pid("child")
		return ok1 && ok2
	})
	// The control arm: both processes exist while the hook runs.
	if !left(self) || !left(child) {
		t.Fatalf("hook %d left %v, its child %d left %v; want both", self, left(self), child, left(child))
	}
	waitFor(t, "the attempt to be recorded", func() bool {
		qs := f.queued()
		return len(qs) == 1 && qs[0].Attempts == 1
	})
	// The kill is delivered asynchronously and the child is reaped by
	// init, so the count settles to 0 rather than being 0 at once; it is
	// read once more after that. A zombie counts as remaining.
	remaining := func() int {
		n := 0
		for _, pid := range []int{self, child} {
			if left(pid) {
				n++
			}
		}
		return n
	}
	waitFor(t, "the hook and its child to be reaped", func() bool { return remaining() == 0 })
	time.Sleep(10 * time.Millisecond)
	if n := remaining(); n != 0 {
		t.Fatalf("remaining = %d, want 0 (hook %d state %q, child %d state %q)", n, self, state(t, self), child, state(t, child))
	}
	waitFor(t, "the dead letter", func() bool { return len(f.deadLetters()) == 1 })
	if reason := f.deadLetters()[0].Reason; !strings.Contains(reason, "timed out") {
		t.Fatalf("reason %q does not say the hook timed out", reason)
	}
}

// The hook sees only the environment the Runner was given: the client's own
// variables do not leak, and nil means an empty environment.
func TestEnvIsOnlyWhatIsGiven(t *testing.T) {
	t.Setenv("FEDNET_TEST_LEAK", "secret")
	for _, tt := range []struct {
		name string
		env  []string
		want []string
	}{
		{"given", []string{"FEDNET_TEST_KEEP=yes"}, []string{"FEDNET_TEST_KEEP=yes"}},
		{"nil", nil, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, `/usr/bin/env > "$1.env"; cp "$1.env" "$(dirname "$1")/../env"`)
			f.r.Env = tt.env
			f.run()
			f.put("m1", `{"t":"x"}`)
			waitFor(t, "m1 delivered", func() bool { return f.delivered("m1") })
			got := f.lines("env")
			if len(got) == 1 && got[0] == "" {
				got = nil
			}
			// The shell may add its own variables; only the given ones
			// and the leak are checked.
			for _, v := range got {
				if strings.HasPrefix(v, "FEDNET_TEST_LEAK=") || strings.HasPrefix(v, "HOME=") || strings.HasPrefix(v, "PATH=") {
					t.Fatalf("hook environment has %q", v)
				}
			}
			for _, w := range tt.want {
				if !slices.Contains(got, w) {
					t.Fatalf("hook environment %v lacks %q", got, w)
				}
			}
		})
	}
}

// A message whose hook failed before a restart is retried by the next Run,
// with its attempts kept.
func TestResumesAfterRestart(t *testing.T) {
	f := newFixture(t, `echo "$1" >> "$DIR/runs"; [ -e "$DIR/fail" ] && exit 1; exit 0`)
	if err := os.WriteFile(filepath.Join(f.dir, "fail"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	f.r.Retry = Retry{Min: time.Hour, Max: time.Hour, Attempts: 3}
	stop := f.run()
	f.put("m1", `{"t":"x"}`)
	waitFor(t, "one failed attempt", func() bool {
		qs := f.queued()
		return len(qs) == 1 && qs[0].Attempts == 1
	})
	stop()
	if err := os.Remove(filepath.Join(f.dir, "fail")); err != nil {
		t.Fatal(err)
	}

	// The next Run honours the scheduled delay, then delivers.
	if err := f.st.Inbox.Retry(t.Context(), "m1", time.Now()); err != nil {
		t.Fatal(err)
	}
	f.r = &Runner{Store: f.st, Command: f.r.Command, Env: f.r.Env, Dir: f.r.Dir, Retry: testRetry}
	f.run()
	waitFor(t, "m1 delivered", func() bool { return f.delivered("m1") })
	if runs := len(f.lines("runs")); runs != 2 {
		t.Fatalf("hook ran %d times across the restart, want 2", runs)
	}
}

// A run that the shutdown interrupts is not counted as an attempt, and the
// hook is not left running.
func TestShutdownDoesNotCountAsAttempt(t *testing.T) {
	f := newFixture(t, `echo $$ > "$DIR/self"; sleep 60`)
	f.r.Timeout = time.Minute
	stop := f.run()
	f.put("m1", `{"t":"x"}`)
	var self int
	waitFor(t, "the hook to start", func() bool {
		var ok bool
		self, ok = f.pid("self")
		return ok
	})
	stop()
	if left(self) {
		t.Fatalf("hook %d is left (state %q) after Run returned", self, state(t, self))
	}
	qs := f.queued()
	if len(qs) != 1 || qs[0].Attempts != 0 || qs[0].NextAttempt.After(time.Now()) {
		t.Fatalf("queued after shutdown = %+v, want m1 with 0 attempts, due now", qs)
	}
}

func TestRetryDelay(t *testing.T) {
	r := Retry{Min: time.Second, Max: 10 * time.Second, Attempts: 5}
	for attempts, want := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 4: 8 * time.Second, 5: 10 * time.Second, 40: 10 * time.Second} {
		if got := r.delay(attempts); got != want {
			t.Errorf("delay(%d) = %v, want %v", attempts, got, want)
		}
	}
}

// left reports whether pid still has a process entry: running, or killed
// but not yet reaped (a zombie). Only a reaped process counts as gone.
func left(pid int) bool {
	_, err := os.Stat("/proc/" + strconv.Itoa(pid))
	return err == nil
}

// state returns the one-letter state of pid from /proc, or "" if it has no
// entry.
func state(t *testing.T, pid int) string {
	t.Helper()
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return ""
	}
	// The state follows the parenthesised command name.
	i := bytes.LastIndexByte(b, ')')
	if i < 0 || len(b) < i+3 {
		t.Fatalf("cannot read the state of %d from %q", pid, b)
	}
	return string(b[i+2])
}

// When the hook exits on its own, what it left running in its process
// group is killed too; a leftover that holds stderr makes the run fail.
func TestLeftoverProcessesAreKilled(t *testing.T) {
	for _, tt := range []struct {
		name      string
		body      string
		delivered bool
	}{
		{"detached", `sleep 60 >/dev/null 2>&1 &`, true},
		{"holding stderr", `sleep 60 &`, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// The hook starts its child, then waits for the test to let
			// it exit, so the child is observed running first.
			f := newFixture(t, tt.body+` echo $! > "$DIR/child"; while [ ! -e "$DIR/go" ]; do sleep 0.01; done; exit 0`)
			f.r.Retry = Retry{Min: time.Hour, Max: time.Hour, Attempts: 5}
			// Longer than the WaitDelay, so that a held stderr is what
			// ends the run, not the timeout.
			f.r.Timeout = 5 * time.Second
			f.run()
			f.put("m1", `{"t":"x"}`)
			var child int
			waitFor(t, "the hook to start its child", func() bool {
				var ok bool
				child, ok = f.pid("child")
				return ok
			})
			// The control arm: the child is running while the hook is.
			if st := state(t, child); st == "" || st == "Z" {
				t.Fatalf("child %d is in state %q, want a live one", child, st)
			}
			if err := os.WriteFile(filepath.Join(f.dir, "go"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			waitFor(t, "the outcome to be recorded", func() bool {
				if tt.delivered {
					return f.delivered("m1")
				}
				qs := f.queued()
				return len(qs) == 1 && qs[0].Attempts == 1
			})
			// Gone means reaped by init, not just killed: a zombie still
			// has its entry. The count settles to 0 and is read once more.
			waitFor(t, "the child to be reaped", func() bool { return !left(child) })
			time.Sleep(10 * time.Millisecond)
			if left(child) {
				t.Fatalf("child %d is left (state %q) after the hook exited", child, state(t, child))
			}
			if !tt.delivered && !strings.Contains(f.logs.String(), "holding its stderr") {
				t.Fatalf("log does not say the hook left a process holding stderr:\n%s", f.logs.String())
			}
		})
	}
}

// An outcome the store refuses is kept and recorded later; the hook does
// not run again for the message meanwhile, whichever write failed.
func TestOutcomeKeptWhenStoreFails(t *testing.T) {
	for _, tt := range []struct {
		name, script, trigger string
		limit                 int
		recorded              func(f *fixture) bool
	}{
		{"delivered", "exit 0",
			"CREATE TRIGGER fail BEFORE UPDATE OF delivered ON inbox BEGIN SELECT RAISE(ABORT, 'disk full'); END", 5,
			func(f *fixture) bool { return f.delivered("m1") }},
		{"retry", "exit 1",
			"CREATE TRIGGER fail BEFORE UPDATE OF attempts ON inbox BEGIN SELECT RAISE(ABORT, 'disk full'); END", 5,
			func(f *fixture) bool { qs := f.queued(); return len(qs) == 1 && qs[0].Attempts == 1 }},
		{"dead letter", "exit 1",
			"CREATE TRIGGER fail BEFORE INSERT ON dead_letter BEGIN SELECT RAISE(ABORT, 'disk full'); END", 1,
			func(f *fixture) bool { return len(f.deadLetters()) == 1 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, `echo run >> "$DIR/runs"; `+tt.script)
			f.r.Retry = Retry{Min: time.Hour, Max: time.Hour, Attempts: tt.limit}
			db, err := sql.Open("sqlite", filepath.Join(f.dir, "client.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { db.Close() })
			if _, err := db.ExecContext(t.Context(), tt.trigger); err != nil {
				t.Fatal(err)
			}
			f.put("m1", `{"t":"x"}`)
			for i := range 3 {
				wait, err := f.r.pass(t.Context())
				if err == nil || !strings.Contains(err.Error(), "disk full") || !strings.Contains(err.Error(), "m1") {
					t.Fatalf("pass %d with the store failing: err = %v, want the store's error naming m1", i, err)
				}
				if wait != 0 || len(f.lines("runs")) != 1 {
					t.Fatalf("pass %d: wait %v, hook ran %d times; want 0, 1", i, wait, len(f.lines("runs")))
				}
			}
			// The store recovers: the kept outcome is recorded, the hook
			// is not run again for it.
			if _, err := db.ExecContext(t.Context(), "DROP TRIGGER fail"); err != nil {
				t.Fatal(err)
			}
			if _, err := f.r.pass(t.Context()); err != nil {
				t.Fatal(err)
			}
			if !tt.recorded(f) {
				t.Fatal("the outcome was not recorded once the store recovered")
			}
			if n := len(f.lines("runs")); n != 1 {
				t.Fatalf("hook ran %d times, want 1", n)
			}
		})
	}
}

// A payload that is not JSON cannot be handed to the hook; that counts as
// a failed attempt, with the reason, rather than being retried forever.
func TestBadPayloadIsAFailure(t *testing.T) {
	f := newFixture(t, `echo run >> "$DIR/runs"; exit 0`)
	f.r.Retry = Retry{Min: time.Millisecond, Max: time.Millisecond, Attempts: 2}
	f.run()
	f.put("m1", "not json")
	waitFor(t, "the dead letter", func() bool { return len(f.deadLetters()) == 1 })
	if reason := f.deadLetters()[0].Reason; !strings.Contains(reason, "json") {
		t.Fatalf("reason %q does not say the payload is not JSON", reason)
	}
	if f.lines("runs") != nil {
		t.Fatal("the hook ran for a payload that could not be written")
	}
}

// prSetChildSubreaper is the Linux prctl option that makes this process
// the parent of its orphaned descendants.
const prSetChildSubreaper = 36

// The residue judge used above tells a reaped process from a killed one
// nobody has waited for: with the test process as subreaper and not
// waiting, the hook's child stays a zombie and still counts as left. The
// earlier judge, which took a zombie for gone, reads 0 here.
func TestResidueCountsZombies(t *testing.T) {
	if _, _, errno := syscall.Syscall6(syscall.SYS_PRCTL, prSetChildSubreaper, 1, 0, 0, 0, 0); errno != 0 {
		t.Fatal(errno)
	}
	t.Cleanup(func() { syscall.Syscall6(syscall.SYS_PRCTL, prSetChildSubreaper, 0, 0, 0, 0, 0) })
	f := newFixture(t, `sleep 60 >/dev/null 2>&1 & echo $! > "$DIR/child"; exit 0`)
	if err := f.r.exec(t.Context(), store.Message{MsgID: "m1", Payload: []byte(`{"t":"x"}`)}); err != nil {
		t.Fatal(err)
	}
	child, ok := f.pid("child")
	if !ok {
		t.Fatal("the hook did not write its child's pid")
	}
	// Killed, reparented to this process, and nobody has waited: a zombie.
	waitFor(t, "the child to be killed", func() bool { return state(t, child) == "Z" })
	time.Sleep(10 * time.Millisecond)
	if !left(child) {
		t.Fatalf("child %d is a zombie but counts as gone", child)
	}
	// Reaped by its parent: now it is gone.
	var ws syscall.WaitStatus
	if _, err := syscall.Wait4(child, &ws, 0, nil); err != nil {
		t.Fatal(err)
	}
	if left(child) {
		t.Fatalf("child %d is left after being reaped", child)
	}
}

// Failures before the hook starts are the client's, not the hook's: they
// are not attempts, the message waits, and once the cause is gone the hook
// runs. The attempt limit is 1, so a miscounted failure would show as a
// dead letter.
func TestNotRunIsNotAnAttempt(t *testing.T) {
	for _, tt := range []struct {
		name  string
		fault func(t *testing.T, f *fixture) (undo func())
	}{
		{"command not found", func(t *testing.T, f *fixture) func() {
			command := f.r.Command
			f.r.Command = []string{filepath.Join(f.dir, "missing-hook")}
			return func() { f.r.Command = command }
		}},
		{"event directory missing", func(t *testing.T, f *fixture) func() {
			dir := f.r.Dir
			f.r.Dir = filepath.Join(f.dir, "missing")
			return func() { f.r.Dir = dir }
		}},
		{"event file write fails", func(t *testing.T, f *fixture) func() {
			// The event file is created, then its write fails: this
			// process may not write files longer than a few bytes.
			var before syscall.Rlimit
			if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &before); err != nil {
				t.Fatal(err)
			}
			signal.Ignore(syscall.SIGXFSZ)
			limited := before
			limited.Cur = 8
			if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limited); err != nil {
				t.Fatal(err)
			}
			return func() {
				if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &before); err != nil {
					t.Fatal(err)
				}
				signal.Reset(syscall.SIGXFSZ)
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, `echo run >> "$DIR/runs"; exit 0`)
			f.r.Retry = Retry{Min: time.Hour, Max: time.Hour, Attempts: 1}
			f.put("m1", `{"t":"x"}`)
			undo := tt.fault(t, f)
			t.Cleanup(undo)
			for i := range 2 {
				wait, err := f.r.pass(t.Context())
				if !errors.Is(err, errNotRun) {
					t.Fatalf("pass %d: err = %v, want errNotRun", i, err)
				}
				if wait != 0 {
					t.Fatalf("pass %d: wait = %v, want 0", i, wait)
				}
			}
			if qs := f.queued(); len(qs) != 1 || qs[0].Attempts != 0 {
				t.Fatalf("queued = %+v, want m1 with 0 attempts", qs)
			}
			if ds := f.deadLetters(); len(ds) != 0 {
				t.Fatalf("dead letters = %+v, want none", ds)
			}
			if f.lines("runs") != nil {
				t.Fatal("the hook ran")
			}
			undo()
			if _, err := f.r.pass(t.Context()); err != nil {
				t.Fatal(err)
			}
			if !f.delivered("m1") || len(f.lines("runs")) != 1 {
				t.Fatalf("after the cause is gone: delivered %v, runs %d; want true, 1", f.delivered("m1"), len(f.lines("runs")))
			}
		})
	}
}
