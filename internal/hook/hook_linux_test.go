package hook

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Luolc/fednet/internal/store"
)

// The tests here read /proc or call prctl, which only linux has.

// prSetChildSubreaper is the Linux prctl option that makes this process
// the parent of its orphaned descendants.
const prSetChildSubreaper = 36

// The residue judge, left, tells a reaped process from a killed one
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

// A dead letter notice stops with the shutdown, as any run of the hook
// does: Run does not wait for it, and the dead letter stays recorded.
func TestShutdownStopsTheNotice(t *testing.T) {
	f := newFixture(t, notice+` && { echo $$ > "$DIR/self"; sleep 60; }; exit 1`)
	f.r.Timeout = time.Minute
	f.r.Retry.Attempts = 1
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.r.Run(ctx)
	}()
	f.put("m1", `{"t":"x"}`)
	var self int
	waitFor(t, "the notice to start", func() bool {
		var ok bool
		self, ok = f.pid("self")
		return ok
	})
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return while the notice ran")
	}
	if left(self) {
		t.Fatalf("notice %d is left (state %q) after Run returned", self, state(t, self))
	}
	if d := f.deadLetters(); len(d) != 1 || d[0].MsgID != "m1" {
		t.Fatalf("dead letters = %+v, want m1", d)
	}
}

// Stop lets the run of the hook in hand end and records it, and takes no
// further message: the next Run, in another process, runs only that one.
func TestStopWaitsForTheHook(t *testing.T) {
	f := newFixture(t, `echo $$ > "$DIR/self"; sleep 0.3`)
	f.r.Timeout = time.Minute
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.r.Run(ctx)
	}()
	f.put("m1", `{"t":"x"}`)
	f.put("m2", `{"t":"y"}`)
	var self int
	waitFor(t, "the hook to start", func() bool {
		var ok bool
		self, ok = f.pid("self")
		return ok
	})
	f.r.Stop()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Stop")
	}
	if left(self) {
		t.Fatalf("hook %d is left (state %q) after Run returned", self, state(t, self))
	}
	if !f.delivered("m1") {
		t.Fatal("m1 is not marked delivered: the run in hand was not recorded")
	}
	qs := f.queued()
	if len(qs) != 1 || qs[0].MsgID != "m2" || qs[0].Attempts != 0 {
		t.Fatalf("queued after Stop = %+v, want m2 untouched", qs)
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
