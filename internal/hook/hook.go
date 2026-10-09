// Package hook hands the messages in the client's inbox to the agent on
// this machine: for each one it runs a configured command, without a shell,
// with the message written to an event file passed as the last argument.
// Exit 0 marks the message delivered; anything else, or a timeout, is
// retried with growing delays, and after the last allowed attempt the
// message is moved to the dead letters. Messages run one at a time, oldest
// first, so no message runs twice at once and the inbox stays in order.
//
// The contract with the hook: it runs in its own process group, which is
// killed when it exits, so it must not leave processes behind; anything
// long-lived goes to a daemon. Its outcome is recorded before the next
// message runs; while the client lives, no message runs again once it has
// exited 0. If the client dies between the hook's exit and the record, the
// next start runs the hook again for that message, so the hook must be
// idempotent by msg_id.
package hook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Luolc/fednet/internal/store"
)

// Event is what the event file holds: one inbox message, as JSON. Payload
// is the message's payload as it came from the hub, a JSON object (see
// package payload), embedded as is.
type Event struct {
	MsgID   string          `json:"msg_id"`
	Payload json.RawMessage `json:"payload"`
}

// errNotRun means the hook was not started: the event file could not be
// written, or the process could not be started. That is the client's
// trouble, not the hook's, so it is not an attempt; the message waits for
// the next pass.
var errNotRun = errors.New("hook not run")

// Retry paces the attempts at one message: after attempt n fails (n from
// 1), the next runs Min doubled n-1 times later, capped at Max; when
// attempt Attempts fails, the message goes to the dead letters.
type Retry struct {
	Min, Max time.Duration
	Attempts int
}

// DefaultRetry is the Retry used when Runner.Retry is zero: about forty
// minutes of attempts.
var DefaultRetry = Retry{Min: 10 * time.Second, Max: 10 * time.Minute, Attempts: 10}

// delay returns how long to wait after the attempts-th failure.
func (r Retry) delay(attempts int) time.Duration {
	d := r.Min << min(attempts-1, 30)
	if d > r.Max || d <= 0 {
		d = r.Max
	}
	return d
}

// Defaults for the zero fields of Runner.
const (
	DefaultTimeout   = time.Minute
	DefaultRetention = 7 * 24 * time.Hour
)

// Runner runs the hook for the inbox. Run serves until its context is done;
// Nudge tells it a message has arrived.
type Runner struct {
	Store *store.Client
	// Command is the hook: the program and its fixed arguments. The event
	// file's path is appended. The program is run directly, not by a shell.
	Command []string
	// Env is the hook's whole environment, as KEY=VALUE. Nothing of the
	// client's own environment is passed unless it is listed here; nil
	// means an empty environment.
	Env []string
	// Dir is where event files are written; the hook runs in it. Empty
	// means the system temporary directory.
	Dir string
	// Timeout bounds one run of the hook; at the timeout the hook's whole
	// process group is killed. Zero means DefaultTimeout.
	Timeout time.Duration
	// Retry paces failed messages. Zero means DefaultRetry.
	Retry Retry
	// Retention is how long delivered messages are kept in the inbox for
	// dedup before they are pruned. Zero means DefaultRetention.
	Retention time.Duration
	// Alert, if not nil, is called once for each message moved to the
	// dead letters, with the alert's text.
	Alert func(ctx context.Context, text string) error

	// nudge has a buffer of one, so a Nudge is kept until Run looks.
	nudge     chan struct{}
	nudgeOnce sync.Once
	// unsaved is an outcome the store refused; Run keeps trying to record
	// it, and runs nothing else until it has, so the hook never runs again
	// for a message whose outcome is known.
	unsaved *outcome
	// stop is closed by Stop.
	stop     chan struct{}
	stopOnce sync.Once
	// beforeWait, if set, runs after Run has decided to wait for
	// something to do and before it picks what to wait for; a test puts
	// a Stop there.
	beforeWait func()
}

// outcome is what one run of the hook ended in: err nil means it exited 0.
type outcome struct {
	q   store.Queued
	err error
}

func (r *Runner) timeout() time.Duration {
	if r.Timeout == 0 {
		return DefaultTimeout
	}
	return r.Timeout
}

func (r *Runner) retry() Retry {
	if r.Retry == (Retry{}) {
		return DefaultRetry
	}
	return r.Retry
}

func (r *Runner) retention() time.Duration {
	if r.Retention == 0 {
		return DefaultRetention
	}
	return r.Retention
}

func (r *Runner) dir() string {
	if r.Dir == "" {
		return os.TempDir()
	}
	return r.Dir
}

func (r *Runner) nudgeCh() chan struct{} {
	r.nudgeOnce.Do(func() { r.nudge = make(chan struct{}, 1) })
	return r.nudge
}

// Nudge tells Run that the inbox has a new message. It never blocks.
func (r *Runner) Nudge() {
	select {
	case r.nudgeCh() <- struct{}{}:
	default:
	}
}

func (r *Runner) stopCh() chan struct{} {
	r.stopOnce.Do(func() { r.stop = make(chan struct{}) })
	return r.stop
}

// Stop makes Run return once the run of the hook in hand, if any, has
// ended and its outcome is recorded, taking no further message; unlike
// ctx it interrupts nothing, and an outcome the store refuses is still
// retried until recorded. It is for handing the inbox to another process.
// It may be called more than once.
func (r *Runner) Stop() {
	r.stopOnce.Do(func() { r.stop = make(chan struct{}) })
	select {
	case <-r.stop:
	default:
		close(r.stop)
	}
}

func (r *Runner) stopping() bool {
	select {
	case <-r.stopCh():
		return true
	default:
		return false
	}
}

// Run runs the hook for every undelivered message as it becomes due, until
// ctx is done or Stop is called. It must be called once. A run of the hook that ctx
// interrupts is not counted as an attempt; the message is tried again by
// the next Run. An outcome the store refuses to record is kept and retried
// before anything else runs; one that is still unrecorded when ctx is done
// is lost, and the next Run runs the hook again for that message.
func (r *Runner) Run(ctx context.Context) {
	for ctx.Err() == nil && (!r.stopping() || r.unsaved != nil) {
		wait, err := r.pass(ctx)
		if err != nil {
			slog.Warn("hook: store", "err", err)
			wait = r.retry().Min
		}
		if wait == 0 || (r.stopping() && r.unsaved == nil) {
			continue
		}
		var due <-chan time.Time
		var timer *time.Timer
		if wait > 0 {
			timer = time.NewTimer(wait)
			due = timer.C
		}
		// With an outcome to record the loop waits only for the retry;
		// Stop, already called, must not wake it. Otherwise it listens for
		// Stop: one that came since pass looked would be missed.
		if r.beforeWait != nil {
			r.beforeWait()
		}
		var stop <-chan struct{}
		if r.unsaved == nil {
			stop = r.stopCh()
		}
		select {
		case <-r.nudgeCh():
		case <-due:
		case <-ctx.Done():
		case <-stop:
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

// pass records an unsaved outcome if there is one, then runs the hook for
// the due messages. It returns 0 when it ran something (the caller looks
// again at once), how long until the next message is due, or -1 when
// nothing is queued. A store error stops the pass; the outcome in hand, if
// any, is kept for the next one.
func (r *Runner) pass(ctx context.Context) (time.Duration, error) {
	if r.unsaved != nil {
		if err := r.record(ctx, *r.unsaved); err != nil {
			return 0, err
		}
		r.unsaved = nil
	}
	qs, err := r.Store.Inbox.Queued(ctx)
	if err != nil {
		return 0, err
	}
	now := time.Now()
	wait := time.Duration(-1)
	ran := false
	for _, q := range qs {
		if ctx.Err() != nil || r.stopping() {
			return 0, nil
		}
		if d := q.NextAttempt.Sub(now); d > 0 {
			if wait < 0 || d < wait {
				wait = d
			}
			continue
		}
		o := outcome{q: q, err: r.exec(ctx, q.Message)}
		if o.err != nil && ctx.Err() != nil {
			// Interrupted by the shutdown, not failed: not an attempt.
			return 0, nil
		}
		if errors.Is(o.err, errNotRun) {
			return 0, o.err
		}
		ran = true
		if err := r.record(ctx, o); err != nil {
			r.unsaved = &o
			return 0, err
		}
	}
	if ran {
		return 0, nil
	}
	return wait, nil
}

// record writes o to the store: delivered, or a retry, or a dead letter.
// It writes even once ctx is done: an outcome lost to the shutdown would
// make the next Run run the hook again.
func (r *Runner) record(ctx context.Context, o outcome) error {
	ctx = context.WithoutCancel(ctx)
	q := o.q
	if o.err == nil {
		if err := r.Store.Inbox.MarkDelivered(ctx, q.MsgID); err != nil {
			return fmt.Errorf("mark %s delivered: %w", q.MsgID, err)
		}
		if _, err := r.Store.Inbox.Prune(ctx, time.Now().Add(-r.retention())); err != nil {
			slog.Warn("hook: prune", "err", err)
		}
		return nil
	}
	attempts := q.Attempts + 1
	retry := r.retry()
	if attempts >= retry.Attempts {
		if err := r.Store.Inbox.Bury(ctx, q.MsgID, o.err.Error()); err != nil {
			return fmt.Errorf("bury %s: %w", q.MsgID, err)
		}
		slog.Error("hook: dead letter", "msg_id", q.MsgID, "attempts", attempts, "err", o.err)
		if r.Alert != nil {
			// The message is buried once, so it is alerted once; an alert
			// that fails is logged, not retried.
			text := fmt.Sprintf("dead letter: msg_id %s after %d attempts: %v", q.MsgID, attempts, o.err)
			if err := r.Alert(ctx, text); err != nil {
				slog.Warn("hook: alert", "msg_id", q.MsgID, "err", err)
			}
		}
		return nil
	}
	delay := retry.delay(attempts)
	if err := r.Store.Inbox.Retry(ctx, q.MsgID, time.Now().Add(delay)); err != nil {
		return fmt.Errorf("record attempt at %s: %w", q.MsgID, err)
	}
	slog.Warn("hook: failed", "msg_id", q.MsgID, "attempt", attempts, "retry_in", delay, "err", o.err)
	return nil
}

// stderrTail is how much of the hook's stderr goes into the error.
const stderrTail = 4 << 10

// exec writes m to an event file, runs the hook on it and returns nil if
// the hook exited 0 and left nothing behind. The error says why it did
// not, with the end of the hook's stderr; it wraps errNotRun when the hook
// never started. Whatever the outcome, the hook's process group is killed
// once the hook itself has exited.
func (r *Runner) exec(ctx context.Context, m store.Message) error {
	// A payload that is not JSON cannot be put in the event; that is the
	// hub's fault, and the error, counted as an attempt, says so.
	event, err := json.Marshal(Event{MsgID: m.MsgID, Payload: m.Payload})
	if err != nil {
		return fmt.Errorf("payload is not JSON: %v", err)
	}
	name, err := r.writeEvent(append(event, '\n'))
	if err != nil {
		return err
	}
	defer func() {
		if err := os.Remove(name); err != nil {
			slog.Warn("hook: remove event file", "err", err)
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, r.timeout())
	defer cancel()
	args := append(append([]string{}, r.Command[1:]...), name)
	cmd := exec.CommandContext(ctx, r.Command[0], args...)
	// An explicit, possibly empty, Env: nil would pass the client's own.
	cmd.Env = append([]string{}, r.Env...)
	cmd.Dir = r.dir()
	var stderr tail
	cmd.Stderr = &stderr
	// The hook gets its own process group, so that killing it takes what
	// it started too, and the client's own signals do not reach it.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return killGroup(cmd.Process.Pid) }
	// Wait blocks until stderr is closed, which a process the hook left
	// behind may hold; this bounds that wait.
	cmd.WaitDelay = time.Second

	// Start fails on the client's side of the fence: the program is not
	// found or not executable, the working directory is gone, the fork,
	// the process group or the stderr pipe cannot be set up, or ctx is
	// already done. None of that is the hook's doing.
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("%w: start %s: %v", errNotRun, r.Command[0], err)
	}
	err = cmd.Wait()
	// Whatever the hook left running in its group dies with it. The group
	// id is the pid of the process just waited for; the kernel does not
	// reuse a pid while it is still a group's id, so this cannot hit a
	// stranger. What is killed is reaped by init: the client is not a
	// subreaper.
	if kerr := killGroup(cmd.Process.Pid); kerr != nil && !errors.Is(kerr, os.ErrProcessDone) {
		slog.Warn("hook: kill process group", "pid", cmd.Process.Pid, "err", kerr)
	}
	switch {
	case err == nil:
		return nil
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		err = fmt.Errorf("timed out after %v", r.timeout())
	case errors.Is(err, exec.ErrWaitDelay):
		err = errors.New("exited 0 but left a process holding its stderr")
	}
	if s := strings.TrimSpace(string(stderr.b)); s != "" {
		err = fmt.Errorf("%v; stderr: %s", err, s)
	}
	return err
}

// writeEvent writes event to a new file in the event directory and returns
// its path. Every error wraps errNotRun: the file is the client's to write.
func (r *Runner) writeEvent(event []byte) (string, error) {
	f, err := os.CreateTemp(r.dir(), "fednet-event-*.json")
	if err != nil {
		return "", fmt.Errorf("%w: %v", errNotRun, err)
	}
	_, err = f.Write(event)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(f.Name())
		return "", fmt.Errorf("%w: write %s: %v", errNotRun, f.Name(), err)
	}
	return f.Name(), nil
}

// killGroup sends SIGKILL to the process group whose id is pid. It returns
// os.ErrProcessDone when there is no such group.
func killGroup(pid int) error {
	err := syscall.Kill(-pid, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return os.ErrProcessDone
	}
	return err
}

// tail keeps the last stderrTail bytes written to it.
type tail struct{ b []byte }

func (t *tail) Write(p []byte) (int, error) {
	t.b = append(t.b, p...)
	if len(t.b) > stderrTail {
		t.b = t.b[len(t.b)-stderrTail:]
	}
	return len(p), nil
}
