// Package hook hands the messages in the client's inbox to the agent on
// this machine: for each one it runs a configured command, without a shell,
// with the message written to an event file passed as the last argument.
// Exit 0 marks the message delivered; anything else, or a timeout, is
// retried with growing delays, and after the last allowed attempt the
// message is moved to the dead letters. Messages run one at a time, oldest
// first, so no message runs twice at once and the inbox stays in order.
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

// Event is what the event file holds: one inbox message, encoded as JSON.
// Payload is base64 in the file, as encoding/json encodes bytes.
type Event struct {
	MsgID   string `json:"msg_id"`
	Payload []byte `json:"payload"`
}

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

	// nudge has a buffer of one, so a Nudge is kept until Run looks.
	nudge     chan struct{}
	nudgeOnce sync.Once
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

// Run runs the hook for every undelivered message as it becomes due, until
// ctx is done. It must be called once. A run of the hook that ctx
// interrupts is not counted as an attempt; the message is tried again by
// the next Run.
func (r *Runner) Run(ctx context.Context) {
	for ctx.Err() == nil {
		wait, err := r.pass(ctx)
		if err != nil {
			slog.Warn("hook: inbox", "err", err)
			wait = r.retry().Min
		}
		if wait == 0 {
			continue
		}
		var due <-chan time.Time
		var timer *time.Timer
		if wait > 0 {
			timer = time.NewTimer(wait)
			due = timer.C
		}
		select {
		case <-r.nudgeCh():
		case <-due:
		case <-ctx.Done():
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

// pass runs the hook for the due messages. It returns 0 when it ran
// something (the caller looks again at once), how long until the next
// message is due, or -1 when nothing is queued.
func (r *Runner) pass(ctx context.Context) (time.Duration, error) {
	qs, err := r.Store.Inbox.Queued(ctx)
	if err != nil {
		return 0, err
	}
	now := time.Now()
	wait := time.Duration(-1)
	ran := false
	for _, q := range qs {
		if ctx.Err() != nil {
			return 0, nil
		}
		if d := q.NextAttempt.Sub(now); d > 0 {
			if wait < 0 || d < wait {
				wait = d
			}
			continue
		}
		r.attempt(ctx, q)
		ran = true
	}
	if ran {
		return 0, nil
	}
	return wait, nil
}

// attempt runs the hook once for q and records the outcome.
func (r *Runner) attempt(ctx context.Context, q store.Queued) {
	attempts := q.Attempts + 1
	err := r.exec(ctx, q.Message)
	if err == nil {
		// The hook has delivered the message; recording that must not be
		// lost to a shutdown, or the next Run would deliver it again.
		dctx := context.WithoutCancel(ctx)
		if err := r.Store.Inbox.MarkDelivered(dctx, q.MsgID); err != nil {
			slog.Error("hook: mark delivered", "msg_id", q.MsgID, "err", err)
		}
		if _, err := r.Store.Inbox.Prune(dctx, time.Now().Add(-r.retention())); err != nil {
			slog.Warn("hook: prune", "err", err)
		}
		return
	}
	if ctx.Err() != nil {
		return
	}
	retry := r.retry()
	if attempts >= retry.Attempts {
		slog.Error("hook: dead letter", "msg_id", q.MsgID, "attempts", attempts, "err", err)
		if err := r.Store.Inbox.Bury(ctx, q.MsgID, err.Error()); err != nil {
			slog.Error("hook: bury", "msg_id", q.MsgID, "err", err)
		}
		return
	}
	delay := retry.delay(attempts)
	slog.Warn("hook: failed", "msg_id", q.MsgID, "attempt", attempts, "retry_in", delay, "err", err)
	if err := r.Store.Inbox.Retry(ctx, q.MsgID, time.Now().Add(delay)); err != nil {
		slog.Error("hook: record attempt", "msg_id", q.MsgID, "err", err)
	}
}

// stderrTail is how much of the hook's stderr goes into the error.
const stderrTail = 4 << 10

// exec writes m to an event file, runs the hook on it and returns nil if
// the hook exited 0. The error says why it did not, with the end of the
// hook's stderr.
func (r *Runner) exec(ctx context.Context, m store.Message) error {
	f, err := os.CreateTemp(r.dir(), "fednet-event-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err := json.NewEncoder(f).Encode(Event{MsgID: m.MsgID, Payload: m.Payload}); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, r.timeout())
	defer cancel()
	args := append(append([]string{}, r.Command[1:]...), f.Name())
	cmd := exec.CommandContext(ctx, r.Command[0], args...)
	// An explicit, possibly empty, Env: nil would pass the client's own.
	cmd.Env = append([]string{}, r.Env...)
	cmd.Dir = r.dir()
	var stderr tail
	cmd.Stderr = &stderr
	// The hook gets its own process group, so that the timeout kills what
	// it started too, and the client's own signals do not reach it.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	// Wait blocks until stderr is closed, which a process the hook left
	// behind may hold; this bounds that wait.
	cmd.WaitDelay = time.Second

	err = cmd.Run()
	if cmd.ProcessState != nil && cmd.ProcessState.Success() {
		return nil
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		err = fmt.Errorf("timed out after %v", r.timeout())
	}
	if s := strings.TrimSpace(string(stderr.b)); s != "" {
		err = fmt.Errorf("%v; stderr: %s", err, s)
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
