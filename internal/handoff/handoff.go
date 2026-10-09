// Package handoff replaces a running hub or client with a new process of
// the same command line, without a gap: the new process is started by the
// old one, inherits its listening sockets, and once it reports ready the old
// one stops taking connections, finishes what it has in hand and exits. A
// new process that fails or takes too long is killed, and the old one keeps
// serving. The mechanism is github.com/cloudflare/tableflip; systemd learns
// of the new main process through sd_notify.
package handoff

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/cloudflare/tableflip"

	"github.com/Luolc/fednet/internal/local"
)

// DefaultTimeout is how long the new process gets to become ready.
const DefaultTimeout = time.Minute

// Process is what a hub or client does with its listening sockets and its
// successor. New returns the real one; None is for a process that never
// hands off, such as the hubs and clients a test runs in its own process.
type Process interface {
	// ListenTCP returns the TCP listener for addr: the one inherited from
	// the predecessor, or a new one.
	ListenTCP(addr string) (net.Listener, error)
	// ListenUnix returns the unix socket at path, inherited from the
	// predecessor together with its lock, or created with local.Listen.
	ListenUnix(path, group string) (net.Listener, error)
	// Ready reports that this process serves: to systemd, and to the
	// predecessor, which then exits. Listeners are to be taken before it;
	// inherited ones not taken are closed by it.
	Ready() error
	// WaitForParent returns once the predecessor has exited, at once when
	// there is none. What must run in one process at a time starts after
	// it.
	WaitForParent(ctx context.Context) error
	// Handoff starts the successor and returns once it is ready, or with
	// why it is not: then it has been killed and this process goes on. The
	// error carries what the successor reported with Report.
	Handoff(ctx context.Context) error
	// Report tells the predecessor why this process failed to start, for
	// its Handoff to return. After Ready, or without a predecessor, it does
	// nothing.
	Report(err error)
	// Exit is closed once a successor is ready: this process stops taking
	// connections and exits.
	Exit() <-chan struct{}
	// Stop ends a handoff in progress, killing the successor, and refuses
	// further ones. For a process that is shutting down.
	Stop()
}

// Live is the Process backed by tableflip.
type Live struct {
	upg *tableflip.Upgrader
	// reports is where a successor writes its Report; reportTo is the
	// predecessor's end of its pipe, nil once Ready or without one.
	reports  *os.File
	reportTo *os.File
}

// reportName is the name under which the report pipe is passed on.
const reportName = "report"

// maxReport bounds a report: it must fit the pipe, and an error is short.
const maxReport = 4 << 10

// New returns the Live process. Only one per OS process can exist; timeout
// is how long a successor gets to become ready, DefaultTimeout when zero.
func New(timeout time.Duration) (*Live, error) {
	upg, err := tableflip.New(tableflip.Options{UpgradeTimeout: timeout})
	if err != nil {
		return nil, err
	}
	l := &Live{upg: upg}
	if l.reportTo, err = upg.File(reportName); err != nil {
		return nil, err
	}
	// The pipe every successor of this process reports into. This process
	// keeps a write end of it too (in the upgrader, to be passed on), so a
	// read never sees EOF: Handoff reads what is there once the successor
	// is gone.
	r, w, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	if err := upg.AddFile(reportName, w); err != nil {
		r.Close()
		w.Close()
		return nil, err
	}
	w.Close()
	l.reports = r
	return l, nil
}

func (l *Live) ListenTCP(addr string) (net.Listener, error) {
	return l.upg.Listen("tcp", addr)
}

// lockName is the name under which a socket's lock file is passed on.
func lockName(path string) string { return "lock:" + path }

func (l *Live) ListenUnix(path, group string) (net.Listener, error) {
	ln, err := l.upg.Listener("unix", path)
	if err != nil {
		return nil, err
	}
	if ln != nil {
		lock, err := l.upg.File(lockName(path))
		if err != nil {
			ln.Close()
			return nil, err
		}
		if lock == nil {
			ln.Close()
			return nil, fmt.Errorf("handoff: inherited the socket %s without its lock", path)
		}
		return local.Inherit(ln, lock), nil
	}
	s, err := local.Listen(path, group)
	if err != nil {
		return nil, err
	}
	// The successor gets the lock file too: the lock belongs to the open
	// file, so it holds while either process has it.
	if err := errors.Join(l.upg.AddListener("unix", path, s), l.upg.AddFile(lockName(path), s.Lock())); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

// Ready tells systemd first, so the service's main pid has moved to this
// process before the predecessor exits.
func (l *Live) Ready() error {
	state := "READY=1"
	if l.upg.HasParent() {
		state = fmt.Sprintf("MAINPID=%d\nREADY=1", os.Getpid())
	}
	if err := notify(state); err != nil {
		return err
	}
	if l.reportTo != nil {
		l.reportTo.Close()
		l.reportTo = nil
	}
	return l.upg.Ready()
}

func (l *Live) WaitForParent(ctx context.Context) error { return l.upg.WaitForParent(ctx) }

func (l *Live) Handoff(context.Context) error {
	err := l.upg.Upgrade()
	if err == nil {
		return nil
	}
	// The successor is gone; whatever it reported is in the pipe.
	l.reports.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	buf := make([]byte, maxReport)
	if n, _ := l.reports.Read(buf); n > 0 {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(buf[:n])))
	}
	return err
}

func (l *Live) Report(err error) {
	if l.reportTo == nil {
		return
	}
	msg := err.Error()
	if len(msg) > maxReport {
		msg = msg[:maxReport]
	}
	l.reportTo.Write([]byte(msg))
	l.reportTo.Close()
	l.reportTo = nil
}

func (l *Live) Exit() <-chan struct{} { return l.upg.Exit() }

func (l *Live) Stop() { l.upg.Stop() }

// ErrNone is returned by None's Handoff.
var ErrNone = errors.New("handoff: this process cannot hand off")

// None is the Process that cannot hand off: it listens like any process
// and is never a successor.
type None struct{}

func (None) ListenTCP(addr string) (net.Listener, error) { return net.Listen("tcp", addr) }

func (None) ListenUnix(path, group string) (net.Listener, error) { return local.Listen(path, group) }

func (None) Ready() error { return nil }

func (None) WaitForParent(context.Context) error { return nil }

func (None) Handoff(context.Context) error { return ErrNone }

func (None) Report(error) {}

// Exit is never closed.
func (None) Exit() <-chan struct{} { return nil }

func (None) Stop() {}

// notify sends state to systemd's notification socket, when there is one
// ($NOTIFY_SOCKET); without one it does nothing. The service's unit must
// let every process of the service notify (NotifyAccess=all): a successor
// is not the main process until its MAINPID= is taken.
func notify(state string) error {
	addr := os.Getenv("NOTIFY_SOCKET")
	if addr == "" {
		return nil
	}
	conn, err := net.Dial("unixgram", addr)
	if err != nil {
		return fmt.Errorf("handoff: notify systemd: %w", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte(strings.TrimSuffix(state, "\n") + "\n")); err != nil {
		return fmt.Errorf("handoff: notify systemd: %w", err)
	}
	return nil
}
