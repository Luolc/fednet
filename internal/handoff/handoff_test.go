package handoff

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// asSuccessor is the environment variable that makes this test binary run
// as the successor a handoff starts: it reports ready and stays until its
// predecessor, the test, has exited.
const asSuccessor = "FEDNET_TEST_AS_SUCCESSOR"

func TestMain(m *testing.M) {
	if os.Getenv(asSuccessor) == "1" {
		l, err := New(0)
		if err == nil {
			err = l.Ready()
		}
		if err == nil {
			err = l.WaitForParent(context.Background())
		}
		if err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// notify writes the state to the socket systemd names; without one it does
// nothing and does not fail.
func TestNotify(t *testing.T) {
	path := filepath.Join(t.TempDir(), "notify")
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	t.Setenv("NOTIFY_SOCKET", path)
	if err := notify("MAINPID=42\nREADY=1"); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	if err != nil || string(buf[:n]) != "MAINPID=42\nREADY=1\n" {
		t.Fatalf("systemd got %q, %v; want MAINPID and READY", buf[:n], err)
	}

	t.Setenv("NOTIFY_SOCKET", "")
	if err := notify("READY=1"); err != nil {
		t.Fatalf("notify without a socket: %v", err)
	}
}

// handedOff is set once a test has made this process's Live.
var handedOff bool

// A handoff asked for once systemd has READY=1 but before tableflip has been
// told waits for Ready and then hands off, instead of being refused.
func TestHandoffWaitsForReady(t *testing.T) {
	if handedOff {
		t.Skip("tableflip allows one Upgrader per process: run with -count=1")
	}
	handedOff = true
	path := filepath.Join(t.TempDir(), "notify")
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	t.Setenv("NOTIFY_SOCKET", path)
	t.Setenv(asSuccessor, "1")
	l, err := New(10 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	handed := make(chan error, 1)
	waiting := make(chan struct{})
	awaitingReady = func() { close(waiting) }
	// Ready stops here until the handoff waits for it, or has returned.
	readying = func() {
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		buf := make([]byte, 64)
		if n, err := conn.Read(buf); err != nil || string(buf[:n]) != "READY=1\n" {
			t.Errorf("systemd got %q, %v; want READY=1 before the handoff", buf[:n], err)
		}
		// Handoff retries on this text from tableflip.
		if err := l.upg.Upgrade(); err == nil || err.Error() != errNotReady {
			t.Errorf("tableflip before Ready: %v, want %q", err, errNotReady)
		}
		go func() { handed <- l.Handoff(t.Context()) }()
		select {
		case <-waiting:
		case err := <-handed:
			handed <- err
		}
	}
	t.Cleanup(func() { readying, awaitingReady = func() {}, func() {} })
	if err := l.Ready(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-handed:
		if err != nil {
			t.Fatalf("handoff: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("handoff did not return")
	}
}
