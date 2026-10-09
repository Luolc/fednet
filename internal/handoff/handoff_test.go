package handoff

import (
	"net"
	"path/filepath"
	"testing"
	"time"
)

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
