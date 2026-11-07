package main

import (
	"strings"
	"testing"
)

func TestSocketFor(t *testing.T) {
	// The socket is bound at <dir>/.fednet-socket-<10 digits>/socket, 104
	// bytes with the NUL at most: a home directory of 43 bytes is the
	// longest that fits.
	fits := "/Users/" + strings.Repeat("x", 36)
	tooLong := fits + "x"
	for _, c := range []struct {
		name, goos, env, home, want string
	}{
		{"linux", "linux", "", "/home/u", "/run/fednet-client/fednet.sock"},
		{"darwin", "darwin", "", "/Users/u", "/Users/u/.local/state/fednet-client/fednet.sock"},
		{"the variable wins on linux", "linux", "/x/a.sock", "/home/u", "/x/a.sock"},
		{"the variable wins on darwin", "darwin", "/x/a.sock", "/Users/u", "/x/a.sock"},
		{"darwin without a home", "darwin", "", "", ""},
		{"darwin with the longest home that fits", "darwin", "", fits, fits + "/.local/state/fednet-client/fednet.sock"},
		{"darwin with a home one byte too long", "darwin", "", tooLong, ""},
		{"another system", "plan9", "", "/home/u", ""},
	} {
		if got := socketFor(c.goos, c.env, c.home); got != c.want {
			t.Errorf("%s: socketFor(%q, %q, %q) = %q, want %q", c.name, c.goos, c.env, c.home, got, c.want)
		}
	}
}

// -socket wins over the variable, which wins over the default; without
// the flag the commands use the variable.
func TestSocketFlagDefault(t *testing.T) {
	t.Setenv(socketEnv, "/nonexistent/env.sock")
	var stdout, stderr strings.Builder
	if code := run(t.Context(), []string{"client", "whoami"}, &stdout, &stderr); code != 4 || !strings.Contains(stderr.String(), "/nonexistent/env.sock") {
		t.Errorf("without -socket: code %d, stderr %q; want 4 naming the variable's socket", code, stderr.String())
	}
	stderr.Reset()
	if code := run(t.Context(), []string{"client", "whoami", "-socket", "/nonexistent/flag.sock"}, &stdout, &stderr); code != 4 || !strings.Contains(stderr.String(), "/nonexistent/flag.sock") {
		t.Errorf("with -socket: code %d, stderr %q; want 4 naming the flag's socket", code, stderr.String())
	}
}
