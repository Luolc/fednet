package main

import (
	"strings"
	"testing"
)

func TestSocketFor(t *testing.T) {
	long := "/Users/" + strings.Repeat("x", 90)
	for _, c := range []struct {
		name, goos, env, home, want string
	}{
		{"linux", "linux", "", "/home/u", "/run/fednet-client/fednet.sock"},
		{"darwin", "darwin", "", "/Users/u", "/Users/u/.local/state/fednet-client/fednet.sock"},
		{"the variable wins on linux", "linux", "/x/a.sock", "/home/u", "/x/a.sock"},
		{"the variable wins on darwin", "darwin", "/x/a.sock", "/Users/u", "/x/a.sock"},
		{"darwin without a home", "darwin", "", "", ""},
		{"darwin with a home too long for sun_path", "darwin", "", long, ""},
		{"another system", "plan9", "", "/home/u", ""},
	} {
		if got := socketFor(c.goos, c.env, c.home); got != c.want {
			t.Errorf("%s: socketFor(%q, %q, %q) = %q, want %q", c.name, c.goos, c.env, c.home, got, c.want)
		}
	}
}

// The default path on darwin fits sun_path for a typical home directory.
func TestDarwinSocketFits(t *testing.T) {
	if got := socketFor("darwin", "", "/Users/"+strings.Repeat("x", 30)); got == "" || len(got) >= 104 {
		t.Fatalf("socketFor = %q", got)
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
