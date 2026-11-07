package main

import (
	"os"
	"path/filepath"
	"runtime"

	"github.com/Luolc/fednet/internal/local"
)

// darwinSunPath is the size of sun_path on darwin, in bytes.
const darwinSunPath = 104

// socketEnv names the environment variable that gives the client's socket
// when -socket is not.
const socketEnv = "FEDNET_SOCKET"

// defaultSocket is the client's socket when -socket is not given:
// $FEDNET_SOCKET, else the default of the operating system. It is empty
// when there is none, which the commands report as a missing -socket.
func defaultSocket() string {
	home, _ := os.UserHomeDir()
	return socketFor(runtime.GOOS, os.Getenv(socketEnv), home)
}

// socketFor is defaultSocket for the given system, value of the variable
// and home directory. On linux the client runs as a service with a
// runtime directory of its own. On darwin there is no such directory, so
// the socket is in the user's state directory; the path has no space, and
// the path the socket is bound at (local.BindPathLen) must fit sun_path,
// 104 bytes there including the terminating NUL, so a home directory too
// long for it gives no default.
func socketFor(goos, env, home string) string {
	if env != "" {
		return env
	}
	switch goos {
	case "linux":
		return "/run/fednet-client/fednet.sock"
	case "darwin":
		if home == "" {
			return ""
		}
		path := filepath.Join(home, ".local", "state", "fednet-client", "fednet.sock")
		if local.BindPathLen(path) >= darwinSunPath {
			return ""
		}
		return path
	}
	return ""
}

// socketDefaultsDoc says what defaultSocket falls back to, for the usage.
const socketDefaultsDoc = "/run/fednet-client/fednet.sock on linux, ~/.local/state/fednet-client/fednet.sock on darwin"
