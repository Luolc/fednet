package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
)

// The commands that ask the hub, from an agent through the client's socket
// to a hub with a fake Slack, with real credentials.
func TestHubRequests(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	hubDB := filepath.Join(dir, "hub.db")
	credPath := filepath.Join(dir, "credential")
	socket := filepath.Join(dir, "fednet.sock")
	f := &slack.Fake{}
	f.AddChannel("C1", "repo: fednet")
	ts, err := f.Start("C1", "U1", "please fix the build")
	if err != nil {
		t.Fatal(err)
	}
	thread := slack.ThreadKey("C1", ts)
	hubSlack = f
	t.Cleanup(func() { hubSlack = nil })

	var stdout, stderr syncBuffer
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("stderr:\n%s", stderr.String())
		}
	})
	// The output of init holds no credential, so the logs above are safe.
	if code := run(ctx, []string{"client", "init", "-id", "workstation", "-credential", credPath}, &stdout, &stderr); code != 0 {
		t.Fatalf("client init: exit %d", code)
	}
	fields := strings.Fields(stdout.String())
	if code := run(ctx, []string{"hub", "register", "-db", hubDB, fields[0], fields[1]}, &stdout, &stderr); code != 0 {
		t.Fatalf("hub register: exit %d", code)
	}
	hs, err := store.OpenHub(ctx, hubDB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hs.Close() })
	if _, _, err := hs.ClaimAndEnqueue(ctx, thread, "old-workstation", []byte("first")); err != nil {
		t.Fatal(err)
	}

	stopHub := start(t, []string{"hub", "-listen", "127.0.0.1:0", "-db", hubDB}, &stdout, &stderr)
	var addr string
	waitFor(t, "the hub to listen", func() bool {
		m := listening.FindStringSubmatch(stdout.String())
		if m != nil {
			addr = m[1]
		}
		return m != nil
	})
	start(t, []string{"client", "-hub", "http://" + addr, "-db", filepath.Join(dir, "client.db"), "-credential", credPath, "-socket", socket}, &stdout, &stderr)
	waitFor(t, "the client's socket", func() bool {
		_, err := os.Stat(socket)
		return err == nil
	})

	// fednet runs one command and returns its exit code and stdout.
	fednet := func(args ...string) (int, string) {
		t.Helper()
		var out syncBuffer
		return run(ctx, args, &out, &stderr), out.String()
	}

	// read-thread, as text and as JSON.
	if _, err := f.Reply("C1", ts, "B1", "on it"); err != nil {
		t.Fatal(err)
	}
	code, out := fednet("client", "read-thread", "-socket", socket, thread)
	if code != 0 || !strings.Contains(out, "U1: please fix the build\n") || !strings.Contains(out, "B1: on it\n") {
		t.Fatalf("read-thread: exit %d, stdout %q; want both messages", code, out)
	}
	code, out = fednet("client", "read-thread", "-socket", socket, "-json", thread)
	var res struct {
		Messages []slack.Message `json:"messages"`
		Text     string          `json:"text"`
	}
	if err := json.Unmarshal([]byte(out), &res); code != 0 || err != nil || len(res.Messages) != 2 || res.Messages[0].TS != ts {
		t.Fatalf("read-thread -json: exit %d, stdout %q; want both messages", code, out)
	}
	if code, _ := fednet("client", "read-thread", "-socket", socket, "C1/1600000000.000001"); code != 1 || !strings.Contains(stderr.String(), "no thread C1/1600000000.000001") {
		t.Fatalf("read-thread of a missing thread: exit %d, want 1 and a message saying so", code)
	}
	if code, _ := fednet("client", "read-thread", "-socket", socket, "C1"); code != 2 {
		t.Fatalf("read-thread of a malformed key: exit %d, want 2", code)
	}

	// channel-context set, then get.
	body := filepath.Join(dir, "context.md")
	if err := os.WriteFile(body, []byte("repos: fednet, fleet"), 0o600); err != nil {
		t.Fatal(err)
	}
	if code, _ := fednet("client", "channel-context", "set", "-socket", socket, "-body-file", body, "C1"); code != 0 {
		t.Fatalf("channel-context set: exit %d", code)
	}
	if code, out := fednet("client", "channel-context", "get", "-socket", socket, "C1"); code != 0 || out != "repos: fednet, fleet\n" {
		t.Fatalf("channel-context get: exit %d, stdout %q", code, out)
	}
	code, out = fednet("client", "channel-context", "get", "-socket", socket, "-json", "C1")
	if err := json.Unmarshal([]byte(out), &res); code != 0 || err != nil || res.Text != "repos: fednet, fleet" {
		t.Fatalf("channel-context get -json: exit %d, stdout %q", code, out)
	}

	// adopt takes the thread over from the old machine; hub reassign then
	// moves everything this machine owns to another.
	if code, _ := fednet("client", "adopt", "-socket", socket, thread); code != 0 {
		t.Fatalf("adopt: exit %d", code)
	}
	if owner, err := hs.Owner(ctx, thread); err != nil || owner != "workstation" {
		t.Fatalf("owner after adopt = %q, %v; want workstation", owner, err)
	}
	if code, _ := fednet("client", "adopt", "-socket", socket, "C1/1600000000.000001"); code != 1 || !strings.Contains(stderr.String(), "has no owner") {
		t.Fatalf("adopt of an unowned thread: exit %d, want 1 and a message saying so", code)
	}
	if code, out := fednet("hub", "reassign", "-db", hubDB, "workstation", "datamachine"); code != 0 || out != "1\n" {
		t.Fatalf("hub reassign: exit %d, stdout %q; want 1 thread moved", code, out)
	}
	if owner, err := hs.Owner(ctx, thread); err != nil || owner != "datamachine" {
		t.Fatalf("owner after reassign = %q, %v; want datamachine", owner, err)
	}

	// A revoked client is denied; with the hub down a request fails at once.
	if code, _ := fednet("hub", "revoke", "-db", hubDB, "workstation"); code != 0 {
		t.Fatalf("hub revoke: exit %d", code)
	}
	if code, _ := fednet("client", "read-thread", "-socket", socket, thread); code != 3 {
		t.Fatalf("read-thread by a revoked client: exit %d, want 3", code)
	}
	if code := stopHub(); code != 0 {
		t.Fatalf("hub exited %d", code)
	}
	if code, _ := fednet("client", "read-thread", "-socket", socket, thread); code != 4 {
		t.Fatalf("read-thread with the hub down: exit %d, want 4", code)
	}
	if got, err := f.Replies(ctx, "C1", ts); err != nil || !slices.ContainsFunc(got, func(m slack.Message) bool { return m.Text == "on it" }) {
		t.Fatalf("Slack thread = %+v, %v", got, err)
	}
}
