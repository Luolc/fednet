package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Luolc/fednet/internal/route"
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

	// workstation may open threads in C1, and no one in C2; U1 and U2 are
	// on the user list.
	f.AddChannel("C2", "")
	config := filepath.Join(dir, "hub.json")
	if err := os.WriteFile(config, []byte(`{"channels": {"C1": {"open_thread": ["workstation"]}, "C2": {}}, "users": {"U1": "maintainer", "U2": ""}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	stopHub := start(t, []string{"hub", "-listen", "127.0.0.1:0", "-db", hubDB, "-config", config}, &stdout, &stderr)
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

	// open-thread starts a thread this machine owns; threads lists it.
	code, out = fednet("client", "open-thread", "-socket", socket, "-channel", "C1", "nightly report")
	opened := strings.TrimSpace(out)
	if code != 0 || !strings.HasPrefix(opened, "C1/") {
		t.Fatalf("open-thread: exit %d, stdout %q; want a thread key in C1", code, out)
	}
	if owner, err := hs.Owner(ctx, opened); err != nil || owner != "workstation" {
		t.Fatalf("owner of the opened thread = %q, %v; want workstation", owner, err)
	}
	if code, _ := fednet("client", "open-thread", "-socket", socket, "-channel", "C2", "not here"); code != 3 {
		t.Fatalf("open-thread in a channel the config does not allow: exit %d, want 3", code)
	}
	if code, out := fednet("client", "threads", "-socket", socket); code != 0 || out != opened+"\n" {
		t.Fatalf("threads: exit %d, stdout %q; want just %s", code, out, opened)
	}
	code, out = fednet("client", "threads", "-socket", socket, "-json")
	var listed struct {
		Threads []string `json:"threads"`
	}
	if err := json.Unmarshal([]byte(out), &listed); code != 0 || err != nil || !slices.Equal(listed.Threads, []string{opened}) {
		t.Fatalf("threads -json: exit %d, stdout %q; want just %s", code, out, opened)
	}

	// users lists the user list; dm reaches a user on it, and no one else.
	if code, out := fednet("client", "users", "-socket", socket); code != 0 || out != "U1 maintainer\nU2\n" {
		t.Fatalf("users: exit %d, stdout %q; want U1 with its name, then U2", code, out)
	}
	code, out = fednet("client", "users", "-socket", socket, "-json")
	var users struct {
		Users []struct{ ID, Name string } `json:"users"`
	}
	if err := json.Unmarshal([]byte(out), &users); code != 0 || err != nil || len(users.Users) != 2 || users.Users[0].Name != "maintainer" {
		t.Fatalf("users -json: exit %d, stdout %q", code, out)
	}
	if code, _ := fednet("client", "dm", "-socket", socket, "-user", "U1", "daily report"); code != 0 || !slices.Equal(f.DMs("U1"), []string{"daily report"}) {
		t.Fatalf("dm: exit %d, DMs to U1 %q; want the report", code, f.DMs("U1"))
	}
	if code, _ := fednet("client", "dm", "-socket", socket, "-user", "U9", "hello"); code != 3 || len(f.DMs("U9")) != 0 {
		t.Fatalf("dm to a user not on the list: exit %d, DMs %q; want 3 and none", code, f.DMs("U9"))
	}
	if code, _ := fednet("client", "dm", "-socket", socket, "hello"); code != 2 {
		t.Fatalf("dm without -user: exit %d, want 2", code)
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
	if code, out := fednet("hub", "reassign", "-db", hubDB, "workstation", "datamachine"); code != 0 || out != "2\n" {
		t.Fatalf("hub reassign: exit %d, stdout %q; want 2 threads moved", code, out)
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
	if code, _ := fednet("client", "dm", "-socket", socket, "-user", "U1", "while the hub is down"); code != 4 || len(f.DMs("U1")) != 1 {
		t.Fatalf("dm with the hub down: exit %d, DMs to U1 %q; want 4 and only the first", code, f.DMs("U1"))
	}
	if got, err := f.Replies(ctx, "C1", ts); err != nil || !slices.ContainsFunc(got, func(m slack.Message) bool { return m.Text == "on it" }) {
		t.Fatalf("Slack thread = %+v, %v", got, err)
	}
}

// A misspelt field in the hub config is an error, not a silent deny, and so
// is anything after the config object.
func TestReadHubConfig(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "good.json")
	typo := filepath.Join(dir, "typo.json")
	two := filepath.Join(dir, "two.json")
	if err := os.WriteFile(good, []byte(`{"channels": {"C1": {"machine": "workstation", "open_thread": ["workstation"]}, "C2": {}}, "dm": {"machine": "datamachine"}, "users": {"U1": "maintainer"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(typo, []byte(`{"channels": {"C1": {"open_threads": ["workstation"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(two, []byte(`{"channels": {"C1": {"open_thread": ["workstation"]}}} {"channels": {}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := readHubConfig(good)
	if err != nil || !slices.Equal(cfg.openThread()["C1"], []string{"workstation"}) || cfg.Users["U1"] != "maintainer" {
		t.Fatalf("readHubConfig(good) = %+v, %v; want workstation allowed in C1 and U1 on the user list", cfg, err)
	}
	// A channel without a machine is left out of the routing defaults.
	if rt := cfg.route(); !reflect.DeepEqual(rt, route.Config{Defaults: map[string]string{"C1": "workstation"}, DM: "datamachine"}) {
		t.Fatalf("route() = %+v, want C1 to workstation, direct messages to datamachine", rt)
	}
	if _, err := readHubConfig(typo); err == nil || !strings.Contains(err.Error(), "open_threads") {
		t.Fatalf("readHubConfig(typo) = %v, want an error naming the unknown field", err)
	}
	if _, err := readHubConfig(two); err == nil {
		t.Fatal("readHubConfig accepted a second JSON value after the config")
	}
}

func TestHubConfigAlerts(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	cfg, err := readHubConfig(write("set.json", `{"alerts": {"slack_down": "90s", "offline_queued": "1h"}}`))
	if err != nil || time.Duration(cfg.Alerts.SlackDown) != 90*time.Second || time.Duration(cfg.Alerts.OfflineQueued) != time.Hour {
		t.Fatalf("readHubConfig = %+v, %v; want 90s and 1h", cfg.Alerts, err)
	}
	if cfg, err := readHubConfig(write("unset.json", `{}`)); err != nil || cfg.Alerts.SlackDown != 0 || cfg.Alerts.OfflineQueued != 0 {
		t.Fatalf("readHubConfig without alerts = %+v, %v; want zero, the defaults", cfg.Alerts, err)
	}
	for _, bad := range []string{`"5"`, `"-5m"`, `300`} {
		if _, err := readHubConfig(write("bad.json", `{"alerts": {"slack_down": `+bad+`}}`)); err == nil {
			t.Errorf("readHubConfig accepted slack_down %s", bad)
		}
	}
}
