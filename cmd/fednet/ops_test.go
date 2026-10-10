package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/upgrade"
)

// latestRelease answers the latest release as GitHub's API does, with
// the tag the test sets.
type latestRelease struct {
	mu  sync.Mutex
	tag string
}

func (l *latestRelease) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(w, `{"tag_name": %q}`, l.tag)
}

func (l *latestRelease) set(tag string) {
	l.mu.Lock()
	l.tag = tag
	l.mu.Unlock()
}

// client ops, from an agent through each client's socket to a hub that
// knows each client by its credential: the client the config lists gets
// version and upgrade, the other is refused, and every request reaches
// the alerts channel.
func TestClientOps(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	hubDB := filepath.Join(dir, "hub.db")
	// The hub runs a release, so that it may upgrade; its clients, in this
	// process, run the same.
	version = "v0.1.0"
	upgradeTimings.wait = 100 * time.Millisecond
	upgradeTimings.poll = 5 * time.Millisecond
	t.Cleanup(func() {
		version = "dev"
		upgradeTimings.wait, upgradeTimings.poll = 0, 0
	})
	latest := &latestRelease{tag: "v0.1.0"}
	relSrv := httptest.NewServer(latest)
	defer relSrv.Close()
	releases = upgrade.Releases{Latest: relSrv.URL, Download: relSrv.URL + "/download"}
	t.Cleanup(func() {
		releases = upgrade.Releases{Latest: "http://127.0.0.1:9/latest", Download: "http://127.0.0.1:9/download"}
	})

	slackFlags, _ := fakeSlack(t, dir, &slack.Fake{}, nil)
	// Every alert is recorded, the first one too.
	wh := &webhook{refused: true}
	hookSrv := httptest.NewServer(wh)
	defer hookSrv.Close()
	webhookFile := filepath.Join(dir, "alert-webhook")
	if err := os.WriteFile(webhookFile, []byte(hookSrv.URL+testHookPath+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(dir, "hub.json")
	if err := os.WriteFile(config, []byte(`{"upgrade": {"auto": false}, "ops": {"upgrade": ["workstation"], "version": ["workstation"]}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	hs := openHubStore(t, hubDB)
	var stdout, stderr syncBuffer
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("stderr:\n%s", strings.ReplaceAll(stderr.String(), testHookPath, "[REDACTED]"))
		}
	})
	start(t, append([]string{"hub", "-listen", "127.0.0.1:0", "-db", hubDB, "-config", config, "-alert-webhook-file", webhookFile,
		"-upgrade-request", filepath.Join(dir, "hub-upgrade")}, slackFlags...), &stdout, &stderr)
	var addr string
	waitFor(t, "the hub to listen", func() bool {
		m := listening.FindStringSubmatch(stdout.String())
		if m != nil {
			addr = m[1]
		}
		return m != nil
	})
	sockets := make(map[string]string)
	for _, id := range []string{"workstation", "datamachine"} {
		cred := filepath.Join(dir, id+".credential")
		newCredential(t, hs, id, cred)
		sockets[id] = filepath.Join(dir, id+".sock")
		start(t, []string{"client", "-hub", "http://" + addr, "-db", filepath.Join(dir, id+".db"), "-credential", cred, "-socket", sockets[id]}, &stdout, &stderr)
	}
	waitFor(t, "both clients online", func() bool {
		for _, id := range []string{"workstation", "datamachine"} {
			if reg, err := hs.Registration(ctx, id); err != nil || reg.Version != version {
				return false
			}
		}
		return true
	})

	ops := func(client string, args ...string) (int, string) {
		t.Helper()
		var out syncBuffer
		return run(ctx, append([]string{"client", "ops", "-socket", sockets[client]}, args...), &out, &stderr), out.String()
	}
	alerted := func(want string) {
		t.Helper()
		waitFor(t, "the alert "+want, func() bool { return strings.Contains(strings.Join(wh.got(), "\n"), want) })
	}

	code, out := ops("workstation", "version")
	if code != 0 || !strings.Contains(out, "hub：v0.1.0\n最新 release：v0.1.0\n") || !strings.Contains(out, "workstation：v0.1.0 在线") {
		t.Fatalf("ops version from workstation: exit %d, stdout %q", code, out)
	}
	alerted("ops：workstation 请求 version，已回答")
	for _, op := range []string{"version", "upgrade"} {
		if code, out := ops("datamachine", op); code != exitDenied || out != "" {
			t.Fatalf("ops %s from datamachine: exit %d, stdout %q; want %d and nothing", op, code, out, exitDenied)
		}
		alerted("ops：datamachine 请求 " + op + "，被拒")
	}
	if code, _ := ops("workstation", "reboot"); code != 2 {
		t.Fatalf("ops reboot: exit %d, want 2", code)
	}

	code, out = ops("workstation", "upgrade")
	if code != 0 || !strings.Contains(out, "已经是最新") {
		t.Fatalf("ops upgrade on the latest release: exit %d, stdout %q", code, out)
	}
	alerted("ops：workstation 请求 upgrade，hub 已经是最新的 release")
	latest.set("v0.2.0")
	code, out = ops("workstation", "upgrade")
	if code != 0 || out != "开始从 v0.1.0 升到 v0.2.0，过程和结果发到报警 channel\n" {
		t.Fatalf("ops upgrade: exit %d, stdout %q", code, out)
	}
	alerted("ops：workstation 请求 upgrade，开始从 v0.1.0 升到 v0.2.0")
	alerted("开始升级到 v0.2.0 (workstation 经 ops 发起)：先升 client datamachine、workstation")
	// The clients have no upgrade request path, so the rollout stops
	// short of the hub, as it would on /fednet upgrade.
	alerted("升级到 v0.2.0 没完成")
}
