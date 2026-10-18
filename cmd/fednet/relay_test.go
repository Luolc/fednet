package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Luolc/fednet/internal/auth"
	"github.com/Luolc/fednet/internal/hook"
	"github.com/Luolc/fednet/internal/inbound"
	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
)

// alerts is a stand-in for the alerts webhook that records every alert.
type alerts struct {
	mu   sync.Mutex
	text []string
}

func (a *alerts) ServeHTTP(_ http.ResponseWriter, r *http.Request) {
	var m struct{ Text string }
	json.NewDecoder(r.Body).Decode(&m)
	a.mu.Lock()
	defer a.mu.Unlock()
	a.text = append(a.text, m.Text)
}

func (a *alerts) got() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.text...)
}

// End to end, with two machines the relay list lets deliver to each other
// and a third it does not list: a thread one opens for the machine of the
// channel's route reaches that machine's hook as a handoff, the two then
// relay back and forth until the third relay in a row, after which the
// hub delivers nothing and alerts, though the post is in the thread; a
// person posting in the thread lets them relay again. The unlisted
// machine can do neither, and its refused open-thread posts nothing.
func TestRelay(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	outboundInterval = time.Hour
	t.Cleanup(func() { outboundInterval = 0 })
	f := &slack.Fake{}
	f.AddChannel("C1", "the workstation's channel")
	f.AddChannel("C2", "the data machine's channel")
	f.RenameChannel("C2", "data-jobs")
	slackFlags, runs := fakeSlack(t, dir, f, nil)
	wh := &alerts{}
	srv := httptest.NewServer(wh)
	defer srv.Close()
	webhookFile := filepath.Join(dir, "alert-webhook")
	if err := os.WriteFile(webhookFile, []byte(srv.URL+testHookPath+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(dir, "hub.json")
	if err := os.WriteFile(config, []byte(`{"users": {"U1": "maintainer"},
		"channels": {"C1": {"machine": "workstation", "open_thread": ["workstation", "datamachine", "other"]},
		             "C2": {"machine": "datamachine", "open_thread": ["workstation", "other"]}},
		"relay": {"workstation": ["datamachine"], "datamachine": ["workstation"]}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr syncBuffer
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("output:\n%s\n%s", stdout.String(), stderr.String())
		}
	})
	hubDB := filepath.Join(dir, "hub.db")
	type machine struct{ cred, socket, events string }
	machines := map[string]*machine{}
	for _, id := range []string{"workstation", "datamachine", "other"} {
		m := &machine{filepath.Join(dir, id+".cred"), filepath.Join(dir, id+".sock"), filepath.Join(dir, id+".events")}
		machines[id] = m
		var out syncBuffer
		if code := run(ctx, []string{"client", "init", "-id", id, "-credential", m.cred}, &out, &stderr); code != 0 {
			t.Fatalf("client init %s: exit %d", id, code)
		}
		fields := strings.Fields(out.String())
		if code := run(ctx, []string{"hub", "register", "-db", hubDB, fields[0], fields[1]}, &stdout, &stderr); code != 0 {
			t.Fatalf("hub register %s: exit %d", id, code)
		}
	}
	start(t, append([]string{"hub", "-listen", "127.0.0.1:0", "-db", hubDB, "-config", config, "-alert-webhook-file", webhookFile}, slackFlags...), &stdout, &stderr)
	var addr string
	waitFor(t, "the hub to listen", func() bool {
		m := listening.FindStringSubmatch(stdout.String())
		if m != nil {
			addr = m[1]
		}
		return m != nil
	})
	var sr slackRun
	select {
	case sr = <-runs:
	case <-time.After(5 * time.Second):
		t.Fatal("the hub did not start Socket Mode")
	}
	hs, err := store.OpenHub(ctx, hubDB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hs.Close() })
	for id, m := range machines {
		script := filepath.Join(dir, id+".sh")
		if err := os.WriteFile(script, []byte("#!/bin/sh\ncat \"$1\" >> '"+m.events+"'\necho >> '"+m.events+"'\n"), 0o700); err != nil {
			t.Fatal(err)
		}
		start(t, []string{"client", "-hub", "http://" + addr, "-db", filepath.Join(dir, id+".db"), "-credential", m.cred, "-socket", m.socket,
			"/bin/sh", script}, &stdout, &stderr)
		waitFor(t, id+" to connect", func() bool {
			reg, err := hs.Registration(ctx, id)
			return err == nil && reg.Version != ""
		})
	}
	relayed := func(id string) []payload.Message {
		b, _ := os.ReadFile(machines[id].events)
		var ms []payload.Message
		for line := range strings.SplitSeq(strings.TrimSpace(string(b)), "\n") {
			var ev hook.Event
			var m payload.Message
			if json.Unmarshal([]byte(line), &ev) == nil && json.Unmarshal(ev.Payload, &m) == nil && m.Type == payload.Inbound {
				ms = append(ms, m)
			}
		}
		return ms
	}
	cli := func(args ...string) (int, string) {
		var out syncBuffer
		code := run(ctx, append([]string{"client"}, args...), &out, &stderr)
		return code, strings.TrimSpace(out.String())
	}

	if code, out := cli("whoami", "-socket", machines["datamachine"].socket); code != 0 || out != "datamachine" {
		t.Fatalf("whoami: exit %d, printed %q; want 0 and datamachine", code, out)
	}
	cred, err := auth.Read(machines["datamachine"].cred)
	if err != nil {
		t.Fatal(err)
	}
	if code, out := cli("whoami", "-socket", machines["datamachine"].socket, "-json"); code != 0 || strings.Contains(out, cred.Secret) {
		t.Fatalf("whoami -json: exit %d, or the credential in what it printed", code)
	}

	// The unlisted machine may open threads in C2 but deliver to no one.
	if code, _ := cli("open-thread", "-socket", machines["other"].socket, "-channel", "C2", "-owner", "route", "job for the data machine"); code != 3 {
		t.Fatalf("open-thread -owner route from an unlisted machine: exit %d, want 3", code)
	}
	if ms, _ := f.History(ctx, "C2", "0"); len(ms) != 0 {
		t.Fatalf("C2 after the refused open-thread = %+v, want nothing", ms)
	}

	code, thread := cli("open-thread", "-socket", machines["workstation"].socket, "-channel", "C2", "-owner", "route", "job for the data machine")
	if code != 0 || thread == "" {
		t.Fatalf("open-thread -owner route: exit %d, printed %q", code, thread)
	}
	if code, out := cli("threads", "-socket", machines["datamachine"].socket); code != 0 || out != thread {
		t.Fatalf("the data machine's threads: exit %d, %q; want %s", code, out, thread)
	}
	waitFor(t, "the handoff to reach the data machine", func() bool { return len(relayed("datamachine")) == 1 })
	_, root, _ := slack.ParseThreadKey(thread)
	if m := relayed("datamachine")[0]; m.Trigger != payload.Handoff || m.FromClient != "workstation" || m.Hop != 1 || m.Thread != thread ||
		m.Text != "job for the data machine" || m.TS != root || m.ChannelName != "data-jobs" {
		t.Fatalf("handoff = %+v", m)
	}
	if code, _ := cli("post", "-socket", machines["other"].socket, "-thread", thread, "-deliver", "not mine"); code != 3 {
		t.Fatalf("post -deliver from an unlisted machine: exit %d, want 3", code)
	}

	// The data machine owns this thread: its own post there is delivered
	// to no one.
	if code, _ := cli("post", "-socket", machines["datamachine"].socket, "-thread", thread, "-deliver", "working on it"); code != 0 {
		t.Fatalf("post -deliver in its own thread: exit %d", code)
	}

	// The workstation's own thread, where the data machine reports back.
	code, home := cli("open-thread", "-socket", machines["workstation"].socket, "-channel", "C1", "the person's job")
	if code != 0 || home == "" {
		t.Fatalf("open-thread: exit %d, printed %q", code, home)
	}
	_, homeRoot, _ := slack.ParseThreadKey(home)
	// The two answer each other until each thread has had three relays
	// in a row; the next one is not delivered.
	steps := []struct {
		from, thread, root string
		posts              int
	}{
		{"datamachine", home, homeRoot, 1},
		{"workstation", thread, root, 2},
		{"datamachine", home, homeRoot, 2},
		{"workstation", thread, root, 3},
		{"datamachine", home, homeRoot, 3},
		{"workstation", thread, root, 4},
	}
	for _, s := range steps {
		if code, _ := cli("post", "-socket", machines[s.from].socket, "-thread", s.thread, "-deliver", "relay from "+s.from); code != 0 {
			t.Fatalf("post -deliver from %s: exit %d", s.from, code)
		}
		channel, _, _ := slack.ParseThreadKey(s.thread)
		waitFor(t, "the post in the thread", func() bool {
			ms, _ := f.Replies(ctx, channel, s.root)
			return len(ms) == s.posts+1
		})
	}
	waitFor(t, "the relay alerted", func() bool { return len(wh.got()) == 1 })
	if a := wh.got()[0]; !strings.Contains(a, "relay from workstation in thread "+thread+" not delivered") {
		t.Fatalf("alert = %q", a)
	}
	hops := func(id string) []int {
		var hs []int
		for _, m := range relayed(id) {
			if m.Trigger == payload.Relay || m.Trigger == payload.Handoff {
				hs = append(hs, m.Hop)
			}
		}
		return hs
	}
	// The alert can come before the last relay reaches its hook.
	waitFor(t, "three relays each way", func() bool { return len(hops("workstation")) == 3 && len(hops("datamachine")) == 3 })
	if got := hops("workstation"); !slices.Equal(got, []int{1, 2, 3}) {
		t.Fatalf("hops the workstation got = %v, want 1, 2, 3", got)
	}
	if got := hops("datamachine"); !slices.Equal(got, []int{1, 2, 3}) {
		t.Fatalf("hops the data machine got = %v, want the handoff's 1, then 2, 3", got)
	}
	if m := relayed("workstation")[0]; m.Trigger != payload.Relay || m.FromClient != "datamachine" || m.Thread != home || m.Text != "relay from datamachine" {
		t.Fatalf("first relay = %+v", m)
	}

	// Handing the work back in a new thread goes on from the count of the
	// thread it comes from: from one at the limit, nothing is posted.
	if code, _ := cli("open-thread", "-socket", machines["workstation"].socket, "-channel", "C2", "-owner", "route", "-from-thread", home, "back again"); code != 3 {
		t.Fatalf("open-thread -from-thread a thread at the limit: exit %d, want 3", code)
	}
	waitFor(t, "the handoff alerted", func() bool { return len(wh.got()) == 2 })
	if a := wh.got()[1]; !strings.Contains(a, "handoff from workstation to datamachine out of thread "+home+" not delivered") {
		t.Fatalf("alert = %q", a)
	}
	if ms, _ := f.History(ctx, "C2", "0"); len(ms) != 1 {
		t.Fatalf("C2 has %d threads, want just the first", len(ms))
	}

	// A person posts in the thread: it reaches the owner, and the count
	// there starts again.
	ts, err := f.Reply("C2", root, "U1", "go on")
	if err != nil {
		t.Fatal(err)
	}
	if err := sr.r.Handle(ctx, inbound.Event{ID: "Ev1", Channel: "C2", Message: slack.Message{TS: ts, ThreadTS: root, User: "U1", Text: "go on"}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the reply to reach the data machine", func() bool { return len(relayed("datamachine")) == 4 })
	if m := relayed("datamachine")[3]; m.Trigger != payload.Reply || m.Text != "go on" {
		t.Fatalf("the data machine got %+v, want the person's reply", m)
	}
	if code, _ := cli("post", "-socket", machines["workstation"].socket, "-thread", thread, "-deliver", "after the person"); code != 0 {
		t.Fatalf("post -deliver after the person: exit %d", code)
	}
	waitFor(t, "the relay after the person", func() bool { return len(relayed("datamachine")) == 5 })
	if m := relayed("datamachine")[4]; m.Hop != 1 || m.Text != "after the person" {
		t.Fatalf("relay after the person = %+v, want hop 1", m)
	}
	// The data machine hands back out of that thread: the new thread
	// starts one hop on.
	code, back := cli("open-thread", "-socket", machines["datamachine"].socket, "-channel", "C1", "-owner", "route", "-from-thread", thread, "done, over to you")
	if code != 0 || back == "" {
		t.Fatalf("open-thread -from-thread: exit %d, printed %q", code, back)
	}
	waitFor(t, "the handoff back", func() bool { return len(relayed("workstation")) == 4 })
	if m := relayed("workstation")[3]; m.Trigger != payload.Handoff || m.Thread != back || m.Hop != 2 || m.FromClient != "datamachine" {
		t.Fatalf("handoff back = %+v, want hop 2 from datamachine", m)
	}
	if n := len(wh.got()); n != 2 {
		t.Fatalf("%d alerts, want 2", n)
	}
}
