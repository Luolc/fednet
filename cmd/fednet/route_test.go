package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/Luolc/fednet/internal/hook"
	"github.com/Luolc/fednet/internal/inbound"
	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/slack"
)

// End to end, on a hub with no approval key: an agent asks for a route
// through the client, the approver approves the card, the outcome reaches
// the hook unsigned, client routes lists the route, and a mention in the
// channel goes to the machine with the route's scope.
func TestRouteFlow(t *testing.T) {
	ctx := t.Context()
	rig := startApprovalRig(t, false)
	rig.f.AddChannel("C2", "the data channel")
	routes := func() string {
		t.Helper()
		var out syncBuffer
		if code := run(ctx, []string{"client", "routes", "-socket", rig.socket}, &out, rig.stderr); code != 0 {
			t.Fatalf("client routes: exit %d", code)
		}
		return out.String()
	}
	if got := routes(); got != "" {
		t.Fatalf("routes at first = %q, want none", got)
	}
	var out syncBuffer
	code := run(ctx, []string{"client", "request-route", "-socket", rig.socket, "-agent", "lead", "-channel", "C2", "-machine", "workstation", "-scope", "data", "new dataset"}, &out, rig.stderr)
	id := strings.TrimSpace(out.String())
	if code != 0 || id == "" {
		t.Fatalf("request-route: exit %d, printed %q; want 0 and an approval id", code, id)
	}
	tss, cards := rig.f.Cards("C9")
	if len(cards) != 1 || cards[0].ID != id || cards[0].Summary != "改路由：new dataset" {
		t.Fatalf("cards = %+v, want the route request %s", cards, id)
	}
	if got := routes(); got != "" {
		t.Fatalf("routes before the approval = %q, want none", got)
	}
	if err := rig.sr.r.Click(ctx, slack.Click{ID: id, Approve: true, User: "U1", Channel: "C9", TS: tss[0]}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the outcome to reach the hook", func() bool { return len(rig.outcomes(t)) == 1 })
	if m := rig.outcomes(t)[0]; m.ApprovalID != id || m.Outcome != payload.Approved || len(m.Approval) != 0 {
		t.Fatalf("outcome = %+v, want %s approved, unsigned", m, id)
	}
	if got := routes(); got != "C2 workstation data workstation\n" {
		t.Fatalf("routes after the approval = %q", got)
	}
	waitFor(t, "the change told in C9", func() bool {
		ms, err := rig.f.History(ctx, "C9", "0")
		return err == nil && len(ms) == 2 && strings.HasPrefix(ms[1].Text, "路由已改：workstation 提议，U1 批准")
	})

	ts, err := rig.f.Add("C2", slack.Message{User: "U1", Text: "<@" + slack.FakeBot + "> new rows"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rig.sr.r.Handle(ctx, inbound.Event{ID: "Ev1", Channel: "C2", Message: slack.Message{TS: ts, User: "U1", Text: "<@" + slack.FakeBot + "> new rows"}}); err != nil {
		t.Fatal(err)
	}
	var got payload.Message
	waitFor(t, "the mention to reach the hook", func() bool {
		b, err := os.ReadFile(rig.events)
		if err != nil {
			return false
		}
		for line := range strings.SplitSeq(strings.TrimSpace(string(b)), "\n") {
			var ev hook.Event
			if json.Unmarshal([]byte(line), &ev) == nil && json.Unmarshal(ev.Payload, &got) == nil && got.Type == payload.Inbound {
				return true
			}
		}
		return false
	})
	if got.Thread != slack.ThreadKey("C2", ts) || got.Scope != "data" {
		t.Fatalf("hook got %+v, want the mention in C2 with scope data", got)
	}
	rig.stop(t)
}
