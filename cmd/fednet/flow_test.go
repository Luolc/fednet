package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Luolc/fednet/internal/approval"
	"github.com/Luolc/fednet/internal/auth"
	"github.com/Luolc/fednet/internal/hook"
	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
)

// approvalRig is a hub with Slack and an approval key, and a client whose
// hook appends every event to a file.
type approvalRig struct {
	dir, socket, events, pub, keyPEM string
	f                                *slack.Fake
	sr                               slackRun
	stdout, stderr                   *syncBuffer
	stopHub, stopClient              func() int
}

// startApprovalRig starts the hub, with the key when withKey is set, and
// the client, and waits until the client is connected. The hub's config
// sends approval cards to C9, which U1 and U2 may decide.
func startApprovalRig(t *testing.T, withKey bool) *approvalRig {
	t.Helper()
	ctx := t.Context()
	dir := t.TempDir()
	rig := &approvalRig{dir: dir, socket: filepath.Join(dir, "fednet.sock"), events: filepath.Join(dir, "events"), f: &slack.Fake{}, stdout: &syncBuffer{}, stderr: &syncBuffer{}}
	outboundInterval = time.Hour
	t.Cleanup(func() { outboundInterval = 0 })
	rig.f.AddChannel("C9", "approvals")
	slackFlags, runs := fakeSlack(t, dir, rig.f, nil)
	config := filepath.Join(dir, "hub.json")
	if err := os.WriteFile(config, []byte(`{"users": {"U1": "maintainer", "U2": "colleague"}, "approvals": {"channel": "C9", "approvers": ["U1", "U2"]}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	rig.keyPEM = string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	keyPath := filepath.Join(dir, "approval-key")
	if err := os.WriteFile(keyPath, []byte(rig.keyPEM), 0o600); err != nil {
		t.Fatal(err)
	}
	rig.pub = filepath.Join(dir, "hub.pub")
	if err := os.WriteFile(rig.pub, []byte(approval.FormatPublicKey(pub)+" hub\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	credPath := filepath.Join(dir, "credential")
	var secret string
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		out := rig.stdout.String() + "\n" + rig.stderr.String()
		for _, s := range []string{secret, testAppToken, testBotToken, rig.keyPEM} {
			if s != "" {
				out = strings.ReplaceAll(out, s, "[REDACTED]")
			}
		}
		t.Logf("output:\n%s", out)
	})
	if code := run(ctx, []string{"client", "init", "-id", "workstation", "-credential", credPath}, rig.stdout, rig.stderr); code != 0 {
		t.Fatalf("client init: exit %d", code)
	}
	cred, err := auth.Read(credPath)
	if err != nil {
		t.Fatal(err)
	}
	secret = cred.Secret
	fields := strings.Fields(rig.stdout.String())
	hubDB := filepath.Join(dir, "hub.db")
	if code := run(ctx, []string{"hub", "register", "-db", hubDB, fields[0], fields[1]}, rig.stdout, rig.stderr); code != 0 {
		t.Fatalf("hub register: exit %d", code)
	}
	args := append([]string{"hub", "-listen", "127.0.0.1:0", "-db", hubDB, "-config", config}, slackFlags...)
	if withKey {
		args = append(args, "-approval-key-file", keyPath)
	}
	rig.stopHub = start(t, args, rig.stdout, rig.stderr)
	var addr string
	waitFor(t, "the hub to listen", func() bool {
		m := listening.FindStringSubmatch(rig.stdout.String())
		if m != nil {
			addr = m[1]
		}
		return m != nil
	})
	select {
	case rig.sr = <-runs:
	case <-time.After(5 * time.Second):
		t.Fatal("the hub did not start Socket Mode")
	}
	script := filepath.Join(dir, "hook.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\ncat \"$1\" >> \"$FEDNET_TEST_EVENTS\"\necho >> \"$FEDNET_TEST_EVENTS\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FEDNET_TEST_EVENTS", rig.events)
	rig.stopClient = start(t, []string{"client", "-hub", "http://" + addr, "-db", filepath.Join(dir, "client.db"), "-credential", credPath, "-socket", rig.socket,
		"-hook-env", "FEDNET_TEST_EVENTS", "/bin/sh", script}, rig.stdout, rig.stderr)
	hs, err := store.OpenHub(ctx, hubDB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hs.Close() })
	waitFor(t, "the client to connect", func() bool {
		reg, err := hs.Registration(ctx, "workstation")
		return err == nil && reg.Version != ""
	})
	return rig
}

// request runs fednet client request-approval for the action file and
// returns the exit code and the approval id.
func (rig *approvalRig) request(t *testing.T, ctx context.Context, actionPath string, extra ...string) (int, string) {
	t.Helper()
	var out syncBuffer
	args := append([]string{"client", "request-approval", "-socket", rig.socket, "-agent", "ops-exec", "-action", actionPath}, extra...)
	code := run(ctx, append(args, "delete the example bucket"), &out, rig.stderr)
	return code, strings.TrimSpace(out.String())
}

// outcomes returns the approval outcomes the hook has received so far.
func (rig *approvalRig) outcomes(t *testing.T) []payload.Message {
	t.Helper()
	b, err := os.ReadFile(rig.events)
	if err != nil {
		return nil
	}
	var ms []payload.Message
	for line := range strings.SplitSeq(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		var ev hook.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatal(err)
		}
		var m payload.Message
		if err := json.Unmarshal(ev.Payload, &m); err != nil {
			t.Fatal(err)
		}
		if m.Type == payload.Approval {
			ms = append(ms, m)
		}
	}
	return ms
}

// stop stops both processes and checks that no credential, the key
// included, reached the output.
func (rig *approvalRig) stop(t *testing.T) {
	t.Helper()
	if code := rig.stopClient(); code != 0 {
		t.Errorf("client exited %d", code)
	}
	if code := rig.stopHub(); code != 0 {
		t.Errorf("hub exited %d", code)
	}
	out := rig.stdout.String() + rig.stderr.String()
	// The PEM body alone, without the header lines, is what a careless
	// log would quote.
	body := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(rig.keyPEM, "-----BEGIN PRIVATE KEY-----"), "-----END PRIVATE KEY-----\n"))
	for name, s := range map[string]string{"app token": testAppToken, "bot token": testBotToken, "private key": body} {
		if strings.Contains(out, s) {
			t.Errorf("output contains the %s", name)
		}
	}
}

// End to end: an agent asks through the client, the card appears in
// Slack, clicks that do not count change nothing, the approver's click
// signs the approval, which reaches the hook and verifies with the hub's
// public key once; a second request is rejected and comes back unsigned.
func TestApprovalFlow(t *testing.T) {
	ctx := t.Context()
	rig := startApprovalRig(t, true)
	actionPath := filepath.Join(rig.dir, "action.json")
	action := "{\n  \"op\": \"delete-bucket\",\n  \"bucket\": \"example\"\n}\n"
	if err := os.WriteFile(actionPath, []byte(action), 0o644); err != nil {
		t.Fatal(err)
	}
	code, id := rig.request(t, ctx, actionPath, "-requester", "U2")
	if code != 0 || id == "" {
		t.Fatalf("request-approval: exit %d, printed %q; want 0 and an approval id", code, id)
	}
	tss, cards := rig.f.Cards("C9")
	if len(cards) != 1 || cards[0].ID != id || cards[0].Outcome != "" || cards[0].Params != action || cards[0].Machine != "workstation" || cards[0].Agent != "ops-exec" || cards[0].Summary != "delete the example bucket" {
		t.Fatalf("cards = %+v, want one pending card for %s with the action as in the file", cards, id)
	}
	ts := tss[0]

	if cards[0].Requester != "U2" {
		t.Fatalf("card = %+v, want the requester note U2", cards[0])
	}
	// Not on the list, a bot: nothing is decided.
	for _, c := range []slack.Click{
		{ID: id, Approve: true, User: "U9", Channel: "C9", TS: ts},
		{ID: id, Approve: true, User: "U1", Bot: true, Channel: "C9", TS: ts},
	} {
		if err := rig.sr.r.Click(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "the hints to the clickers", func() bool { return len(rig.f.Whispers("U9")) == 1 && len(rig.f.Whispers("U1")) == 1 })
	if c, _ := rig.f.Card("C9", ts); c.Outcome != "" {
		t.Fatalf("card after clicks that do not count = %+v, want still pending", c)
	}
	if ms := rig.outcomes(t); len(ms) != 0 {
		t.Fatalf("outcomes after clicks that do not count = %+v, want none", ms)
	}

	// The approver clicks twice: one outcome, signed, reaches the hook.
	for range 2 {
		if err := rig.sr.r.Click(ctx, slack.Click{ID: id, Approve: true, User: "U1", Channel: "C9", TS: ts}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "the outcome to reach the hook", func() bool { return len(rig.outcomes(t)) == 1 })
	m := rig.outcomes(t)[0]
	if m.ApprovalID != id || m.Outcome != payload.Approved || m.Approver != "U1" || m.Agent != "ops-exec" || m.Text != "delete the example bucket" || len(m.Approval) == 0 {
		t.Fatalf("outcome = %+v, want %s approved by U1 with the approval document", m, id)
	}
	waitFor(t, "the card to show the approval", func() bool { c, _ := rig.f.Card("C9", ts); return c.Outcome == payload.Approved })
	if c, _ := rig.f.Card("C9", ts); c.Approver != "U1" {
		t.Fatalf("card after approval = %+v, want approved by U1", c)
	}
	approvalPath, used := filepath.Join(rig.dir, "approval.json"), filepath.Join(rig.dir, "used")
	if err := os.WriteFile(approvalPath, m.Approval, 0o644); err != nil {
		t.Fatal(err)
	}
	verifyArgs := []string{"approval", "verify", "-pubkey", rig.pub, "-approval", approvalPath, "-action", actionPath, "-machine", "workstation", "-agent", "ops-exec", "-used", used}
	if code, out, errOut := verify(t, verifyArgs); code != 0 || !strings.HasPrefix(out, id+" approved by U1") {
		t.Fatalf("approval verify: exit %d, stdout %q, stderr %q; want 0", code, out, errOut)
	}
	if code, _, _ := verify(t, verifyArgs); code != exitUsed {
		t.Fatalf("approval verify again: exit %d, want %d", code, exitUsed)
	}
	// The action file changed by one byte: refused.
	if err := os.WriteFile(actionPath, []byte(strings.Replace(action, "example", "exampl3", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	if code, _, _ := verify(t, append(verifyArgs, "-used", filepath.Join(rig.dir, "used2"))); code != exitMismatch {
		t.Fatalf("approval verify with a changed action: exit %d, want %d", code, exitMismatch)
	}

	// An action the card cannot show whole is refused as a bad request.
	big := filepath.Join(rig.dir, "big.json")
	if err := os.WriteFile(big, []byte(`"`+strings.Repeat("x", 46*2900)+`"`), 0o644); err != nil {
		t.Fatal(err)
	}
	if code, out := rig.request(t, ctx, big); code != exitBadRequest || out != "" {
		t.Fatalf("request-approval with an action too big for a card: exit %d, printed %q; want %d and nothing", code, out, exitBadRequest)
	}
	if _, cards := rig.f.Cards("C9"); len(cards) != 1 {
		t.Fatalf("%d cards after the refused request, want still 1", len(cards))
	}

	// A second request, rejected: the outcome comes back without a
	// signature, and a click to approve after that changes nothing. The
	// approver named as requester decides like any approver.
	code, id2 := rig.request(t, ctx, actionPath, "-requester", "U2")
	if code != 0 || id2 == "" || id2 == id {
		t.Fatalf("second request-approval: exit %d, printed %q", code, id2)
	}
	tss, _ = rig.f.Cards("C9")
	ts2 := tss[1]
	if err := rig.sr.r.Click(ctx, slack.Click{ID: id2, Approve: false, User: "U2", Channel: "C9", TS: ts2}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the rejection to reach the hook", func() bool { return len(rig.outcomes(t)) == 2 })
	m = rig.outcomes(t)[1]
	if m.ApprovalID != id2 || m.Outcome != payload.Rejected || m.Approver != "U2" || len(m.Approval) != 0 {
		t.Fatalf("outcome = %+v, want %s rejected by U2 without an approval document", m, id2)
	}
	if err := rig.sr.r.Click(ctx, slack.Click{ID: id2, Approve: true, User: "U1", Channel: "C9", TS: ts2}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the card to show the rejection", func() bool { c, _ := rig.f.Card("C9", ts2); return c.Outcome == payload.Rejected })
	waitFor(t, "the hint to U1", func() bool { return len(rig.f.Whispers("U1")) == 2 })
	if ms := rig.outcomes(t); len(ms) != 2 {
		t.Fatalf("outcomes after approving a rejected card = %+v, want still two", ms)
	}
	rig.stop(t)
}

// An approval nobody decides expires: the hub's sweep sends the outcome,
// unsigned, and the card says so; a click after that changes nothing.
func TestApprovalExpires(t *testing.T) {
	ctx := t.Context()
	approvalTTL, approvalInterval = 50*time.Millisecond, 10*time.Millisecond
	t.Cleanup(func() { approvalTTL, approvalInterval = 0, 0 })
	rig := startApprovalRig(t, true)
	actionPath := filepath.Join(rig.dir, "action.json")
	if err := os.WriteFile(actionPath, []byte(`{"op": "delete-bucket"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	code, id := rig.request(t, ctx, actionPath)
	if code != 0 || id == "" {
		t.Fatalf("request-approval: exit %d, printed %q", code, id)
	}
	waitFor(t, "the expiry to reach the hook", func() bool { return len(rig.outcomes(t)) == 1 })
	m := rig.outcomes(t)[0]
	if m.ApprovalID != id || m.Outcome != payload.Expired || m.Approver != "" || len(m.Approval) != 0 {
		t.Fatalf("outcome = %+v, want %s expired, unsigned", m, id)
	}
	waitFor(t, "the card to show the expiry", func() bool {
		_, cards := rig.f.Cards("C9")
		return len(cards) == 1 && cards[0].Outcome == payload.Expired
	})
	tss, _ := rig.f.Cards("C9")
	if err := rig.sr.r.Click(ctx, slack.Click{ID: id, Approve: true, User: "U1", Channel: "C9", TS: tss[0]}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the hint to U1", func() bool { return len(rig.f.Whispers("U1")) == 1 })
	if w := rig.f.Whispers("U1"); !strings.Contains(w[0], "已过期") {
		t.Fatalf("U1 was told %q, want that the card expired", w)
	}
	if ms := rig.outcomes(t); len(ms) != 1 {
		t.Fatalf("outcomes after approving an expired card = %+v, want still one", ms)
	}
	rig.stop(t)
}

// Without the key the hub refuses a request outright: no card, exit 3.
func TestApprovalOffWithoutKey(t *testing.T) {
	ctx := t.Context()
	rig := startApprovalRig(t, false)
	actionPath := filepath.Join(rig.dir, "action.json")
	if err := os.WriteFile(actionPath, []byte(`{"op": "delete-bucket"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	code, id := rig.request(t, ctx, actionPath)
	if code != exitDenied || id != "" {
		t.Fatalf("request-approval without a key: exit %d, printed %q; want %d and nothing", code, id, exitDenied)
	}
	if !strings.Contains(rig.stderr.String(), "not set up") {
		t.Fatalf("stderr %q does not say approvals are not set up", rig.stderr.String())
	}
	if _, cards := rig.f.Cards("C9"); len(cards) != 0 {
		t.Fatalf("cards = %+v, want none", cards)
	}
	if !strings.Contains(rig.stderr.String(), "approvals are off") {
		t.Fatal("the hub did not log that approvals are off")
	}
	rig.stop(t)
}
