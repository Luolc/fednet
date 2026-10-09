package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Luolc/fednet/internal/approval"
)

// approvalSetup is where setupApproval put the hub's public key, the action
// file and the used file.
type approvalSetup struct {
	dir, pub, action, used string
}

// setupApproval writes pub as the hub's public key, an action file and an
// approval of it for ops-exec on workstation signed by signer, and returns
// the paths and the approval file. change edits the content before it is
// signed.
func setupApproval(t *testing.T, signer ed25519.PrivateKey, pub ed25519.PublicKey, change func(*approval.Content)) (approvalSetup, string) {
	t.Helper()
	dir := t.TempDir()
	s := approvalSetup{dir: dir, pub: filepath.Join(dir, "hub.pub"), action: filepath.Join(dir, "action.json"), used: filepath.Join(dir, "used")}
	action := []byte(`{"op":"delete-bucket","bucket":"example"}` + "\n")
	if err := os.WriteFile(s.pub, []byte(approval.FormatPublicKey(pub)+" hub\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.action, action, 0o644); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	c := approval.Content{
		ApprovalID:    "apr-7",
		ParamsSHA256:  sha256.Sum256(action),
		TargetMachine: "workstation",
		TargetAgent:   "ops-exec",
		Approver:      "U123",
		ApprovedAt:    now,
		ExpiresAt:     now.Add(approval.TTL),
		Nonce:         []byte("0123456789abcdef"),
	}
	if change != nil {
		change(&c)
	}
	path := filepath.Join(dir, "approval.json")
	b, err := json.Marshal(approval.Approval{Content: c, Signature: approval.Sign(signer, c)})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	return s, path
}

func (s approvalSetup) args(approvalPath string) []string {
	return []string{"approval", "verify", "-pubkey", s.pub, "-approval", approvalPath, "-action", s.action, "-machine", "workstation", "-agent", "ops-exec", "-used", s.used}
}

func verify(t *testing.T, args []string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func TestApprovalVerify(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, other, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("ok then used", func(t *testing.T) {
		s, a := setupApproval(t, priv, pub, nil)
		code, out, errOut := verify(t, s.args(a))
		if code != 0 || !strings.HasPrefix(out, "apr-7 approved by U123, expires ") {
			t.Fatalf("exit %d, stdout %q, stderr %q", code, out, errOut)
		}
		used, _ := os.ReadFile(s.used)
		if string(used) != "apr-7\n" {
			t.Errorf("used file = %q, want apr-7", used)
		}
		code, _, errOut = verify(t, s.args(a))
		if code != exitUsed || !strings.Contains(errOut, "already used") {
			t.Errorf("second run: exit %d, stderr %q; want %d", code, errOut, exitUsed)
		}
		if used, _ := os.ReadFile(s.used); string(used) != "apr-7\n" {
			t.Errorf("used file after the refused run = %q", used)
		}
	})
	t.Run("used file without a final newline", func(t *testing.T) {
		s, a := setupApproval(t, priv, pub, nil)
		os.WriteFile(s.used, []byte("previous-id"), 0o600)
		if code, _, errOut := verify(t, s.args(a)); code != 0 {
			t.Fatalf("first run: exit %d, stderr %q", code, errOut)
		}
		if used, _ := os.ReadFile(s.used); string(used) != "previous-id\napr-7\n" {
			t.Errorf("used file = %q, want previous-id and apr-7 on their own lines", used)
		}
		if code, _, _ := verify(t, s.args(a)); code != exitUsed {
			t.Errorf("second run: exit %d, want %d", code, exitUsed)
		}
	})
	t.Run("expires while waiting for the lock", func(t *testing.T) {
		expiry := time.Now().Add(200 * time.Millisecond)
		s, a := setupApproval(t, priv, pub, func(c *approval.Content) { c.ExpiresAt = expiry })
		held, err := os.OpenFile(s.used, os.O_RDWR|os.O_CREATE, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if err := syscall.Flock(int(held.Fd()), syscall.LOCK_EX); err != nil {
			t.Fatal(err)
		}
		done := make(chan int, 1)
		go func() {
			code, _, _ := verify(t, s.args(a))
			done <- code
		}()
		select {
		case code := <-done:
			t.Fatalf("verify returned %d while the used file was locked", code)
		case <-time.After(time.Until(expiry) + 50*time.Millisecond):
		}
		held.Close()
		if code := <-done; code != exitExpired {
			t.Errorf("exit %d, want %d", code, exitExpired)
		}
		if used, _ := os.ReadFile(s.used); len(used) != 0 {
			t.Errorf("used file = %q, want empty", used)
		}
	})
	t.Run("action changed", func(t *testing.T) {
		s, a := setupApproval(t, priv, pub, nil)
		action, _ := os.ReadFile(s.action)
		action[len(action)-3] ^= 0x20
		os.WriteFile(s.action, action, 0o644)
		code, _, errOut := verify(t, s.args(a))
		if code != exitMismatch || !strings.Contains(errOut, "hashes to") {
			t.Errorf("exit %d, stderr %q; want %d", code, errOut, exitMismatch)
		}
		if used, _ := os.ReadFile(s.used); len(used) != 0 {
			t.Errorf("used file = %q after a refused approval, want empty", used)
		}
	})
	t.Run("other machine", func(t *testing.T) {
		s, a := setupApproval(t, priv, pub, func(c *approval.Content) { c.TargetMachine = "datamachine" })
		code, _, errOut := verify(t, s.args(a))
		if code != exitMismatch || !strings.Contains(errOut, `on machine "datamachine", not "ops-exec" on "workstation"`) {
			t.Errorf("exit %d, stderr %q; want %d", code, errOut, exitMismatch)
		}
	})
	t.Run("other agent", func(t *testing.T) {
		s, a := setupApproval(t, priv, pub, func(c *approval.Content) { c.TargetAgent = "other" })
		if code, _, errOut := verify(t, s.args(a)); code != exitMismatch {
			t.Errorf("exit %d, stderr %q; want %d", code, errOut, exitMismatch)
		}
	})
	t.Run("expired", func(t *testing.T) {
		s, a := setupApproval(t, priv, pub, func(c *approval.Content) {
			c.ApprovedAt = c.ApprovedAt.Add(-2 * approval.TTL)
			c.ExpiresAt = c.ExpiresAt.Add(-2 * approval.TTL)
		})
		code, _, errOut := verify(t, s.args(a))
		if code != exitExpired || !strings.Contains(errOut, "expired at") {
			t.Errorf("exit %d, stderr %q; want %d", code, errOut, exitExpired)
		}
	})
	t.Run("other key", func(t *testing.T) {
		s, a := setupApproval(t, other, pub, nil)
		code, _, errOut := verify(t, s.args(a))
		if code != exitBadSignature || !strings.Contains(errOut, "signature does not match") {
			t.Errorf("exit %d, stderr %q; want %d", code, errOut, exitBadSignature)
		}
	})
	t.Run("field changed after signing", func(t *testing.T) {
		s, a := setupApproval(t, priv, pub, nil)
		b, _ := os.ReadFile(a)
		b = bytes.Replace(b, []byte(`"approver_slack_id":"U123"`), []byte(`"approver_slack_id":"U124"`), 1)
		os.WriteFile(a, b, 0o644)
		if code, _, errOut := verify(t, s.args(a)); code != exitBadSignature {
			t.Errorf("exit %d, stderr %q; want %d", code, errOut, exitBadSignature)
		}
	})
	t.Run("bad public key", func(t *testing.T) {
		s, a := setupApproval(t, priv, pub, nil)
		os.WriteFile(s.pub, []byte("ssh-rsa AAAAB3NzaC1yc2E= hub\n"), 0o644)
		code, _, errOut := verify(t, s.args(a))
		if code != 1 || !strings.Contains(errOut, `key type "ssh-rsa", want ssh-ed25519`) {
			t.Errorf("exit %d, stderr %q; want 1", code, errOut)
		}
	})
	t.Run("malformed approval", func(t *testing.T) {
		s, a := setupApproval(t, priv, pub, nil)
		for name, doc := range map[string]string{
			"not json":      "{",
			"unknown field": `{"approval_id":"x","signature":"AA==","expires_at":"2026-10-09T00:00:00Z","extra":1}`,
			"no signature":  `{"approval_id":"x","expires_at":"2026-10-09T00:00:00Z"}`,
			"id with space": `{"approval_id":"x y","signature":"AA==","expires_at":"2026-10-09T00:00:00Z"}`,
			"bad hash":      `{"approval_id":"x","signature":"AA==","expires_at":"2026-10-09T00:00:00Z","params_sha256":"zz"}`,
		} {
			os.WriteFile(a, []byte(doc), 0o644)
			if code, _, errOut := verify(t, s.args(a)); code != 2 || !strings.Contains(errOut, "approval.json") {
				t.Errorf("%s: exit %d, stderr %q; want 2", name, code, errOut)
			}
		}
	})
	t.Run("missing files", func(t *testing.T) {
		s, a := setupApproval(t, priv, pub, nil)
		for _, args := range [][]string{
			replace(s.args(a), s.pub, filepath.Join(s.dir, "nope")),
			replace(s.args(a), a, filepath.Join(s.dir, "nope")),
			replace(s.args(a), s.action, filepath.Join(s.dir, "nope")),
		} {
			if code, _, errOut := verify(t, args); code != 1 || !strings.Contains(errOut, "no such file") {
				t.Errorf("exit %d, stderr %q; want 1", code, errOut)
			}
		}
	})
}

// replace returns args with every old swapped for new.
func replace(args []string, old, new string) []string {
	out := make([]string, len(args))
	for i, a := range args {
		if a == old {
			a = new
		}
		out[i] = a
	}
	return out
}
