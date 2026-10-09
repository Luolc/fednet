package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/Luolc/fednet/internal/approval"
)

// Exit codes of approval verify, besides 1 for a file that cannot be read
// or a public key that cannot be parsed, and 2 for a usage error or an
// approval document that is not well formed. Each check has its own code so
// that an executor's log says which one failed.
const (
	exitBadSignature = 5 // the signature is not the hub's over this content
	exitExpired      = 6 // the approval has expired by this machine's clock
	exitMismatch     = 7 // the action file or the target is not what was approved
	exitUsed         = 8 // this approval_id is already in the used file
)

// now is the clock approval verify judges expiry by; tests replace it.
var now = time.Now

func approvalCommand(args []string, stdout io.Writer) error {
	if len(args) > 0 && args[0] == "verify" {
		return approvalVerify(args[1:], stdout)
	}
	return usageError("fednet approval: want verify")
}

// approvalVerify is what an executor runs before it acts: it checks, in this
// order, the hub's signature over the approval, that the approval has not
// expired, that this machine and agent are its target, that the action file
// hashes to what was approved, and that the approval has not been used
// before; then it records the approval_id as used. The used file is locked
// before the checks start and until the id is appended, so two executors
// with the same approval cannot both pass, and the clock is read only once
// the lock is held, so an approval that expires while waiting for the lock
// is refused.
func approvalVerify(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("fednet approval verify", flag.ContinueOnError)
	pubPath := fs.String("pubkey", "", "file holding the hub's public key, one ssh-ed25519 line (required)")
	approvalPath := fs.String("approval", "", "file holding the approval the hub sent back, JSON with the signature (required)")
	actionPath := fs.String("action", "", "file holding the action's parameters, byte for byte as handed to request-approval (required)")
	machine := fs.String("machine", "", "this machine's client id (required)")
	agent := fs.String("agent", "", "the agent that is about to act (required)")
	usedPath := fs.String("used", "", "file of approval ids already acted on, one a line; created if missing (required)")
	if err := parseFlags(fs, args, 0); err != nil {
		return err
	}
	if *pubPath == "" || *approvalPath == "" || *actionPath == "" || *machine == "" || *agent == "" || *usedPath == "" {
		return usageError("fednet approval verify: -pubkey, -approval, -action, -machine, -agent and -used are required")
	}
	pub, err := approval.ReadPublicKey(*pubPath)
	if err != nil {
		return err
	}
	a, err := readApproval(*approvalPath)
	if err != nil {
		return err
	}
	action, err := os.ReadFile(*actionPath)
	if err != nil {
		return err
	}
	used, err := lockUsed(*usedPath)
	if err != nil {
		return err
	}
	defer used.Close()
	switch err := approval.Verify(pub, a.Content, a.Signature, now()); {
	case errors.Is(err, approval.ErrBadSignature):
		return exitError{exitBadSignature, err}
	case errors.Is(err, approval.ErrExpired):
		return exitError{exitExpired, fmt.Errorf("%w at %s", err, a.ExpiresAt.Format(time.RFC3339))}
	case err != nil:
		return err
	}
	if a.TargetMachine != *machine || a.TargetAgent != *agent {
		return exitError{exitMismatch, fmt.Errorf("approved for agent %q on machine %q, not %q on %q", a.TargetAgent, a.TargetMachine, *agent, *machine)}
	}
	if sum := sha256.Sum256(action); sum != [sha256.Size]byte(a.ParamsSHA256) {
		return exitError{exitMismatch, fmt.Errorf("%s hashes to %x, the approval is for %x", *actionPath, sum, a.ParamsSHA256[:])}
	}
	if err := markUsed(used, a.ApprovalID); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "%s approved by %s, expires %s\n", a.ApprovalID, a.Approver, a.ExpiresAt.Format(time.RFC3339))
	return err
}

// readApproval reads the approval document at path. A document that is not
// well formed is a usage error: the executor was handed something that is
// not an approval.
func readApproval(path string) (approval.Approval, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return approval.Approval{}, err
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	var a approval.Approval
	if err := d.Decode(&a); err != nil {
		return approval.Approval{}, usageError(fmt.Sprintf("%s: %v", path, err))
	}
	if a.ApprovalID == "" || bytes.ContainsAny([]byte(a.ApprovalID), " \t\r\n") || len(a.Signature) == 0 || a.ExpiresAt.IsZero() {
		return approval.Approval{}, usageError(path + ": approval_id, signature and expires_at are required, and approval_id has no white space")
	}
	return a, nil
}

// lockUsed opens the used file at path, creating it if missing, and takes
// an exclusive lock on it that lasts until the file is closed.
func lockUsed(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, fmt.Errorf("lock %s: %w", path, err)
	}
	return f, nil
}

// markUsed appends id as a line of the locked used file f, unless it is
// already there, which is an exitUsed failure. A last line left without its
// newline, by a hand edit or an interrupted write, gets one first, so the
// new id never runs into it.
func markUsed(f *os.File, id string) error {
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if sc.Text() == id {
			return exitError{exitUsed, fmt.Errorf("approval %s was already used (listed in %s)", id, f.Name())}
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("read %s: %w", f.Name(), err)
	}
	line := id + "\n"
	if fi, err := f.Stat(); err != nil {
		return err
	} else if fi.Size() > 0 {
		end := make([]byte, 1)
		if _, err := f.ReadAt(end, fi.Size()-1); err != nil {
			return fmt.Errorf("read %s: %w", f.Name(), err)
		}
		if end[0] != '\n' {
			line = "\n" + line
		}
	}
	if _, err := f.WriteString(line); err != nil {
		return fmt.Errorf("write %s: %w", f.Name(), err)
	}
	return f.Close()
}
