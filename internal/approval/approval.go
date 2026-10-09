// Package approval is the signature on a user's approval of a high-risk
// action. The hub holds the only private key and signs what the user
// approved: which approval, the hash of the action's parameters, which
// agent on which machine may act, who approved, when it was approved and
// when it expires, and a nonce. The program that acts verifies the
// signature with the hub's public key before it does anything, so an agent
// cannot forge an approval or reuse one for other parameters.
//
// The signature covers a canonical encoding of the content, not a JSON
// document: every field is written length-prefixed in a fixed order, so
// two different contents never encode the same and the same content always
// encodes the same, however the document that carried it was formatted.
package approval

import (
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

// TTL is how long after it is approved an approval may be used.
const TTL = time.Hour

// Errors Verify returns. Anything else it returns is about the key.
var (
	ErrBadSignature = errors.New("approval: signature does not match")
	ErrExpired      = errors.New("approval: expired")
)

// Digest is a SHA-256 hash, written as hex in JSON.
type Digest [sha256.Size]byte

// MarshalText writes d as hex.
func (d Digest) MarshalText() ([]byte, error) {
	return []byte(hex.EncodeToString(d[:])), nil
}

// UnmarshalText reads the hex of a SHA-256 hash.
func (d *Digest) UnmarshalText(text []byte) error {
	b, err := hex.DecodeString(string(text))
	if err != nil {
		return fmt.Errorf("approval: params_sha256 is not hex: %v", err)
	}
	if len(b) != sha256.Size {
		return fmt.Errorf("approval: params_sha256 has %d bytes, want %d", len(b), sha256.Size)
	}
	copy(d[:], b)
	return nil
}

// Content is what the signature covers.
type Content struct {
	// ApprovalID identifies the approval; an executor acts on each id once.
	ApprovalID string `json:"approval_id"`
	// ParamsSHA256 is the hash of the action's parameters, a JSON document
	// hashed byte for byte as the agent handed it in.
	ParamsSHA256 Digest `json:"params_sha256"`
	// TargetMachine and TargetAgent say which agent on which machine the
	// approval is for.
	TargetMachine string `json:"target_machine"`
	TargetAgent   string `json:"target_agent"`
	// Approver is the Slack user id of the person who approved.
	Approver string `json:"approver_slack_id"`
	// ApprovedAt and ExpiresAt bound when the approval may be used; the
	// executor judges them by its own clock.
	ApprovedAt time.Time `json:"approved_at"`
	ExpiresAt  time.Time `json:"expires_at"`
	// Nonce is random, so that two approvals never sign the same bytes.
	Nonce []byte `json:"nonce"`
}

// Approval is the document the executor is handed: the content and the
// hub's signature over it.
type Approval struct {
	Content
	Signature []byte `json:"signature"`
}

// Encode returns the canonical bytes the signature covers: a format tag,
// then every field of c, each preceded by its length as 4 bytes big-endian.
// A time is its Unix seconds as 8 bytes big-endian followed by its
// nanoseconds as 4 bytes big-endian, so that no two instants encode the
// same; a single nanosecond count would wrap after 584 years.
func (c Content) Encode() []byte {
	var b []byte
	field := func(v []byte) {
		b = binary.BigEndian.AppendUint32(b, uint32(len(v)))
		b = append(b, v...)
	}
	at := func(t time.Time) []byte {
		v := binary.BigEndian.AppendUint64(nil, uint64(t.Unix()))
		return binary.BigEndian.AppendUint32(v, uint32(t.Nanosecond()))
	}
	field([]byte("fednet approval v1"))
	field([]byte(c.ApprovalID))
	field(c.ParamsSHA256[:])
	field([]byte(c.TargetMachine))
	field([]byte(c.TargetAgent))
	field([]byte(c.Approver))
	field(at(c.ApprovedAt))
	field(at(c.ExpiresAt))
	field(c.Nonce)
	return b
}

// Sign returns key's signature over c.
func Sign(key ed25519.PrivateKey, c Content) []byte {
	return ed25519.Sign(key, c.Encode())
}

// Verify checks that sig is pub's signature over c and that c has not
// expired at now: an approval is usable before ExpiresAt, not at it. It
// does not check that the executor is the target or that the parameters
// match: the executor does, with c's fields.
func Verify(pub ed25519.PublicKey, c Content, sig []byte, now time.Time) error {
	if len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("approval: public key has %d bytes, want %d", len(pub), ed25519.PublicKeySize)
	}
	if !ed25519.Verify(pub, c.Encode(), sig) {
		return ErrBadSignature
	}
	if !now.Before(c.ExpiresAt) {
		return ErrExpired
	}
	return nil
}

// ReadPrivateKey reads the hub's Ed25519 private key from the PKCS#8 PEM
// file at path, the format 1Password's `op read` gives for an SSH key. A
// file that group or others can read is refused. No error quotes the file.
func ReadPrivateKey(path string) (ed25519.PrivateKey, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("approval: %s is readable by group or others (mode %04o), want 0600", path, fi.Mode().Perm())
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, rest := pem.Decode(b)
	if block == nil {
		return nil, fmt.Errorf("approval: %s is not PEM", path)
	}
	if block.Type != "PRIVATE KEY" {
		return nil, fmt.Errorf("approval: %s holds a %q block, want PRIVATE KEY (PKCS#8)", path, block.Type)
	}
	if len(strings.TrimSpace(string(rest))) != 0 {
		return nil, fmt.Errorf("approval: %s has more than one PEM block", path)
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("approval: %s is not a PKCS#8 private key", path)
	}
	priv, ok := key.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("approval: %s holds a %T, want an Ed25519 key", path, key)
	}
	return priv, nil
}

// sshKeyType names an Ed25519 key in the OpenSSH formats.
const sshKeyType = "ssh-ed25519"

// ReadPublicKey reads the hub's public key from the file at path, one line
// in the OpenSSH format: `ssh-ed25519 <base64> [comment]`.
func ReadPublicKey(path string) (ed25519.PublicKey, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pub, err := ParsePublicKey(string(b))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return pub, nil
}

// ParsePublicKey parses one OpenSSH public key line, `ssh-ed25519 <base64>
// [comment]`, with white space around it allowed.
func ParsePublicKey(line string) (ed25519.PublicKey, error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return nil, errors.New("approval: empty, want one line `ssh-ed25519 <base64> [comment]`")
	}
	if strings.ContainsAny(line, "\r\n") {
		return nil, errors.New("approval: more than one line, want one `ssh-ed25519 <base64> [comment]`")
	}
	fields := strings.Fields(line)
	if fields[0] != sshKeyType {
		return nil, fmt.Errorf("approval: key type %q, want %s", fields[0], sshKeyType)
	}
	if len(fields) < 2 {
		return nil, errors.New("approval: no key after ssh-ed25519")
	}
	wire, err := base64.StdEncoding.DecodeString(fields[1])
	if err != nil {
		return nil, fmt.Errorf("approval: key is not base64: %v", err)
	}
	// The wire format is two length-prefixed strings: the type again, then
	// the 32 key bytes.
	want := encodeString([]byte(sshKeyType))
	if len(wire) < len(want) || string(wire[:len(want)]) != string(want) {
		return nil, errors.New("approval: key does not start with the ssh-ed25519 type")
	}
	key := wire[len(want):]
	if len(key) != 4+ed25519.PublicKeySize || binary.BigEndian.Uint32(key) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("approval: key has %d bytes after the type, want a %d-byte key", len(key), ed25519.PublicKeySize)
	}
	return ed25519.PublicKey(key[4:]), nil
}

// FormatPublicKey writes pub as an OpenSSH public key line, without a
// comment or a newline.
func FormatPublicKey(pub ed25519.PublicKey) string {
	wire := append(encodeString([]byte(sshKeyType)), encodeString(pub)...)
	return sshKeyType + " " + base64.StdEncoding.EncodeToString(wire)
}

// encodeString is an SSH wire string: 4 bytes of big-endian length, then b.
func encodeString(b []byte) []byte {
	return append(binary.BigEndian.AppendUint32(nil, uint32(len(b))), b...)
}
