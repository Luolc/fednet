package approval

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// sample is a content with every field set.
func sample() Content {
	approved := time.Date(2026, 10, 9, 12, 0, 0, 123456789, time.UTC)
	return Content{
		ApprovalID:    "apr-1",
		ParamsSHA256:  sha256.Sum256([]byte(`{"op":"delete","bucket":"b"}`)),
		TargetMachine: "workstation",
		TargetAgent:   "ops-exec",
		Approver:      "U123",
		ApprovedAt:    approved,
		ExpiresAt:     approved.Add(TTL),
		Nonce:         []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	}
}

func keyPair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return pub, priv
}

func TestSignVerify(t *testing.T) {
	pub, priv := keyPair(t)
	c := sample()
	sig := Sign(priv, c)
	if err := Verify(pub, c, sig, c.ApprovedAt); err != nil {
		t.Fatalf("Verify = %v, want nil", err)
	}
	// The same content read back from JSON, in another time zone, still
	// verifies: the signature is over the content, not the document.
	b, err := json.Marshal(Approval{Content: c, Signature: sig})
	if err != nil {
		t.Fatal(err)
	}
	var a Approval
	if err := json.Unmarshal(b, &a); err != nil {
		t.Fatal(err)
	}
	a.ApprovedAt = a.ApprovedAt.In(time.FixedZone("x", 3600))
	if err := Verify(pub, a.Content, a.Signature, c.ApprovedAt); err != nil {
		t.Fatalf("Verify after JSON round trip = %v, want nil", err)
	}
	if !bytes.Equal(a.Encode(), c.Encode()) {
		t.Error("Encode differs after a JSON round trip")
	}
}

// Changing any one field, by as little as one byte or one nanosecond, breaks
// the signature.
func TestVerifyRejectsChangedField(t *testing.T) {
	pub, priv := keyPair(t)
	c := sample()
	sig := Sign(priv, c)
	changes := map[string]func(*Content){
		"approval_id":       func(c *Content) { c.ApprovalID = "apr-2" },
		"params_sha256":     func(c *Content) { c.ParamsSHA256[31] ^= 1 },
		"target_machine":    func(c *Content) { c.TargetMachine = "workstatioN" },
		"target_agent":      func(c *Content) { c.TargetAgent = "ops-exed" },
		"approver_slack_id": func(c *Content) { c.Approver = "U124" },
		"approved_at":       func(c *Content) { c.ApprovedAt = c.ApprovedAt.Add(time.Nanosecond) },
		"expires_at":        func(c *Content) { c.ExpiresAt = c.ExpiresAt.Add(time.Nanosecond) },
		"nonce":             func(c *Content) { c.Nonce[0] ^= 1 },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			changed := sample()
			change(&changed)
			if bytes.Equal(changed.Encode(), c.Encode()) {
				t.Fatal("the change did not alter the content")
			}
			if err := Verify(pub, changed, sig, c.ApprovedAt); !errors.Is(err, ErrBadSignature) {
				t.Errorf("Verify = %v, want ErrBadSignature", err)
			}
		})
	}
}

// Two instants 2^64 nanoseconds apart share their UnixNano; they must still
// encode differently, so a signature over one does not cover the other.
func TestEncodeDistinguishesWrappedNanoseconds(t *testing.T) {
	pub, priv := keyPair(t)
	c := sample()
	sig := Sign(priv, c)
	for name, at := range map[string]*time.Time{"approved_at": &c.ApprovedAt, "expires_at": &c.ExpiresAt} {
		t.Run(name, func(t *testing.T) {
			moved := sample()
			field := map[string]*time.Time{"approved_at": &moved.ApprovedAt, "expires_at": &moved.ExpiresAt}[name]
			*field = time.Unix(at.Unix()+18446744073, int64(at.Nanosecond())+709551616).UTC()
			if field.UnixNano() != at.UnixNano() {
				t.Fatalf("the moved time does not share UnixNano: %d != %d", field.UnixNano(), at.UnixNano())
			}
			if bytes.Equal(moved.Encode(), c.Encode()) {
				t.Fatal("the moved time encodes the same")
			}
			if err := Verify(pub, moved, sig, c.ApprovedAt); !errors.Is(err, ErrBadSignature) {
				t.Errorf("Verify = %v, want ErrBadSignature", err)
			}
		})
	}
}

// Moving bytes across a field boundary, or into an empty field, changes the
// encoding: fields are length-prefixed, not concatenated.
func TestEncodeFieldBoundaries(t *testing.T) {
	a, b := sample(), sample()
	a.TargetMachine, a.TargetAgent = "ab", "c"
	b.TargetMachine, b.TargetAgent = "a", "bc"
	if bytes.Equal(a.Encode(), b.Encode()) {
		t.Error("ab|c and a|bc encode the same")
	}
	a, b = sample(), sample()
	a.ApprovalID, a.Nonce = "", []byte("x")
	b.ApprovalID, b.Nonce = "x", nil
	if bytes.Equal(a.Encode(), b.Encode()) {
		t.Error("an empty id with nonce x and id x with no nonce encode the same")
	}
	if !bytes.Equal(sample().Encode(), sample().Encode()) {
		t.Error("the same content encodes differently")
	}
}

// Usable up to the last nanosecond before expires_at, not at it or after.
func TestVerifyExpiry(t *testing.T) {
	pub, priv := keyPair(t)
	c := sample()
	sig := Sign(priv, c)
	if err := Verify(pub, c, sig, c.ExpiresAt.Add(-time.Nanosecond)); err != nil {
		t.Errorf("just before expires_at: Verify = %v, want nil", err)
	}
	for _, now := range []time.Time{c.ExpiresAt, c.ExpiresAt.Add(time.Nanosecond), c.ExpiresAt.Add(24 * time.Hour)} {
		if err := Verify(pub, c, sig, now); !errors.Is(err, ErrExpired) {
			t.Errorf("at %v: Verify = %v, want ErrExpired", now, err)
		}
	}
}

func TestVerifyRejectsOtherKey(t *testing.T) {
	pub, _ := keyPair(t)
	_, other := keyPair(t)
	c := sample()
	sig := Sign(other, c)
	if err := Verify(pub, c, sig, c.ApprovedAt); !errors.Is(err, ErrBadSignature) {
		t.Errorf("Verify = %v, want ErrBadSignature", err)
	}
	if err := Verify(pub, c, nil, c.ApprovedAt); !errors.Is(err, ErrBadSignature) {
		t.Errorf("Verify with no signature = %v, want ErrBadSignature", err)
	}
	if err := Verify(pub[:31], c, sig, c.ApprovedAt); err == nil || errors.Is(err, ErrBadSignature) {
		t.Errorf("Verify with a short key = %v, want a key error", err)
	}
}

// writePKCS8 writes key as a PKCS#8 PEM file with the given mode.
func writePKCS8(t *testing.T, path string, key any, mode os.FileMode) {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	b := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	if err := os.WriteFile(path, b, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

func TestReadPrivateKey(t *testing.T) {
	dir := t.TempDir()
	pub, priv := keyPair(t)
	path := filepath.Join(dir, "key.pem")
	c := sample()
	for _, mode := range []os.FileMode{0o600, 0o640, 0o440} {
		writePKCS8(t, path, priv, mode)
		got, err := ReadPrivateKey(path)
		if err != nil {
			t.Fatalf("mode %04o: %v", mode, err)
		}
		if err := Verify(pub, c, Sign(got, c), c.ApprovedAt); err != nil {
			t.Errorf("mode %04o: a signature by the key read back does not verify: %v", mode, err)
		}
	}

	ec, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ecPath := filepath.Join(dir, "ec.pem")
	writePKCS8(t, ecPath, ec, 0o600)
	var loose []string
	for _, mode := range []os.FileMode{0o604, 0o644, 0o444} {
		p := filepath.Join(dir, fmt.Sprintf("loose%04o.pem", mode))
		writePKCS8(t, p, priv, mode)
		loose = append(loose, p)
	}
	openssh := filepath.Join(dir, "openssh.pem")
	os.WriteFile(openssh, pem.EncodeToMemory(&pem.Block{Type: "OPENSSH PRIVATE KEY", Bytes: []byte("x")}), 0o600)
	plain := filepath.Join(dir, "plain")
	os.WriteFile(plain, []byte("not a key\n"), 0o600)
	for _, tt := range []struct{ path, want string }{
		{ecPath, "want an Ed25519 key"},
		{loose[0], "accessible to others (mode 0604)"},
		{loose[1], "accessible to others (mode 0644)"},
		{loose[2], "accessible to others (mode 0444)"},
		{openssh, `want PRIVATE KEY (PKCS#8)`},
		{plain, "is not PEM"},
		{filepath.Join(dir, "missing"), "no such file"},
	} {
		_, err := ReadPrivateKey(tt.path)
		if err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("ReadPrivateKey(%s) = %v, want an error containing %q", filepath.Base(tt.path), err, tt.want)
		}
	}
}

// vector is a key ssh-keygen made, and its 32 key bytes decoded from the
// wire format by hand.
const (
	vectorLine = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIEsDRvhwlSlGOHyLEUDF5WBT8RzGTTXfuVnfIDYUpp56 vector"
	vectorHex  = "4b0346f870952946387c8b1140c5e56053f11cc64d35dfb959df203614a69e7a"
)

func TestReadPublicKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hub.pub")
	os.WriteFile(path, []byte(vectorLine+"\n"), 0o644)
	pub, err := ReadPublicKey(path)
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(pub) != vectorHex {
		t.Errorf("key = %x, want %s", []byte(pub), vectorHex)
	}

	// A key we generate survives a trip through FormatPublicKey.
	want, priv := keyPair(t)
	got, err := ParsePublicKey(FormatPublicKey(want) + "\n")
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(want) {
		t.Error("ParsePublicKey(FormatPublicKey(k)) != k")
	}
	c := sample()
	if err := Verify(got, c, Sign(priv, c), c.ApprovedAt); err != nil {
		t.Errorf("Verify with the parsed key = %v", err)
	}

	wrongLen := "ssh-ed25519 " + strings.TrimPrefix(FormatPublicKey(want[:31]), "ssh-ed25519 ")
	for _, tt := range []struct{ name, line, want string }{
		{"empty", "\n", "empty"},
		{"rsa", "ssh-rsa AAAAB3NzaC1yc2E= x", `key type "ssh-rsa"`},
		{"no key", "ssh-ed25519", "no key after"},
		{"bad base64", "ssh-ed25519 ***", "not base64"},
		{"wrong wire type", "ssh-ed25519 AAAAB3NzaC1yc2E=", "does not start with the ssh-ed25519 type"},
		{"short key", wrongLen, "want a 32-byte key"},
		{"two lines", vectorLine + "\n" + vectorLine + "\n", "more than one line"},
		{"pem", "-----BEGIN PUBLIC KEY-----\nAAAA\n-----END PUBLIC KEY-----\n", "more than one line"},
	} {
		_, err := ParsePublicKey(tt.line)
		if err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("%s: ParsePublicKey = %v, want an error containing %q", tt.name, err, tt.want)
		}
	}
	_, err = ReadPublicKey(filepath.Join(dir, "missing"))
	if err == nil || !strings.Contains(err.Error(), "no such file") {
		t.Errorf("ReadPublicKey(missing) = %v", err)
	}
}

func TestDigestJSON(t *testing.T) {
	var d Digest
	for _, bad := range []string{`"zz"`, `"abcd"`, `""`} {
		if err := json.Unmarshal([]byte(bad), &d); err == nil {
			t.Errorf("Unmarshal(%s) = nil, want an error", bad)
		}
	}
}
