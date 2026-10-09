// Package auth is how a client proves to the hub which client it is. The
// client generates a random credential once and keeps it in a file only its
// user can read; the hub's registry holds the credential's SHA-256, never the
// credential. On every request the client sends its id, the credential and
// its version; the hub hashes the credential, compares in constant time, and
// records the version.
package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/Luolc/fednet/internal/link"
	"github.com/Luolc/fednet/internal/store"
)

// VersionHeader carries the client's version on every request to the hub.
const VersionHeader = "Fednet-Version"

// secretBytes is the entropy of a credential: 256 bits.
const secretBytes = 32

// Credential identifies one client. It is what the credential file holds.
type Credential struct {
	ClientID string `json:"client_id"`
	// Secret is a random value; the hub knows only its hash.
	Secret string `json:"secret"`
}

// New returns a credential for clientID with a fresh random secret.
func New(clientID string) (Credential, error) {
	if clientID == "" {
		return Credential{}, errors.New("auth: empty client id")
	}
	b := make([]byte, secretBytes)
	if _, err := rand.Read(b); err != nil {
		return Credential{}, err
	}
	return Credential{ClientID: clientID, Secret: base64.RawURLEncoding.EncodeToString(b)}, nil
}

// Hash returns what the hub stores for this credential.
func (c Credential) Hash() []byte { return hash(c.Secret) }

func hash(secret string) []byte {
	h := sha256.Sum256([]byte(secret))
	return h[:]
}

// Header returns the headers that carry the credential and version to the
// hub. The client id goes in link.ClientHeader, set by the link.
func (c Credential) Header(version string) http.Header {
	return http.Header{
		"Authorization": {"Bearer " + c.Secret},
		VersionHeader:   {version},
	}
}

// Write creates the credential file at path, readable only by its owner. It
// refuses to replace an existing file: the hub was registered with the hash
// of the credential in it.
func Write(path string, c Credential) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(f).Encode(c); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// Read loads the credential file at path. A file that group or others can
// read is refused; the credential in it should be treated as leaked.
func Read(path string) (Credential, error) {
	f, err := os.Open(path)
	if err != nil {
		return Credential{}, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return Credential{}, err
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return Credential{}, fmt.Errorf("auth: %s is readable by group or others (mode %04o), want 0600", path, fi.Mode().Perm())
	}
	var c Credential
	if err := json.NewDecoder(f).Decode(&c); err != nil {
		return Credential{}, fmt.Errorf("auth: %s: %v", path, err)
	}
	if c.ClientID == "" || c.Secret == "" {
		return Credential{}, fmt.Errorf("auth: %s: missing client_id or secret", path)
	}
	return c, nil
}

// Authenticator identifies clients against the hub's registry. Its Identify
// is meant for link.Hub.Identify.
type Authenticator struct {
	Store *store.Hub
}

// Identify returns the id of the registered client whose credential r
// carries, and records the version r reports. The error, which never
// includes the credential, says why a request is refused.
func (a *Authenticator) Identify(r *http.Request) (string, error) {
	id := r.Header.Get(link.ClientHeader)
	secret, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if id == "" || !ok || secret == "" {
		return "", errors.New("auth: missing client id or credential")
	}
	reg, err := a.Store.Registration(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		return "", fmt.Errorf("auth: client %q is not registered", id)
	}
	if err != nil {
		return "", err
	}
	if reg.Revoked {
		return "", fmt.Errorf("auth: client %q is revoked", id)
	}
	if subtle.ConstantTimeCompare(hash(secret), reg.SecretHash) != 1 {
		return "", fmt.Errorf("auth: wrong credential for client %q", id)
	}
	if v := r.Header.Get(VersionHeader); v != reg.Version {
		if err := a.Store.SetVersion(r.Context(), id, v); err != nil {
			return "", err
		}
	}
	return id, nil
}
