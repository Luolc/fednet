// Package release decides whether a hub serves a client of a given version.
// A release is vMAJOR.MINOR.PATCH; anything else, such as the "dev" of an
// unversioned build, is not a release.
package release

import (
	"fmt"
	"strconv"
	"strings"
)

// Compatible returns nil when a hub running hub serves a client running
// client: the two are the same version, or client is a release no older
// than min. A client that is not a release, or an older release, gets an
// error that says what the hub requires.
func Compatible(min, client, hub string) error {
	if client == hub {
		return nil
	}
	r, ok := parse(client)
	if !ok {
		return fmt.Errorf("client version %q is not a release; this hub (%s) serves %s and newer", client, hub, min)
	}
	m, ok := parse(min)
	if !ok {
		return fmt.Errorf("the hub's required client version %q is not a release", min)
	}
	if r.before(m) {
		return fmt.Errorf("client version %s is older than this hub (%s) serves: %s and newer", client, hub, min)
	}
	return nil
}

// release is a parsed vMAJOR.MINOR.PATCH.
type release [3]int

func (r release) before(o release) bool {
	for i := range r {
		if r[i] != o[i] {
			return r[i] < o[i]
		}
	}
	return false
}

func parse(v string) (release, bool) {
	var r release
	s, ok := strings.CutPrefix(v, "v")
	if !ok {
		return r, false
	}
	parts := strings.Split(s, ".")
	if len(parts) != len(r) {
		return r, false
	}
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 || strconv.Itoa(n) != p {
			return r, false
		}
		r[i] = n
	}
	return r, true
}
