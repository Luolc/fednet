package inbound

import (
	"strings"

	"github.com/Luolc/fednet/internal/payload"
)

// Defaults of Prefetch.
const (
	DefaultPrefetchMaxBytes = 20 << 20
	DefaultPrefetchMaxTotal = 50 << 20
)

// DefaultPrefetchTypes are the types fetched before the hook runs when
// Prefetch.Types is nil: images and PDFs.
var DefaultPrefetchTypes = []string{"image/*", "application/pdf"}

// Prefetch says which of the files uploaded with a message the client
// fetches before it runs the hook for it: those whose type is one of
// Types, no larger than MaxBytes, as long as they add up to no more than
// MaxTotal, taken in the order Slack lists them. A zero field means the
// default.
type Prefetch struct {
	// Types are mimetypes; one ending in "/*" matches its whole top-level
	// type.
	Types []string
	// MaxBytes is the largest file fetched; MaxTotal is the most one
	// message's fetched files add up to.
	MaxBytes int64
	MaxTotal int64
}

func (p Prefetch) types() []string {
	if p.Types == nil {
		return DefaultPrefetchTypes
	}
	return p.Types
}

func (p Prefetch) maxBytes() int64 { return or64(p.MaxBytes, DefaultPrefetchMaxBytes) }
func (p Prefetch) maxTotal() int64 { return or64(p.MaxTotal, DefaultPrefetchMaxTotal) }

func or64(v, def int64) int64 {
	if v == 0 {
		return def
	}
	return v
}

// mark sets Fetch on the files of fs the client is to fetch.
func (p Prefetch) mark(fs []payload.File) []payload.File {
	var total int64
	for i, f := range fs {
		size := int64(f.Size)
		if f.ID == "" || !p.matches(f.Mimetype) || size > p.maxBytes() || total+size > p.maxTotal() {
			continue
		}
		fs[i].Fetch = true
		total += size
	}
	return fs
}

// matches reports whether mimetype is one of the types.
func (p Prefetch) matches(mimetype string) bool {
	for _, t := range p.types() {
		if top, ok := strings.CutSuffix(t, "/*"); ok {
			if strings.HasPrefix(mimetype, top+"/") {
				return true
			}
		} else if mimetype == t {
			return true
		}
	}
	return false
}
