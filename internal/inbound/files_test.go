package inbound

import (
	"reflect"
	"testing"

	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/slack"
)

// The hub marks, on the message handed to the client, the files the
// client fetches before the hook runs: images and PDFs within the limits;
// the files of the history are listed with their ids and never marked.
func TestPrefetchMarks(t *testing.T) {
	r, f := newReceiver(t)
	r.Prefetch = Prefetch{MaxBytes: 100, MaxTotal: 150}
	root, err := f.Start("C1", "U1", "see the thread")
	if err != nil {
		t.Fatal(err)
	}
	earlier := slack.Message{ThreadTS: root, User: "U1", Text: "earlier", Files: []slack.File{{ID: "F0", Name: "old.png", Mimetype: "image/png", Size: 10, URL: "https://example.invalid/F0"}}}
	if _, err := f.Add("C1", earlier); err != nil {
		t.Fatal(err)
	}
	ev := message("Ev1", "C1", "1700000001.000000", root, hey+"and these")
	ev.SubType = "file_share"
	ev.Files = []slack.File{
		{ID: "F1", Name: "a.png", Mimetype: "image/png", Size: 100, URL: "https://example.invalid/F1"},
		{ID: "F2", Name: "b.zip", Mimetype: "application/zip", Size: 10, URL: "https://example.invalid/F2"},
		{ID: "F3", Name: "c.pdf", Mimetype: "application/pdf", Size: 101, URL: "https://example.invalid/F3"},
		{ID: "F4", Name: "d.jpeg", Mimetype: "image/jpeg", Size: 60, URL: "https://example.invalid/F4"},
		{ID: "F5", Name: "e.pdf", Mimetype: "application/pdf", Size: 50, URL: "https://example.invalid/F5"},
		{Name: "noid.png", Mimetype: "image/png", Size: 1, URL: "https://example.invalid/F6"},
	}
	handle(t, r, ev)
	got := queued(t, r.Store, "workstation")
	if len(got) != 1 {
		t.Fatalf("queued %d messages, want 1", len(got))
	}
	want := []payload.File{
		// Within both limits.
		{ID: "F1", Name: "a.png", Mimetype: "image/png", Size: 100, URL: "https://example.invalid/F1", Fetch: true},
		// Not a type fetched before the hook.
		{ID: "F2", Name: "b.zip", Mimetype: "application/zip", Size: 10, URL: "https://example.invalid/F2"},
		// Over MaxBytes.
		{ID: "F3", Name: "c.pdf", Mimetype: "application/pdf", Size: 101, URL: "https://example.invalid/F3"},
		// Would take the total over MaxTotal (100 + 60).
		{ID: "F4", Name: "d.jpeg", Mimetype: "image/jpeg", Size: 60, URL: "https://example.invalid/F4"},
		// Fits in what is left (100 + 50).
		{ID: "F5", Name: "e.pdf", Mimetype: "application/pdf", Size: 50, URL: "https://example.invalid/F5", Fetch: true},
		// Without an id nothing can be fetched.
		{Name: "noid.png", Mimetype: "image/png", Size: 1, URL: "https://example.invalid/F6"},
	}
	if !reflect.DeepEqual(got[0].Files, want) {
		t.Fatalf("files = %+v, want %+v", got[0].Files, want)
	}
	if h := got[0].History; h == nil || len(h.Messages) != 2 || !reflect.DeepEqual(h.Messages[1].Files, []payload.File{{ID: "F0", Name: "old.png", Mimetype: "image/png", Size: 10, URL: "https://example.invalid/F0"}}) {
		t.Fatalf("history = %+v, want the earlier message's file listed with its id and not marked", h)
	}
}

func TestPrefetchTypes(t *testing.T) {
	var def Prefetch
	for mimetype, want := range map[string]bool{"image/png": true, "image/svg+xml": true, "application/pdf": true, "application/zip": false, "imagex/png": false, "": false} {
		if got := def.matches(mimetype); got != want {
			t.Errorf("default matches(%q) = %v, want %v", mimetype, got, want)
		}
	}
	only := Prefetch{Types: []string{"text/plain"}}
	if only.matches("image/png") || !only.matches("text/plain") {
		t.Error("Types set: want only text/plain to match")
	}
	none := Prefetch{Types: []string{}}
	if none.matches("image/png") {
		t.Error("empty Types: want nothing to match")
	}
}
