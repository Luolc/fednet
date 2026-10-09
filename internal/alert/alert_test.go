package alert

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// secret is the part of the test webhook's URL that must not leak.
const secret = "/services/T000/B000/s3cr3tw3bh00kpath"

// testHook is a webhook that records the texts posted to it and answers
// with status.
type testHook struct {
	mu     sync.Mutex
	status int
	texts  []string
}

// newTestHook returns the test webhook and its URL.
func newTestHook(t *testing.T) (*testHook, *httptest.Server) {
	h := &testHook{status: http.StatusOK}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m struct{ Text string }
		if r.URL.Path != secret || json.NewDecoder(r.Body).Decode(&m) != nil {
			http.Error(w, "bad", http.StatusBadRequest)
			return
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.status == http.StatusOK {
			h.texts = append(h.texts, m.Text)
		}
		w.WriteHeader(h.status)
	}))
	t.Cleanup(srv.Close)
	return h, srv
}

func TestSend(t *testing.T) {
	ctx := context.Background()
	h, srv := newTestHook(t)
	w := &Webhook{URL: srv.URL + secret, From: "workstation"}
	if err := w.Send(ctx, "dead letter"); err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	got := h.texts
	h.status = http.StatusInternalServerError
	h.mu.Unlock()
	if len(got) != 1 || got[0] != "[workstation] dead letter" {
		t.Fatalf("webhook got %q, want the text after the machine", got)
	}

	errs := []error{w.Send(ctx, "x")}
	srv.Close()
	errs = append(errs, w.Send(ctx, "x"))
	for _, err := range errs {
		if err == nil {
			t.Fatal("a failed post returned no error")
		}
		if strings.Contains(err.Error(), "s3cr3t") {
			t.Errorf("error %q contains the webhook URL", err)
		}
	}
}
