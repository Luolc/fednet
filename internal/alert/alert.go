// Package alert posts alerts to a Slack incoming webhook, which delivers
// them to the alerts channel without going through the hub's Slack
// connection: an alert has to get out even when that connection is what
// failed. Only the hub has the webhook; a client's alerts reach it over the
// link. The webhook URL is a credential; it never appears in an error or a
// log.
package alert

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	slackgo "github.com/slack-go/slack"
)

// DefaultTimeout bounds one post to the webhook when Webhook.HTTP is nil.
const DefaultTimeout = 10 * time.Second

// Webhook posts alerts to one incoming webhook.
type Webhook struct {
	// URL is the webhook's URL.
	URL string
	// From names the machine sending the alerts; every alert starts with
	// it.
	From string
	// HTTP sends the posts. Nil means a client with DefaultTimeout.
	HTTP *http.Client
}

// Send posts text, after From, to the webhook. The error never contains
// the URL.
func (w *Webhook) Send(ctx context.Context, text string) error {
	c := w.HTTP
	if c == nil {
		c = &http.Client{Timeout: DefaultTimeout}
	}
	err := slackgo.PostWebhookCustomHTTPContext(ctx, w.URL, c, &slackgo.WebhookMessage{Text: "[" + w.From + "] " + text})
	if err == nil {
		return nil
	}
	return errors.New("alert: " + w.redact(err.Error()))
}

// redact removes the URL from s, in the forms an error may quote it in.
func (w *Webhook) redact(s string) string {
	forms := []string{w.URL}
	if u, err := url.Parse(w.URL); err == nil {
		forms = append(forms, u.String(), u.Redacted(), u.Path, u.EscapedPath())
	}
	for _, f := range forms {
		if f != "" && f != "/" {
			s = strings.ReplaceAll(s, f, "<webhook>")
		}
	}
	return s
}
