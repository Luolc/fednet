package slack

import (
	"errors"
	"slices"
	"testing"
)

// Slack's already_reacted and no_reaction mean the bot's reaction already
// is, or is not, there: React and Unreact take them as done. Other errors
// stay errors.
func TestWebReact(t *testing.T) {
	w, ts, _ := newTestWeb(t, func(r request) (int, string) {
		switch r.form.Get("name") {
		case "repeat":
			if r.method == "reactions.add" {
				return 200, `{"ok":false,"error":"already_reacted"}`
			}
			return 200, `{"ok":false,"error":"no_reaction"}`
		case "scope":
			return 200, `{"ok":false,"error":"missing_scope","needed":"reactions:write"}`
		}
		if r.form.Get("timestamp") == "9.9" {
			return 200, `{"ok":false,"error":"message_not_found"}`
		}
		return 200, `{"ok":true}`
	})
	ctx := t.Context()
	for _, call := range []func(c, ts, name string) error{
		func(c, ts, name string) error { return w.React(ctx, c, ts, name) },
		func(c, ts, name string) error { return w.Unreact(ctx, c, ts, name) },
	} {
		if err := call("C1", "1.1", "hourglass_flowing_sand"); err != nil {
			t.Fatal(err)
		}
		if err := call("C1", "1.1", "repeat"); err != nil {
			t.Fatalf("a reaction already so = %v, want done", err)
		}
		if err := call("C1", "1.1", "scope"); Code(err) != "missing_scope" {
			t.Fatalf("missing scope = %v, want missing_scope", err)
		}
		if err := call("C1", "9.9", "question"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("an unknown message = %v, want ErrNotFound", err)
		}
	}
	rs, _ := ts.got()
	for i, want := range []string{"reactions.add", "reactions.remove"} {
		r := rs[4*i]
		if r.method != want || r.form.Get("channel") != "C1" || r.form.Get("timestamp") != "1.1" || r.form.Get("name") != "hourglass_flowing_sand" {
			t.Fatalf("sent %s %v, want %s on C1/1.1", r.method, r.form, want)
		}
	}
}

func TestFakeReact(t *testing.T) {
	ctx := t.Context()
	var f Fake
	f.AddChannel("C1", "")
	root, _ := f.Start("C1", "U1", "hi")
	for _, name := range []string{"question", "question", "white_check_mark"} {
		if err := f.React(ctx, "C1", root, name); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		if err := f.Unreact(ctx, "C1", root, "question"); err != nil {
			t.Fatal(err)
		}
	}
	if got := f.Reactions("C1", root); !slices.Equal(got, []string{"white_check_mark"}) {
		t.Fatalf("Reactions = %v, want [white_check_mark]", got)
	}
	if err := f.React(ctx, "C1", "9.9", "question"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("React on no message = %v, want ErrNotFound", err)
	}
}
