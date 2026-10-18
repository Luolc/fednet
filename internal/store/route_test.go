package store

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestRoutes(t *testing.T) {
	ctx := t.Context()
	h := openHub(t, filepath.Join(t.TempDir(), "hub.db"))
	if _, err := h.Route(ctx, "C1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Route(C1) on a new database = %v, want ErrNotFound", err)
	}
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	c1 := Route{Channel: "C1", Machine: "workstation", Scope: "control-plane", OpenThread: []string{"workstation", "datamachine"}}
	dm := Route{Channel: DMRoute, Machine: "workstation"}
	who := RouteChange{RequestID: "r1", Client: "workstation", Agent: "lead", Approver: "U1", At: at}
	for _, r := range []Route{c1, dm} {
		if err := h.SetRoute(ctx, r.Channel, &r, who); err != nil {
			t.Fatal(err)
		}
	}
	moved := Route{Channel: "C1", Machine: "datamachine"}
	if err := h.SetRoute(ctx, "C1", &moved, who); err != nil {
		t.Fatal(err)
	}
	if err := h.SetRoute(ctx, DMRoute, nil, who); err != nil {
		t.Fatal(err)
	}
	if got, err := h.Routes(ctx); err != nil || !reflect.DeepEqual(got, []Route{moved}) {
		t.Fatalf("Routes = %+v, %v; want only %+v", got, err, moved)
	}
	if got, err := h.Route(ctx, "C1"); err != nil || !reflect.DeepEqual(got, moved) {
		t.Fatalf("Route(C1) = %+v, %v; want %+v", got, err, moved)
	}

	// Each change is recorded with the route as it was and as it is.
	changes, err := h.UnpostedRouteChanges(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var got [][2][]Route
	for _, c := range changes {
		if c.RequestID != "r1" || c.Client != "workstation" || c.Agent != "lead" || c.Approver != "U1" || !c.At.Equal(at) {
			t.Fatalf("change %+v does not say who and when", c)
		}
		got = append(got, [2][]Route{c.Before, c.After})
	}
	want := [][2][]Route{{{}, {c1}}, {{}, {dm}}, {{c1}, {moved}}, {{dm}, {}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("changes (before, after) = %+v, want %+v", got, want)
	}
	if err := h.MarkRouteChangePosted(ctx, changes[0].Seq); err != nil {
		t.Fatal(err)
	}
	if left, err := h.UnpostedRouteChanges(ctx); err != nil || len(left) != 3 || left[0].Seq != changes[1].Seq {
		t.Fatalf("UnpostedRouteChanges after posting the first = %+v, %v; want the other three", left, err)
	}
}

// The config's routes go into a database once: not into one whose
// routes were set before, even if no route is left.
func TestImportRoutes(t *testing.T) {
	ctx := t.Context()
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	routes := []Route{{Channel: "C1", Machine: "workstation", OpenThread: []string{"workstation"}}, {Channel: DMRoute, Machine: "workstation"}}

	h := openHub(t, filepath.Join(t.TempDir(), "hub.db"))
	if imported, err := h.ImportRoutes(ctx, nil, at); err != nil || imported {
		t.Fatalf("ImportRoutes(nil) = %v, %v; want false", imported, err)
	}
	if imported, err := h.ImportRoutes(ctx, routes, at); err != nil || !imported {
		t.Fatalf("ImportRoutes = %v, %v; want true", imported, err)
	}
	if got, err := h.Routes(ctx); err != nil || !reflect.DeepEqual(got, []Route{routes[0], routes[1]}) {
		t.Fatalf("Routes after the import = %+v, %v; want %+v", got, err, routes)
	}
	changes, err := h.UnpostedRouteChanges(ctx)
	if err != nil || len(changes) != 1 || changes[0].RequestID != "" || len(changes[0].Before) != 0 || !reflect.DeepEqual(changes[0].After, routes) || !changes[0].At.Equal(at) {
		t.Fatalf("changes after the import = %+v, %v; want the one import", changes, err)
	}
	if imported, err := h.ImportRoutes(ctx, []Route{{Channel: "C2", Machine: "datamachine"}}, at); err != nil || imported {
		t.Fatalf("a second ImportRoutes = %v, %v; want false", imported, err)
	}

	h = openHub(t, filepath.Join(t.TempDir(), "hub.db"))
	if err := h.SetRoute(ctx, "C1", &routes[0], RouteChange{At: at}); err != nil {
		t.Fatal(err)
	}
	if err := h.SetRoute(ctx, "C1", nil, RouteChange{At: at}); err != nil {
		t.Fatal(err)
	}
	if imported, err := h.ImportRoutes(ctx, routes, at); err != nil || imported {
		t.Fatalf("ImportRoutes after every route was deleted = %v, %v; want false", imported, err)
	}
	if got, err := h.Routes(ctx); err != nil || len(got) != 0 {
		t.Fatalf("Routes = %+v, %v; want none", got, err)
	}
}

// releasedHubMigrations is how many migrations the hub had in the release
// before the routes moved into the database: a hub rolled back to that
// release opens the database with them.
const releasedHubMigrations = 9

// A hub of the release before the routes opens a database this one has
// written routes, route requests, channel requests, question cards and
// scopes to, and
// sees no route request among its approvals; a hub of the release after
// it, which reads route requests, sees no channel request among them.
func TestOlderHubOpensTheDatabase(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "hub.db")
	h := openHub(t, path)
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	if _, err := h.ImportRoutes(ctx, []Route{{Channel: "C1", Machine: "workstation", Scope: "data"}}, at); err != nil {
		t.Fatal(err)
	}
	if err := h.PutApproval(ctx, Approval{ID: "r1", Kind: KindRoute, Client: "workstation", Agent: "lead", Summary: "move C1", Action: []byte(`{}`),
		Nonce: []byte{}, RequestedAt: at, ExpiresAt: at.Add(time.Hour), Channel: "C9", TS: "1.1"}); err != nil {
		t.Fatal(err)
	}
	if err := h.PutApproval(ctx, Approval{ID: "c1", Kind: KindChannel, Client: "workstation", Agent: "lead", Summary: "new repo", Action: []byte(`{"create":{"name":"repo-x"}}`),
		Nonce: []byte{}, RequestedAt: at, ExpiresAt: at.Add(time.Hour), Channel: "C9", TS: "1.2"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.ClaimAndEnqueue(ctx, "C1/1.1", "workstation", "", "data", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := h.PutQuestion(ctx, Question{ID: "q1", Client: "workstation", Ref: "r1", Thread: "C1/1.1", Card: []byte(`{"question": "q?"}`), AskedAt: at, TS: "1.3"}); err != nil {
		t.Fatal(err)
	}
	if err := h.SetHop(ctx, "C1/1.1", 2); err != nil {
		t.Fatal(err)
	}
	h.Close()

	db, err := open(ctx, path, hubMigrations[:releasedHubMigrations])
	if err != nil {
		t.Fatalf("the older hub cannot open the database: %v", err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM approval").Scan(&n); err != nil || n != 0 {
		t.Fatalf("the older hub's approval table has %d rows, %v; want none", n, err)
	}
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM route_request").Scan(&n); err != nil || n != 1 {
		t.Fatalf("route_request has %d rows, %v; want only the route request, not the channel request", n, err)
	}
}

// Route requests are approvals of their own kind: read, listed and
// decided with the others, and kept apart from them.
func TestRouteRequests(t *testing.T) {
	ctx := t.Context()
	h := openHub(t, filepath.Join(t.TempDir(), "hub.db"))
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	a := Approval{ID: "apr-1", Client: "workstation", Agent: "ops", Summary: "delete b", Action: []byte(`{"b":1}`), Nonce: []byte{1},
		RequestedAt: at, ExpiresAt: at.Add(time.Hour), Channel: "C9", TS: "1.1"}
	r := a
	r.ID, r.Kind, r.Nonce, r.RequestedAt = "route-1", KindRoute, []byte{}, at.Add(time.Second)
	for _, x := range []Approval{a, r} {
		if err := h.PutApproval(ctx, x); err != nil {
			t.Fatal(err)
		}
	}
	got, err := h.Approval(ctx, "route-1")
	r.Status = Pending
	if len(got.Nonce) == 0 {
		got.Nonce = r.Nonce // an empty blob reads back as nil
	}
	if err != nil || !reflect.DeepEqual(got, r) {
		t.Fatalf("Approval(route-1) = %+v, %v; want %+v", got, err, r)
	}
	if ps, err := h.PendingApprovals(ctx); err != nil || len(ps) != 2 || ps[0].ID != "apr-1" || ps[1].ID != "route-1" {
		t.Fatalf("PendingApprovals = %+v, %v; want apr-1 then route-1", ps, err)
	}
	decided, err := h.DecideApproval(ctx, "route-1", func(tx *Hub, got Approval) (Decision, error) {
		return Decision{Status: "approved", By: "U1", At: at.Add(time.Minute)}, nil
	})
	if err != nil || !decided {
		t.Fatalf("DecideApproval(route-1) = %v, %v; want true", decided, err)
	}
	if us, err := h.UnfinishedCards(ctx); err != nil || len(us) != 1 || us[0].ID != "route-1" || us[0].Kind != KindRoute {
		t.Fatalf("UnfinishedCards = %+v, %v; want route-1", us, err)
	}
	if err := h.MarkCardFinal(ctx, "route-1"); err != nil {
		t.Fatal(err)
	}
	if us, err := h.UnfinishedCards(ctx); err != nil || len(us) != 0 {
		t.Fatalf("UnfinishedCards after marking final = %+v, %v; want none", us, err)
	}
	if got, err := h.Approval(ctx, "apr-1"); err != nil || got.Status != Pending || got.Kind != KindAction {
		t.Fatalf("Approval(apr-1) = %+v, %v; want still a pending action", got, err)
	}
}
