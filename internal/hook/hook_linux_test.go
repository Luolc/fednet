package hook

import (
	"github.com/Luolc/fednet/internal/store"
	"syscall"
	"testing"
	"time"
)

// prSetChildSubreaper is the Linux prctl option that makes this process
// the parent of its orphaned descendants.
const prSetChildSubreaper = 36

// The residue judge used above tells a reaped process from a killed one
// nobody has waited for: with the test process as subreaper and not
// waiting, the hook's child stays a zombie and still counts as left. The
// earlier judge, which took a zombie for gone, reads 0 here.
func TestResidueCountsZombies(t *testing.T) {
	if _, _, errno := syscall.Syscall6(syscall.SYS_PRCTL, prSetChildSubreaper, 1, 0, 0, 0, 0); errno != 0 {
		t.Fatal(errno)
	}
	t.Cleanup(func() { syscall.Syscall6(syscall.SYS_PRCTL, prSetChildSubreaper, 0, 0, 0, 0, 0) })
	f := newFixture(t, `sleep 60 >/dev/null 2>&1 & echo $! > "$DIR/child"; exit 0`)
	if err := f.r.exec(t.Context(), store.Message{MsgID: "m1", Payload: []byte(`{"t":"x"}`)}); err != nil {
		t.Fatal(err)
	}
	child, ok := f.pid("child")
	if !ok {
		t.Fatal("the hook did not write its child's pid")
	}
	// Killed, reparented to this process, and nobody has waited: a zombie.
	waitFor(t, "the child to be killed", func() bool { return state(t, child) == "Z" })
	time.Sleep(10 * time.Millisecond)
	if !left(child) {
		t.Fatalf("child %d is a zombie but counts as gone", child)
	}
	// Reaped by its parent: now it is gone.
	var ws syscall.WaitStatus
	if _, err := syscall.Wait4(child, &ws, 0, nil); err != nil {
		t.Fatal(err)
	}
	if left(child) {
		t.Fatalf("child %d is left after being reaped", child)
	}
}
