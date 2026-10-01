package desktoppool

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// within runs f and fails if it does not return promptly, proving that
// queries and cancels never block on in-flight executor calls.
func within(t *testing.T, what string, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); f() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not complete promptly", what)
	}
}

func findBatch(s Snapshot, id BatchID) (BatchSnapshot, bool) {
	for _, b := range s.Batches {
		if b.ID == id {
			return b, true
		}
	}
	return BatchSnapshot{}, false
}

func mustBatch(t *testing.T, s Snapshot, id BatchID) BatchSnapshot {
	t.Helper()
	b, ok := findBatch(s, id)
	if !ok {
		t.Fatalf("batch %d missing from snapshot", id)
	}
	return b
}

func handleSet(list []Handle) map[Handle]struct{} {
	set := make(map[Handle]struct{}, len(list))
	for _, h := range list {
		set[h] = struct{}{}
	}
	return set
}

func equalSets(a map[Handle]struct{}, b []Handle) bool {
	if len(a) != len(b) {
		return false
	}
	for _, h := range b {
		if _, ok := a[h]; !ok {
			return false
		}
	}
	return true
}

// TestCancelWithLateCreateAndTransientDeleteFailures covers: 450 Ready,
// accept 50 more, cancel while the last Create has produced a resource but
// not returned, deletes blocked then transiently failing, and exactly-once
// quota release.
func TestCancelWithLateCreateAndTransientDeleteFailures(t *testing.T) {
	exec := NewControlledExecutor()
	coord := NewCoordinator(exec, Capacity)

	// Bring 450 desktops to Ready in 9 batches of 50.
	for i := 0; i < 9; i++ {
		if _, err := coord.StartBatch(MaxBatchSize); err != nil {
			t.Fatalf("setup StartBatch: %v", err)
		}
	}
	waitFor(t, "450 desktops Ready", func() bool {
		s := coord.Snapshot()
		ready := 0
		for _, b := range s.Batches {
			if b.State == StateReady {
				ready += b.Count
			}
		}
		return ready == 450
	})
	original := handleSet(exec.List())
	if len(original) != 450 {
		t.Fatalf("expected 450 live resources, got %d", len(original))
	}

	// The 50th Create of the next batch blocks after creating its resource.
	createGate := NewGate()
	var arrival atomic.Int64
	exec.SetCreateHook(func(seq uint64) error {
		if arrival.Add(1) == 50 {
			createGate.Wait()
		}
		return nil
	})

	id, err := coord.StartBatch(MaxBatchSize)
	if err != nil {
		t.Fatalf("StartBatch(50) at 450 used: %v", err)
	}
	waitFor(t, "49 creates returned and 50th resource created", func() bool {
		creates, _, _ := exec.Stats()
		return creates == 499 && exec.LiveCount() == 500
	})

	// Deletes block on a gate and fail transiently on first attempt.
	deleteGate := NewGate()
	var deleteCalls atomic.Int64
	transient := errors.New("transient delete failure")
	exec.SetDeleteHook(func(h Handle, attempt uint64) error {
		deleteCalls.Add(1)
		deleteGate.Wait()
		if attempt == 1 {
			return transient
		}
		return nil
	})

	if err := coord.CancelBatch(id); err != nil {
		t.Fatalf("CancelBatch: %v", err)
	}
	if err := coord.CancelBatch(id); err != nil {
		t.Fatalf("repeated CancelBatch must be idempotent: %v", err)
	}

	// While the batch is undoing, its quota is still occupied.
	if _, err := coord.StartBatch(1); !errors.Is(err, ErrInsufficientCapacity) {
		t.Fatalf("expected capacity rejection during undo, got %v", err)
	}
	s := coord.Snapshot()
	if s.Used != Capacity {
		t.Fatalf("expected used=500 during undo, got %d", s.Used)
	}
	if b := mustBatch(t, s, id); b.State != StateUndoing || len(b.Handles) != 0 {
		t.Fatalf("expected Undoing with no published handles, got %+v", b)
	}

	// Let the late create succeed; its resource must also be deleted.
	createGate.Open()
	waitFor(t, "all 50 deletes attempted", func() bool {
		return deleteCalls.Load() == 50
	})
	if _, err := coord.StartBatch(1); !errors.Is(err, ErrInsufficientCapacity) {
		t.Fatalf("expected capacity rejection while deletes blocked, got %v", err)
	}

	// Release deletes: first attempts fail transiently, retries succeed.
	deleteGate.Open()
	waitFor(t, "batch Terminated", func() bool {
		b, _ := findBatch(coord.Snapshot(), id)
		return b.State == StateTerminated
	})

	s = coord.Snapshot()
	if s.Used != 450 {
		t.Fatalf("quota must be released exactly once: used=%d, want 450", s.Used)
	}
	if got := exec.List(); !equalSets(original, got) {
		t.Fatalf("live resources drifted: have %d, want the original 450", len(got))
	}
	// Quota stays released; repeated cancel remains idempotent.
	for i := 0; i < 10; i++ {
		if err := coord.CancelBatch(id); err != nil {
			t.Fatalf("cancel after Terminated must be idempotent: %v", err)
		}
		if got := coord.Snapshot().Used; got != 450 {
			t.Fatalf("quota released more than once: used=%d", got)
		}
	}
}

// observer continuously validates cross-cutting invariants until done is
// closed: no partial publish, no publish after cancel, used <= capacity,
// live resources <= used quota.
func observe(t *testing.T, coord *Coordinator, exec *ControlledExecutor, id BatchID, count int, cancelAccepted *atomic.Bool, done <-chan struct{}) {
	t.Helper()
	for {
		select {
		case <-done:
			return
		default:
		}
		s := coord.Snapshot()
		if s.Used > Capacity {
			t.Errorf("used quota %d exceeds capacity", s.Used)
		}
		if live := exec.LiveCount(); live > s.Used {
			t.Errorf("live resources %d exceed used quota %d", live, s.Used)
		}
		if b, ok := findBatch(s, id); ok {
			if b.State == StateReady && len(b.Handles) != count {
				t.Errorf("Ready batch published %d handles, want %d", len(b.Handles), count)
			}
			if b.State != StateReady && len(b.Handles) != 0 {
				t.Errorf("non-Ready batch %s published %d handles", b.State, len(b.Handles))
			}
			if cancelAccepted.Load() && (b.State == StateReady || len(b.Handles) != 0) {
				t.Errorf("batch published desktops after cancel was accepted: %+v", b)
			}
		}
	}
}

// TestRaceLastCreateVsCancel covers both outcomes of the race between the
// final Create completing and CancelBatch, with repeated cancels mixed in.
func TestRaceLastCreateVsCancel(t *testing.T) {
	setup := func(t *testing.T) (*ControlledExecutor, *Coordinator, *Gate, BatchID) {
		exec := NewControlledExecutor()
		coord := NewCoordinator(exec, Capacity)
		gate := NewGate()
		var arrival atomic.Int64
		exec.SetCreateHook(func(seq uint64) error {
			if arrival.Add(1) == 50 {
				gate.Wait()
			}
			return nil
		})
		id, err := coord.StartBatch(MaxBatchSize)
		if err != nil {
			t.Fatalf("StartBatch: %v", err)
		}
		waitFor(t, "49 creates returned, last one blocked", func() bool {
			creates, _, _ := exec.Stats()
			return creates == 49 && exec.LiveCount() == 50
		})
		return exec, coord, gate, id
	}

	t.Run("cancel wins", func(t *testing.T) {
		exec, coord, gate, id := setup(t)
		var cancelAccepted atomic.Bool
		done := make(chan struct{})
		defer close(done)
		go observe(t, coord, exec, id, MaxBatchSize, &cancelAccepted, done)

		if err := coord.CancelBatch(id); err != nil {
			t.Fatalf("CancelBatch: %v", err)
		}
		cancelAccepted.Store(true)
		if err := coord.CancelBatch(id); err != nil {
			t.Fatalf("repeated cancel must be idempotent: %v", err)
		}

		gate.Open() // late success must be deleted, never published
		waitFor(t, "batch Terminated", func() bool {
			b, _ := findBatch(coord.Snapshot(), id)
			return b.State == StateTerminated
		})
		s := coord.Snapshot()
		b := mustBatch(t, s, id)
		if len(b.Handles) != 0 {
			t.Fatalf("cancelled batch published handles: %+v", b)
		}
		if s.Used != 0 || exec.LiveCount() != 0 {
			t.Fatalf("after Terminated: used=%d live=%d, want 0/0", s.Used, exec.LiveCount())
		}
	})

	t.Run("create wins", func(t *testing.T) {
		exec, coord, gate, id := setup(t)
		var cancelAccepted atomic.Bool
		done := make(chan struct{})
		defer close(done)
		go observe(t, coord, exec, id, MaxBatchSize, &cancelAccepted, done)

		gate.Open() // last create completes first: batch becomes Ready
		waitFor(t, "batch Ready", func() bool {
			b, _ := findBatch(coord.Snapshot(), id)
			return b.State == StateReady
		})
		for i := 0; i < 3; i++ {
			if err := coord.CancelBatch(id); !errors.Is(err, ErrBatchAlreadyReady) {
				t.Fatalf("cancel of Ready batch: got %v, want ErrBatchAlreadyReady", err)
			}
		}
		s := coord.Snapshot()
		b := mustBatch(t, s, id)
		if len(b.Handles) != MaxBatchSize {
			t.Fatalf("Ready batch published %d handles, want 50", len(b.Handles))
		}
		if !equalSets(handleSet(exec.List()), b.Handles) {
			t.Fatalf("published handles do not match live resources")
		}
		if s.Used != MaxBatchSize {
			t.Fatalf("Ready batch must hold its quota: used=%d", s.Used)
		}
	})

	t.Run("concurrent hammer", func(t *testing.T) {
		exec := NewControlledExecutor()
		coord := NewCoordinator(exec, Capacity)
		for i := 0; i < 200; i++ {
			id, err := coord.StartBatch(2)
			if err != nil {
				t.Fatalf("iteration %d: StartBatch: %v", i, err)
			}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() { defer wg.Done(); _ = coord.CancelBatch(id) }()
			wg.Wait()
			waitFor(t, "batch settled", func() bool {
				b, _ := findBatch(coord.Snapshot(), id)
				return b.State == StateReady || b.State == StateTerminated
			})
			b := mustBatch(t, coord.Snapshot(), id)
			if b.State == StateReady && len(b.Handles) != 2 {
				t.Fatalf("iteration %d: Ready with %d handles", i, len(b.Handles))
			}
			if b.State == StateTerminated && len(b.Handles) != 0 {
				t.Fatalf("iteration %d: Terminated with published handles", i)
			}
		}
		s := coord.Snapshot()
		if exec.LiveCount() != s.Used {
			t.Fatalf("live=%d != used=%d", exec.LiveCount(), s.Used)
		}
	})
}

// TestUndoDoesNotBlockOtherBatches blocks one batch's Delete and verifies
// another batch with sufficient quota can still be accepted and become
// Ready, and that Snapshot/repeated cancels complete promptly.
func TestUndoDoesNotBlockOtherBatches(t *testing.T) {
	exec := NewControlledExecutor()
	coord := NewCoordinator(exec, Capacity)

	createGate := NewGate()
	var arrival atomic.Int64
	exec.SetCreateHook(func(seq uint64) error {
		if arrival.Add(1) == 50 {
			createGate.Wait()
		}
		return nil
	})
	a, err := coord.StartBatch(MaxBatchSize)
	if err != nil {
		t.Fatalf("StartBatch A: %v", err)
	}
	waitFor(t, "49 creates of A returned", func() bool {
		creates, _, _ := exec.Stats()
		return creates == 49
	})

	deleteGate := NewGate()
	exec.SetDeleteHook(func(h Handle, attempt uint64) error {
		deleteGate.Wait()
		return nil
	})
	if err := coord.CancelBatch(a); err != nil {
		t.Fatalf("CancelBatch A: %v", err)
	}
	exec.SetCreateHook(nil)
	createGate.Open() // A's late create returns; its delete blocks too

	// B has enough quota and must not be blocked by A's stuck deletes.
	b, err := coord.StartBatch(MaxBatchSize)
	if err != nil {
		t.Fatalf("StartBatch B: %v", err)
	}
	waitFor(t, "B Ready", func() bool {
		bb, _ := findBatch(coord.Snapshot(), b)
		return bb.State == StateReady
	})

	s := coord.Snapshot()
	if got := mustBatch(t, s, a).State; got != StateUndoing {
		t.Fatalf("A should still be Undoing, got %s", got)
	}
	if s.Used != 100 || exec.LiveCount() != 100 {
		t.Fatalf("used=%d live=%d, want 100/100", s.Used, exec.LiveCount())
	}

	// Queries and repeated cancels complete while A's deletes are blocked.
	within(t, "Snapshot during blocked deletes", func() { coord.Snapshot() })
	within(t, "repeated cancel of A", func() {
		if err := coord.CancelBatch(a); err != nil {
			t.Errorf("repeated cancel of A: %v", err)
		}
	})
	within(t, "cancel of Ready B", func() {
		if err := coord.CancelBatch(b); !errors.Is(err, ErrBatchAlreadyReady) {
			t.Errorf("cancel of Ready B: %v", err)
		}
	})

	deleteGate.Open()
	waitFor(t, "A Terminated", func() bool {
		bb, _ := findBatch(coord.Snapshot(), a)
		return bb.State == StateTerminated
	})
	s = coord.Snapshot()
	if s.Used != MaxBatchSize {
		t.Fatalf("after A terminated: used=%d, want 50", s.Used)
	}
	bb := mustBatch(t, s, b)
	if !equalSets(handleSet(exec.List()), bb.Handles) {
		t.Fatalf("only B's desktops should remain alive")
	}
}

// TestCreateFailureUndoesBatch verifies that any Create failure rolls back
// the whole batch and releases the quota exactly once.
func TestCreateFailureUndoesBatch(t *testing.T) {
	exec := NewControlledExecutor()
	coord := NewCoordinator(exec, Capacity)

	boom := errors.New("create boom")
	var arrival atomic.Int64
	exec.SetCreateHook(func(seq uint64) error {
		if arrival.Add(1) == 25 {
			return boom
		}
		return nil
	})
	id, err := coord.StartBatch(MaxBatchSize)
	if err != nil {
		t.Fatalf("StartBatch: %v", err)
	}
	waitFor(t, "batch Terminated", func() bool {
		b, _ := findBatch(coord.Snapshot(), id)
		return b.State == StateTerminated
	})
	s := coord.Snapshot()
	if s.Used != 0 || exec.LiveCount() != 0 {
		t.Fatalf("after undo: used=%d live=%d, want 0/0", s.Used, exec.LiveCount())
	}
	if b := mustBatch(t, s, id); len(b.Handles) != 0 {
		t.Fatalf("failed batch published handles: %+v", b)
	}
}

// TestValidation covers argument and state checks.
func TestValidation(t *testing.T) {
	exec := NewControlledExecutor()
	coord := NewCoordinator(exec, Capacity)
	for _, n := range []int{0, -1, 51, 1000} {
		if _, err := coord.StartBatch(n); !errors.Is(err, ErrInvalidCount) {
			t.Fatalf("StartBatch(%d): got %v, want ErrInvalidCount", n, err)
		}
	}
	if err := coord.CancelBatch(BatchID(999)); !errors.Is(err, ErrBatchNotFound) {
		t.Fatalf("CancelBatch(unknown): got %v, want ErrBatchNotFound", err)
	}
	if got := coord.Snapshot().Used; got != 0 {
		t.Fatalf("invalid StartBatch consumed quota: used=%d", got)
	}
}
