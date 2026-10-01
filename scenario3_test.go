package desktoppool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

// 场景三：阻塞撤销批次的 Delete，另一个有足够额度的批次仍能接收并
// 成为 Ready；查询与重复取消也能完成。
func TestUndoDoesNotBlockOtherBatch(t *testing.T) {
	exec := NewLocalExecutor()
	c := NewCoordinator(exec)

	// 批次 A 的 50 个 Create 全部在资源登记后阻塞，验证同批多个
	// Create 可同时处于未返回状态。
	var createCalls int32
	gateA := newGate()
	allInFlight := make(chan struct{})
	var inflight int32
	var inflightOnce sync.Once
	exec.SetCreateHook(func(ctx context.Context, h Handle) error {
		n := atomic.AddInt32(&createCalls, 1)
		if n <= 50 {
			if atomic.AddInt32(&inflight, 1) == 50 {
				inflightOnce.Do(func() { close(allInFlight) })
			}
			gateA.wait()
		}
		return nil
	})
	idA, err := c.StartBatch(50)
	if err != nil {
		t.Fatalf("start batch A: %v", err)
	}
	<-allInFlight // 50 个 Create 同时未返回

	// 取消 A（重复取消幂等），此时 Create 尚未返回。
	for i := 0; i < 2; i++ {
		res, err := c.CancelBatch(idA)
		if err != nil || res != CancelAccepted {
			t.Fatalf("cancel A: res=%v err=%v", res, err)
		}
	}

	// 放行 A 的晚到成功；撤销开始，但 Delete 全部被阻塞。
	delGate := newGate()
	delEntered := make(chan struct{})
	var delOnce sync.Once
	exec.SetDeleteHook(func(ctx context.Context, h Handle) error {
		delOnce.Do(func() { close(delEntered) })
		delGate.wait()
		return nil
	})
	gateA.open()
	<-delEntered

	// A 的删除被阻塞期间：B（10 台）仍有足够额度，能被接收并成为 Ready。
	idB, err := c.StartBatch(10)
	if err != nil {
		t.Fatalf("start batch B while A undoing: %v", err)
	}
	waitFor(t, "batch B ready", func() bool {
		return findBatch(t, c.Snapshot(), idB).State == StateReady
	})

	// 查询与重复取消都能完成，不被 A 的清理阻塞。
	s := c.Snapshot()
	if s.Used != 60 {
		t.Fatalf("used=%d, want 60 (50 undoing + 10 ready)", s.Used)
	}
	if b := findBatch(t, s, idA); b.State != StateUndoing || len(b.Handles) != 0 {
		t.Fatalf("batch A must be Undoing without handles, got %+v", b)
	}
	if b := findBatch(t, s, idB); b.State != StateReady || len(b.Handles) != 10 {
		t.Fatalf("batch B must be Ready with 10 handles, got %+v", b)
	}
	res, err := c.CancelBatch(idA)
	if err != nil || res != CancelAccepted {
		t.Fatalf("repeat cancel A: res=%v err=%v", res, err)
	}
	checkInvariant(t, c, exec)

	// 放行 A 的删除：A 终结并释放额度，B 不受影响。
	delGate.open()
	waitFor(t, "batch A terminated", func() bool {
		return findBatch(t, c.Snapshot(), idA).State == StateTerminated
	})
	s = c.Snapshot()
	if s.Used != 10 {
		t.Fatalf("used=%d, want 10 after A terminated", s.Used)
	}
	live := liveSet(exec)
	if len(live) != 10 {
		t.Fatalf("only batch B resources may survive, live=%d", len(live))
	}
	for _, h := range findBatch(t, s, idB).Handles {
		if !live[h.id] {
			t.Fatalf("batch B handle %d not live", h.id)
		}
	}
	if b := findBatch(t, s, idB); b.State != StateReady {
		t.Fatalf("batch B must stay Ready, got %+v", b)
	}
	checkInvariant(t, c, exec)
}
