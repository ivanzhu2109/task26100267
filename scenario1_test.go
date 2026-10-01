package desktoppool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

// 场景一：450 台 Ready 后接收 50 台；最后一个 Create 已产生资源但尚未
// 返回时取消；删除被暂停期间新批次因容量不足被拒绝；放行晚到成功后删除
// 经历暂时失败；最终只剩原 450 台且额度恰好释放一次。
func TestCancelWithLateCreateAndFlakyDelete(t *testing.T) {
	exec := NewLocalExecutor()
	c := NewCoordinator(exec)

	// 第一批 450 台（9 批 × 50）直接 Ready。
	idsA := make([]uint64, 0, 9)
	for i := 0; i < 9; i++ {
		id, err := c.StartBatch(50)
		if err != nil {
			t.Fatalf("start batch A%d: %v", i, err)
		}
		idsA = append(idsA, id)
	}
	waitFor(t, "all A batches ready", func() bool {
		s := c.Snapshot()
		for _, id := range idsA {
			for _, b := range s.Batches {
				if b.ID == id && b.State != StateReady {
					return false
				}
			}
		}
		return true
	})
	handlesA := liveSet(exec)
	if len(handlesA) != 450 {
		t.Fatalf("expected 450 live resources, got %d", len(handlesA))
	}

	// 第二批 50 台：前 49 个 Create 放行，第 50 个在资源已登记后阻塞。
	var createCalls int32
	lateGate := newGate()
	lateEntered := make(chan struct{})
	exec.SetCreateHook(func(ctx context.Context, h Handle) error {
		if atomic.AddInt32(&createCalls, 1) == 50 {
			close(lateEntered)
			lateGate.wait() // 已产生资源但尚未返回
		}
		return nil
	})
	idB, err := c.StartBatch(50)
	if err != nil {
		t.Fatalf("start batch B: %v", err)
	}
	<-lateEntered
	if n := len(exec.List()); n != 500 {
		t.Fatalf("expected 500 live resources while late create blocked, got %d", n)
	}

	// 取消批次 B：接受，但资源尚未清理。
	res, err := c.CancelBatch(idB)
	if err != nil || res != CancelAccepted {
		t.Fatalf("cancel batch B: res=%v err=%v", res, err)
	}

	// 撤销期间（Create 未返回、删除未开始）仍占满 500，新批次整批拒绝。
	if _, err := c.StartBatch(1); !errors.Is(err, ErrInsufficientCapacity) {
		t.Fatalf("expected insufficient capacity, got %v", err)
	}
	s := c.Snapshot()
	if s.Used != 500 {
		t.Fatalf("expected used=500 during undo, got %d", s.Used)
	}
	if b := findBatch(t, s, idB); b.State != StateUndoing || len(b.Handles) != 0 {
		t.Fatalf("batch B must be Undoing without published handles, got %+v", b)
	}

	// 删除暂时失败：每个句柄第一次 Delete 返回错误，之后成功。
	errFlaky := errors.New("temporary delete failure")
	var delMu sync.Mutex
	failedOnce := make(map[uint64]bool)
	exec.SetDeleteHook(func(ctx context.Context, h Handle) error {
		delMu.Lock()
		defer delMu.Unlock()
		if !failedOnce[h.id] {
			failedOnce[h.id] = true
			return errFlaky
		}
		return nil
	})

	// 放行晚到成功，撤销开始。
	lateGate.open()
	waitFor(t, "batch B terminated", func() bool {
		return findBatch(t, c.Snapshot(), idB).State == StateTerminated
	})

	// 最终只剩原 450 台，额度恰好释放一次（used=450 而非更少）。
	waitFor(t, "late create returned and deletes retired", func() bool {
		return atomic.LoadInt32(&createCalls) == 50 && c.Snapshot().Used == 450
	})
	s = c.Snapshot()
	if s.Used != 450 {
		t.Fatalf("quota must be released exactly once: used=%d, want 450", s.Used)
	}
	live := liveSet(exec)
	if len(live) != 450 {
		t.Fatalf("expected exactly the original 450 live resources, got %d", len(live))
	}
	for id := range handlesA {
		if !live[id] {
			t.Fatalf("original resource %d missing", id)
		}
	}
	for _, id := range idsA {
		if b := findBatch(t, s, id); b.State != StateReady || len(b.Handles) != 50 {
			t.Fatalf("batch A(%d) must stay Ready with 50 handles, got %+v", id, b)
		}
	}
	checkInvariant(t, c, exec)
}
