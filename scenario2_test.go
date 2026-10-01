package desktoppool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

// 准备：批次大小 5，前 4 个 Create 放行，最后一个在资源登记后阻塞，
// 从而把“最后一次创建完成”变成一个可由测试精确释放的事件。
func setupLastCreateBlocked(t *testing.T) (*Coordinator, *LocalExecutor, *gate, chan struct{}, uint64) {
	t.Helper()
	exec := NewLocalExecutor()
	c := NewCoordinator(exec)

	var calls int32
	lastGate := newGate()
	entered := make(chan struct{})
	exec.SetCreateHook(func(ctx context.Context, h Handle) error {
		if atomic.AddInt32(&calls, 1) == 5 {
			close(entered)
			lastGate.wait()
		}
		return nil
	})
	id, err := c.StartBatch(5)
	if err != nil {
		t.Fatalf("start batch: %v", err)
	}
	<-entered
	return c, exec, lastGate, entered, id
}

// 交错 (a)：最后一次创建先完成，批次 Ready，取消落败。
func TestRaceCreateWins(t *testing.T) {
	c, exec, lastGate, _, id := setupLastCreateBlocked(t)

	lastGate.open()
	waitFor(t, "batch ready", func() bool {
		return findBatch(t, c.Snapshot(), id).State == StateReady
	})

	for i := 0; i < 3; i++ {
		res, err := c.CancelBatch(id)
		if err != nil || res != CancelAlreadyReady {
			t.Fatalf("cancel after ready: res=%v err=%v", res, err)
		}
	}

	s := c.Snapshot()
	b := findBatch(t, s, id)
	if b.State != StateReady || len(b.Handles) != 5 {
		t.Fatalf("batch must be Ready with 5 handles, got %+v", b)
	}
	if s.Used != 5 || len(exec.List()) != 5 {
		t.Fatalf("used=%d live=%d, want 5/5", s.Used, len(exec.List()))
	}
	checkInvariant(t, c, exec)
}

// 交错 (b)：取消先被接受，晚到的创建成功不得发布，整批撤销。
func TestRaceCancelWins(t *testing.T) {
	c, exec, lastGate, _, id := setupLastCreateBlocked(t)

	// 重复取消均幂等接受。
	for i := 0; i < 3; i++ {
		res, err := c.CancelBatch(id)
		if err != nil || res != CancelAccepted {
			t.Fatalf("cancel: res=%v err=%v", res, err)
		}
	}

	// 接受取消后，任何快照都不得出现发布（含部分发布）。
	stop := make(chan struct{})
	violation := make(chan string, 1)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			b := findBatch(t, c.Snapshot(), id)
			if b.State == StateReady || len(b.Handles) != 0 {
				select {
				case violation <- "published after cancel accepted":
				default:
				}
				return
			}
		}
	}()
	defer close(stop)

	// 暂停删除，固定“撤销进行中”的快照点。
	delGate := newGate()
	delEntered := make(chan struct{})
	var delOnce sync.Once
	exec.SetDeleteHook(func(ctx context.Context, h Handle) error {
		delOnce.Do(func() { close(delEntered) })
		delGate.wait()
		return nil
	})

	// 放行晚到成功：撤销开始但删除被阻塞。
	lastGate.open()
	<-delEntered
	s := c.Snapshot()
	b := findBatch(t, s, id)
	if b.State != StateUndoing || len(b.Handles) != 0 {
		t.Fatalf("batch must be Undoing without handles, got %+v", b)
	}
	if s.Used != 5 {
		t.Fatalf("undoing batch must still hold quota, used=%d", s.Used)
	}
	if n := len(exec.List()); n != 5 {
		t.Fatalf("late-created resources must still be live before delete, got %d", n)
	}

	// 放行删除，整批撤销完成。
	delGate.open()
	waitFor(t, "batch terminated", func() bool {
		return findBatch(t, c.Snapshot(), id).State == StateTerminated
	})
	select {
	case v := <-violation:
		t.Fatal(v)
	default:
	}

	s = c.Snapshot()
	if s.Used != 0 {
		t.Fatalf("quota must be fully released, used=%d", s.Used)
	}
	if n := len(exec.List()); n != 0 {
		t.Fatalf("no resource may survive, live=%d", n)
	}
	// Terminated 后重复取消仍幂等，批次不会复活。
	res, err := c.CancelBatch(id)
	if err != nil || res != CancelAccepted {
		t.Fatalf("cancel after terminated: res=%v err=%v", res, err)
	}
	if b := findBatch(t, c.Snapshot(), id); b.State != StateTerminated {
		t.Fatalf("terminated batch reappeared as %+v", b)
	}
	checkInvariant(t, c, exec)
}
