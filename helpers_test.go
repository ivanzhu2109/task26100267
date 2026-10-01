package desktoppool

import (
	"sync"
	"testing"
	"time"
)

// gate 是一次性同步门：open 后所有 wait 立即通过。
type gate struct {
	ch   chan struct{}
	once sync.Once
}

func newGate() *gate { return &gate{ch: make(chan struct{})} }

func (g *gate) open() { g.once.Do(func() { close(g.ch) }) }

func (g *gate) wait() { <-g.ch }

// waitFor 轮询等待条件成立，超时失败。用于等待异步完成，
// 不用于碰运气地固定交错顺序（交错由 gate 固定）。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func findBatch(t *testing.T, s Snapshot, id uint64) BatchSnapshot {
	t.Helper()
	for _, b := range s.Batches {
		if b.ID == id {
			return b
		}
	}
	t.Fatalf("batch %d not found in snapshot", id)
	return BatchSnapshot{}
}

func liveSet(exec *LocalExecutor) map[uint64]bool {
	set := make(map[uint64]bool)
	for _, h := range exec.List() {
		set[h.id] = true
	}
	return set
}

// checkInvariant 核对：占用额度不超过容量，且执行器真实存活资源数
// 不超过协调器占用额度。
func checkInvariant(t *testing.T, c *Coordinator, exec *LocalExecutor) {
	t.Helper()
	s := c.Snapshot()
	if s.Used < 0 || s.Used > Capacity {
		t.Fatalf("used quota %d out of range [0,%d]", s.Used, Capacity)
	}
	if n := len(exec.List()); n > s.Used {
		t.Fatalf("live resources %d exceed used quota %d", n, s.Used)
	}
}
