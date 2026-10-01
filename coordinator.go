package desktoppool

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

const (
	// Capacity 是池的固定容量（台）。
	Capacity = 500
	// MaxBatchSize 是每批最大台数。
	MaxBatchSize = 50
)

var (
	ErrInvalidCount         = errors.New("count must be between 1 and 50")
	ErrInsufficientCapacity = errors.New("insufficient pool capacity")
	ErrBatchNotFound        = errors.New("batch not found")
)

// BatchState 描述批次在“供应到 Ready 之前”生命周期中的状态。
type BatchState string

const (
	StateProvisioning BatchState = "Provisioning" // 已占用额度，Create 进行中
	StateReady        BatchState = "Ready"        // 全部桌面已一次性发布
	StateUndoing      BatchState = "Undoing"      // 撤销中：等待 Create 返回并删除已建资源
	StateTerminated   BatchState = "Terminated"   // 清理完成，额度已一次性释放
)

// BatchSnapshot 是单个批次的一致视图。Handles 仅在 Ready 时非空。
type BatchSnapshot struct {
	ID      uint64
	State   BatchState
	Count   int
	Handles []Handle
}

// Snapshot 是池占用额度与各批次状态、已发布句柄的一致视图。
type Snapshot struct {
	Capacity int
	Used     int
	Batches  []BatchSnapshot
}

// CancelResult 是 CancelBatch 的结果。
type CancelResult int

const (
	// CancelAccepted 表示取消被接受；不代表资源已清理完。
	CancelAccepted CancelResult = iota
	// CancelAlreadyReady 表示批次已 Ready，不可取消。
	CancelAlreadyReady
)

func (r CancelResult) String() string {
	if r == CancelAlreadyReady {
		return "AlreadyReady"
	}
	return "Accepted"
}

type batch struct {
	id             uint64
	count          int
	state          BatchState
	cancelAccepted bool
	published      []Handle
	ctx            context.Context
	cancel         context.CancelFunc
}

type createResult struct {
	handle Handle
	err    error
}

// Coordinator 是桌面池批量供应协调器。
//
// 锁纪律：持有内部锁时绝不调用执行器；所有 Create/Delete 都在独立
// goroutine 中发出，StartBatch/CancelBatch/Snapshot 只操作内存状态，
// 不会等待任何一次执行器调用结束。
type Coordinator struct {
	exec Executor

	mu      sync.Mutex
	used    int
	nextID  uint64
	batches map[uint64]*batch

	deleteRetryInterval time.Duration
}

func NewCoordinator(exec Executor) *Coordinator {
	return &Coordinator{
		exec:                exec,
		batches:             make(map[uint64]*batch),
		deleteRetryInterval: 5 * time.Millisecond,
	}
}

// StartBatch 整批接收并返回批次 ID；容量不足则整批拒绝。
// 接收时立即占用 count 个额度，不等待任何 Create 完成。
func (c *Coordinator) StartBatch(count int) (uint64, error) {
	if count < 1 || count > MaxBatchSize {
		return 0, ErrInvalidCount
	}
	c.mu.Lock()
	if c.used+count > Capacity {
		c.mu.Unlock()
		return 0, ErrInsufficientCapacity
	}
	c.used += count
	c.nextID++
	ctx, cancel := context.WithCancel(context.Background())
	b := &batch{id: c.nextID, count: count, state: StateProvisioning, ctx: ctx, cancel: cancel}
	c.batches[b.id] = b
	c.mu.Unlock()

	go c.provision(b)
	return b.id, nil
}

// provision 并发发出全部 Create（同批可有多个 Create 同时未返回），
// 等全部返回后决定发布或撤销。全程不持有协调器锁等待执行器。
func (c *Coordinator) provision(b *batch) {
	results := make(chan createResult, b.count)
	var wg sync.WaitGroup
	for i := 0; i < b.count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h, err := c.exec.Create(b.ctx)
			results <- createResult{handle: h, err: err}
		}()
	}
	wg.Wait()
	close(results)

	var created []Handle
	failed := false
	for r := range results {
		if r.err != nil {
			failed = true
			continue
		}
		created = append(created, r.handle)
	}

	c.mu.Lock()
	if !failed && !b.cancelAccepted {
		// 取消未被接受：一次性发布全部桌面，成为 Ready。
		b.state = StateReady
		b.published = created
		c.mu.Unlock()
		return
	}
	// 任一 Create 失败或取消已被接受：撤销整批（含晚到成功的结果）。
	b.state = StateUndoing
	c.mu.Unlock()

	c.undo(b, created)
}

// undo 删除全部已成功创建的资源，失败自动重试；全部删除完毕后
// 才转为 Terminated 并一次性释放整批额度。
func (c *Coordinator) undo(b *batch, handles []Handle) {
	var wg sync.WaitGroup
	for _, h := range handles {
		wg.Add(1)
		go func(h Handle) {
			defer wg.Done()
			for {
				if err := c.exec.Delete(context.Background(), h); err == nil {
					return
				}
				time.Sleep(c.deleteRetryInterval)
			}
		}(h)
	}
	wg.Wait()

	c.mu.Lock()
	b.state = StateTerminated
	c.used -= b.count
	c.mu.Unlock()
}

// CancelBatch 取消尚未 Ready 的批次。重复取消幂等；
// 返回接受不代表资源已经清理完。
func (c *Coordinator) CancelBatch(id uint64) (CancelResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.batches[id]
	if !ok {
		return CancelAccepted, ErrBatchNotFound
	}
	switch b.state {
	case StateReady:
		return CancelAlreadyReady, nil
	case StateProvisioning:
		b.cancelAccepted = true
		b.state = StateUndoing
		b.cancel() // 仅发信号；执行器可能忽略，晚到成功仍会被回收
		return CancelAccepted, nil
	default: // Undoing / Terminated：幂等
		return CancelAccepted, nil
	}
}

// Snapshot 返回池占用额度及各批次状态、已发布句柄的一致视图。
func (c *Coordinator) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := Snapshot{Capacity: Capacity, Used: c.used, Batches: make([]BatchSnapshot, 0, len(c.batches))}
	for _, b := range c.batches {
		bs := BatchSnapshot{ID: b.id, State: b.state, Count: b.count}
		if b.state == StateReady {
			bs.Handles = append([]Handle(nil), b.published...)
		}
		s.Batches = append(s.Batches, bs)
	}
	sort.Slice(s.Batches, func(i, j int) bool { return s.Batches[i].ID < s.Batches[j].ID })
	return s
}
