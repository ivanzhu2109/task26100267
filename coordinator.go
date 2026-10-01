package desktoppool

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

// Pool capacity and per-batch limits.
const (
	Capacity     = 500
	MaxBatchSize = 50
)

// Errors returned by coordinator operations.
var (
	ErrInvalidCount         = errors.New("desktoppool: count must be between 1 and 50")
	ErrInsufficientCapacity = errors.New("desktoppool: insufficient pool capacity")
	ErrBatchNotFound        = errors.New("desktoppool: unknown batch id")
	ErrBatchAlreadyReady    = errors.New("desktoppool: batch already Ready, cannot cancel")
)

// BatchID identifies an accepted batch.
type BatchID int64

// BatchState is the lifecycle state of a batch.
type BatchState string

// Batch states.
const (
	StateCreating   BatchState = "Creating"
	StateReady      BatchState = "Ready"
	StateUndoing    BatchState = "Undoing"
	StateTerminated BatchState = "Terminated"
)

// BatchSnapshot is a consistent view of one batch.
type BatchSnapshot struct {
	ID      BatchID
	State   BatchState
	Count   int
	Handles []Handle // published desktops; non-empty only when Ready
}

// Snapshot is a consistent view of the whole pool.
type Snapshot struct {
	Capacity int
	Used     int
	Batches  []BatchSnapshot
}

// Coordinator batches desktop provisioning against an Executor.
type Coordinator struct {
	mu       sync.Mutex
	exec     Executor
	capacity int
	used     int
	nextID   BatchID
	batches  map[BatchID]*batch
	cancels  map[BatchID]context.CancelFunc
}

type batch struct {
	id        BatchID
	count     int
	state     BatchState
	returned  int
	handles   map[Handle]struct{}
	published []Handle
}

// NewCoordinator returns a Coordinator with the given capacity (<=500).
func NewCoordinator(exec Executor, capacity int) *Coordinator {
	if capacity <= 0 || capacity > Capacity {
		capacity = Capacity
	}
	return &Coordinator{
		exec:     exec,
		capacity: capacity,
		batches:  make(map[BatchID]*batch),
		cancels:  make(map[BatchID]context.CancelFunc),
	}
}

// StartBatch reserves count slots and starts provisioning asynchronously.
// The quota is occupied immediately, before any Create returns.
func (c *Coordinator) StartBatch(count int) (BatchID, error) {
	if count < 1 || count > MaxBatchSize {
		return 0, ErrInvalidCount
	}
	c.mu.Lock()
	if c.used+count > c.capacity {
		c.mu.Unlock()
		return 0, ErrInsufficientCapacity
	}
	c.used += count
	c.nextID++
	id := c.nextID
	b := &batch{
		id:      id,
		count:   count,
		state:   StateCreating,
		handles: make(map[Handle]struct{}),
	}
	c.batches[id] = b
	ctx, cancel := context.WithCancel(context.Background())
	c.cancels[id] = cancel
	c.mu.Unlock()

	// One goroutine per Create: up to `count` creates may be in flight
	// concurrently, and no coordinator lock is held while they run.
	for i := 0; i < count; i++ {
		go c.createOne(b, ctx)
	}
	return id, nil
}

// CancelBatch cancels a batch that is not Ready yet. Accepting the cancel
// only starts the undo; resources are cleaned up asynchronously. Repeated
// calls are idempotent.
func (c *Coordinator) CancelBatch(id BatchID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.batches[id]
	if !ok {
		return ErrBatchNotFound
	}
	switch b.state {
	case StateReady:
		return ErrBatchAlreadyReady
	case StateCreating:
		c.beginUndoLocked(b)
		return nil
	default:
		// Undoing or Terminated: cancel was already accepted.
		return nil
	}
}

// createOne runs a single Create and records the outcome. A success that
// arrives after undo started is deleted like any other created resource.
func (c *Coordinator) createOne(b *batch, ctx context.Context) {
	h, err := c.exec.Create(ctx)
	c.mu.Lock()
	defer c.mu.Unlock()
	b.returned++
	if err != nil {
		// Any Create failure undoes the whole batch.
		c.beginUndoLocked(b)
	} else {
		b.handles[h] = struct{}{}
		if b.state == StateUndoing {
			c.deleteAsync(b, h)
		}
	}
	c.settleLocked(b)
}

// beginUndoLocked transitions a Creating batch into Undoing: it signals
// cancellation to in-flight creates and deletes every resource created so
// far. The caller must hold c.mu.
func (c *Coordinator) beginUndoLocked(b *batch) {
	if b.state != StateCreating {
		return
	}
	b.state = StateUndoing
	c.cancels[b.id]() // signal only; late successes are still handled
	for h := range b.handles {
		c.deleteAsync(b, h)
	}
}

// deleteAsync deletes one handle, retrying transient failures until the
// executor confirms the resource is gone. It never holds c.mu while
// calling into the executor.
func (c *Coordinator) deleteAsync(b *batch, h Handle) {
	go func() {
		for {
			if err := c.exec.Delete(context.Background(), h); err == nil {
				break
			}
			time.Sleep(time.Millisecond)
		}
		c.mu.Lock()
		delete(b.handles, h)
		c.settleLocked(c.batches[b.id])
		c.mu.Unlock()
	}()
}

// settleLocked advances the batch state once all outstanding work has
// reported back. The caller must hold c.mu.
func (c *Coordinator) settleLocked(b *batch) {
	switch b.state {
	case StateCreating:
		// Reaching `returned == count` while still Creating means every
		// Create succeeded (a failure would have moved us to Undoing).
		// Publish all desktops atomically and become Ready.
		if b.returned == b.count {
			b.state = StateReady
			b.published = sortedHandles(b.handles)
		}
	case StateUndoing:
		// Terminate only after every issued Create has returned and every
		// created resource (including late successes) has been deleted.
		// The quota is released exactly once, here.
		if b.returned == b.count && len(b.handles) == 0 {
			b.state = StateTerminated
			c.used -= b.count
			delete(c.cancels, b.id)
		}
	}
}

func sortedHandles(set map[Handle]struct{}) []Handle {
	out := make([]Handle, 0, len(set))
	for h := range set {
		out = append(out, h)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Snapshot returns a consistent view of pool usage and batch states.
func (c *Coordinator) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	snap := Snapshot{
		Capacity: c.capacity,
		Used:     c.used,
		Batches:  make([]BatchSnapshot, 0, len(c.batches)),
	}
	for _, b := range c.batches {
		bs := BatchSnapshot{ID: b.id, State: b.state, Count: b.count}
		if b.state == StateReady {
			bs.Handles = append([]Handle(nil), b.published...)
		}
		snap.Batches = append(snap.Batches, bs)
	}
	sort.Slice(snap.Batches, func(i, j int) bool { return snap.Batches[i].ID < snap.Batches[j].ID })
	return snap
}
