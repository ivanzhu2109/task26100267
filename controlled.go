package desktoppool

import (
	"context"
	"sync"
	"sync/atomic"
)

// Gate is a one-shot synchronization barrier: waiters block until Open is
// called. Arrived closes when the first waiter reaches Wait.
type Gate struct {
	arrived     chan struct{}
	release     chan struct{}
	arriveOnce  sync.Once
	releaseOnce sync.Once
}

// NewGate returns a closed Gate.
func NewGate() *Gate {
	return &Gate{arrived: make(chan struct{}), release: make(chan struct{})}
}

// Wait blocks until the gate is opened. The first caller closes Arrived.
func (g *Gate) Wait() {
	g.arriveOnce.Do(func() { close(g.arrived) })
	<-g.release
}

// Arrived returns a channel closed once the first waiter reaches Wait.
func (g *Gate) Arrived() <-chan struct{} { return g.arrived }

// Open releases all current and future waiters. It is idempotent.
func (g *Gate) Open() {
	g.releaseOnce.Do(func() { close(g.release) })
}

// CreateHook runs inside ControlledExecutor.Create after the resource has
// been created but before Create returns. A non-nil error makes Create fail
// and cleans up the just-created resource.
type CreateHook func(seq uint64) error

// DeleteHook runs inside ControlledExecutor.Delete before the real deletion.
// attempt is the per-handle attempt counter starting at 1. A non-nil error
// makes Delete fail transiently, leaving the resource alive.
type DeleteHook func(h Handle, attempt uint64) error

// ControlledExecutor wraps a MemoryExecutor with test-controllable
// synchronization points and failure injection. Hooks may block to model
// long delays; ctx cancellation is intentionally ignored.
type ControlledExecutor struct {
	inner *MemoryExecutor

	mu         sync.Mutex
	createHook CreateHook
	deleteHook DeleteHook

	createSeq      atomic.Uint64
	deleteSeq      map[Handle]uint64
	createsDone    atomic.Int64
	deletesDone    atomic.Int64
	deleteHookHits atomic.Int64
}

// NewControlledExecutor returns a ControlledExecutor with no hooks.
func NewControlledExecutor() *ControlledExecutor {
	return &ControlledExecutor{
		inner:     NewMemoryExecutor(),
		deleteSeq: make(map[Handle]uint64),
	}
}

// SetCreateHook installs (or clears, with nil) the create hook.
func (e *ControlledExecutor) SetCreateHook(h CreateHook) {
	e.mu.Lock()
	e.createHook = h
	e.mu.Unlock()
}

// SetDeleteHook installs (or clears, with nil) the delete hook.
func (e *ControlledExecutor) SetDeleteHook(h DeleteHook) {
	e.mu.Lock()
	e.deleteHook = h
	e.mu.Unlock()
}

// Create implements Executor.
func (e *ControlledExecutor) Create(ctx context.Context) (Handle, error) {
	// Create the resource first, then run the hook: this models an
	// executor that has already allocated the desktop but delays (or
	// ignores cancellation) before reporting success.
	h, err := e.inner.Create(ctx)
	if err != nil {
		return "", err
	}
	e.mu.Lock()
	hook := e.createHook
	e.mu.Unlock()
	if hook != nil {
		if herr := hook(e.createSeq.Add(1)); herr != nil {
			// Create reports failure: guarantee no leftover resource.
			_ = e.inner.Delete(context.Background(), h)
			return "", herr
		}
	}
	e.createsDone.Add(1)
	return h, nil
}

// Delete implements Executor.
func (e *ControlledExecutor) Delete(ctx context.Context, h Handle) error {
	e.mu.Lock()
	e.deleteSeq[h]++
	attempt := e.deleteSeq[h]
	hook := e.deleteHook
	e.mu.Unlock()
	if hook != nil {
		e.deleteHookHits.Add(1)
		if err := hook(h, attempt); err != nil {
			return err // transient failure: resource stays alive
		}
	}
	if err := e.inner.Delete(ctx, h); err != nil {
		return err
	}
	e.deletesDone.Add(1)
	return nil
}

// List implements Executor.
func (e *ControlledExecutor) List() []Handle { return e.inner.List() }

// LiveCount reports how many resources are currently alive.
func (e *ControlledExecutor) LiveCount() int { return e.inner.LiveCount() }

// Stats reports completed Create/Delete call counts.
func (e *ControlledExecutor) Stats() (creates, deletes, deleteHookHits int64) {
	return e.createsDone.Load(), e.deletesDone.Load(), e.deleteHookHits.Load()
}
