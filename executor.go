// Package desktoppool implements a batch provisioning coordinator for a
// fixed-capacity desktop pool.
package desktoppool

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// Handle is an opaque identifier for a live desktop resource.
type Handle string

// Executor manages real desktop resources. The coordinator only talks to
// this interface and never touches the executor's internal resource set.
type Executor interface {
	// Create creates one resource. On success it returns a globally unique
	// handle; on error it guarantees no resource is left behind. It may
	// delay arbitrarily and may ignore ctx cancellation.
	Create(ctx context.Context) (Handle, error)
	// Delete removes a resource. It may fail transiently; deletion is
	// idempotent and only success guarantees the resource is gone.
	Delete(ctx context.Context, h Handle) error
	// List enumerates currently live resources.
	List() []Handle
}

// MemoryExecutor is a runnable in-process Executor. Create succeeds
// immediately; Delete always succeeds and is idempotent.
type MemoryExecutor struct {
	mu   sync.Mutex
	live map[Handle]struct{}
	seq  atomic.Int64
	id   string
}

// NewMemoryExecutor returns an empty MemoryExecutor.
func NewMemoryExecutor() *MemoryExecutor {
	return &MemoryExecutor{
		live: make(map[Handle]struct{}),
		id:   fmt.Sprintf("exec-%p", new(int)),
	}
}

// Create implements Executor.
func (e *MemoryExecutor) Create(ctx context.Context) (Handle, error) {
	h := Handle(fmt.Sprintf("%s-desk-%d", e.id, e.seq.Add(1)))
	e.mu.Lock()
	e.live[h] = struct{}{}
	e.mu.Unlock()
	return h, nil
}

// Delete implements Executor.
func (e *MemoryExecutor) Delete(ctx context.Context, h Handle) error {
	e.mu.Lock()
	delete(e.live, h)
	e.mu.Unlock()
	return nil
}

// List implements Executor.
func (e *MemoryExecutor) List() []Handle {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Handle, 0, len(e.live))
	for h := range e.live {
		out = append(out, h)
	}
	return out
}

// LiveCount reports how many resources are currently alive.
func (e *MemoryExecutor) LiveCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.live)
}
